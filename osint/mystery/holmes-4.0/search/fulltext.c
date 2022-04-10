/*
 *	Sherlock Search Engine -- Fulltext Matching
 *
 *	(c) 2005--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/bbuf.h"
#include "sherlock/index.h"
#include "sherlock/tagged-text.h"
#include "search/sherlockd.h"
#include "search/refs.h"
#include "search/lexicon.h"
#include "search/vocabolario.h"
#include "search/fulltext.h"

/*
 * The context of the fulltext matcher is a part of the ref_context, but since
 * it takes some time to initialize it, we keep a cache of ref_contexts for
 * all database we have already encountered.
 */

struct ft_context {
  snode n;
  struct ref_context *ref_context;
};

static void
ft_init_voc(struct ref_context *c)
{
  struct query *q = c->query;
  struct database *db = c->dbase;

  uns cnt = 0;
  for (uns i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      if (w->is_string || w->hide_count >= w->use_count)
	continue;
      if (w->root)
	w = w->root;
      SLIST_FOR_EACH(struct variant *, v, w->variants)
	cnt++;
    }
  struct vocabolario *voc = voc_new(q->pool, cnt, db->wexc_vocabolario);
  c->ft_voc = voc;

  for (uns i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      if (w->is_string || w->hide_count >= w->use_count)
	continue;
      if (w->root)
	w = w->root;
      SLIST_FOR_EACH(struct variant *, v, w->variants)
	{
	  byte ww[MAX_WORD_BYTES+1];
	  word_extract_variant(w, v, ww);
	  DBG("\tInserting <%s> (wid=%d, class=%d)", ww, i, w->word_class);
	  voc_key_t key;
	  if (voc_key_utf8(voc, &key, ww))
	    {
	      struct vocabolo *o = voc_insert_variant(voc, &key);
	      if (o->word_class == 0xff)
		o->word_class = w->word_class;
	      else
		ASSERT(o->word_class == w->word_class);
	      o->word_id = i;
	      o->penalty = v->penalty;
	      o->noaccent_only = v->noaccent_only;
	      o->lang_mask = v->lang_mask;
	    }
	  else
	    ASSERT(0);  // Lowercasing overflow cannot happen here since the variants should be already lowercase
	}
    }

#ifdef LOCAL_DEBUG
  voc_dump(voc);
#endif
}

static struct ref_context *
ft_init_db(struct query *q, struct database *db)
{
  db_switch_config(db);
  if (!restore_interims(q, q->results, db))
    {
      DBG("FT: Requested database %s, which is not available", db->name);
      return NULL;
    }

  SLIST_FOR_EACH(struct ft_context *, ftc, q->ft_contexts)
    if (ftc->ref_context->dbase == db)
      {
	DBG("FT: Switched to cached context for db %s", db->name);
	return ftc->ref_context;
      }

  DBG("FT: Creating new context for db %s", db->name);
  struct ref_context *c = init_bare_ref_context(q, &ref_buffers[0]);
  ft_init_voc(c);

  struct ft_context *ftc = mp_alloc(q->pool, sizeof(*ftc));
  ftc->ref_context = c;
  slist_add_tail(&q->ft_contexts, &ftc->n);
  return c;
}

struct ref_context *
ft_init_card(struct query *q, struct database *db, struct card_attr *attr)
{
  struct ref_context *c = ft_init_db(q, db);
  if (c)
    {
      c->attrs = attr;
#ifdef CONFIG_LANG
      c->ft_lang_mask = 1 << CA_GET_FILE_LANG(attr);
#endif
      c->ft_is_accented = !!(attr->flags & CARD_FLAG_ACCENTED);
      DBG("FT: Working on card %x (lang_mask=%x, is_acc=%d)", attr-db->card_attrs, c->ft_lang_mask, c->ft_is_accented);
    }
  return c;
}

static void
ft_expand_matches(struct ref_context *c, struct ft_results *res, uns min)
{
  struct ref_buffers *buf = c->buffers;
  bb_grow(&buf->fulltext_words, min*sizeof(struct ft_word));
  res->words = (void *) buf->fulltext_words.ptr;
  res->max_words = buf->fulltext_words.len / sizeof(struct ft_word);
  ASSERT(res->max_words >= min);
}

typedef struct {
  int ftw_index;		// We must not use pointers here, because the buffer migrates
  struct vocabolo *voc;
} word_id_t;

#define WORD_ID_NULL (word_id_t){ -1, NULL }
#define WORD_ID_DEFINED_P(x) ((x).ftw_index >= 0)

static struct ft_results *ft_current_results;
static struct ref_context *ft_current_context;

static inline uns
ft_add_word(void)
{
  struct ft_results *res = ft_current_results;
  if (unlikely(res->num_words >= res->max_words))
    ft_expand_matches(ft_current_context, res, res->num_words+1);
  return res->num_words++;
}

static enum word_class
lm_lookup(enum word_class orig_class, u16 *uni, uns ulen, word_id_t *p, const byte *orig, const uns olen, void *user UNUSED)
{
  struct ft_word *w;
  voc_key_t key;
  struct vocabolo *v;

  if (!uni)
    return orig_class;
  if (!voc_key_ucs2(ft_current_context->ft_voc, &key, uni, ulen))
    return WC_GARBAGE;
  v = voc_find(ft_current_context->ft_voc, &key);

  p->ftw_index = ft_add_word();
  p->voc = v;

  w = ft_current_results->words + p->ftw_index;
  w->pos = POS_NOWHERE;
  w->orig = (byte*) orig;
  w->olen = MIN(olen, 0xff);
  w->type = FT_TYPE_UNSET;
  w->weight = (v && v->word_id < 0x80);	// Ignored query words get weight 1, other query words get more from the trail later

  if (v && orig_class == WC_NORMAL)
    return v->word_class;
  else
    return orig_class;
}

static inline void
ft_got_word(word_id_t p, uns pos, uns type)
{
  if (pos < POS_NOWHERE)
    {
      struct ref_context *c = ft_current_context;
      struct ft_results *res = ft_current_results;
      struct ft_word *w = res->words + p.ftw_index;
      struct vocabolo *v = p.voc;

      if (w->pos == POS_NOWHERE)
	w->pos = pos;
      else
	{
	  ASSERT(w->pos == pos);
	  return;
	}
      w->type = type;
      for (; v && v->word_id < 0x80; v=v->same)
	{
	  struct ref_word *rw = &c->words[v->word_id];
	  uns type_mask = tweak_type_mask(rw->type_mask, v->lang_mask, v->noaccent_only, c->ft_lang_mask, c->ft_is_accented);
	  int wt;
	  switch (res->input_type)
	    {
	    case FT_TEXT:
	      if (!(type_mask & (1 << type)))
		continue;
	      wt = rw->weight_array[type];
	      break;
	    case FT_META:
	      if (!(type_mask & (0x10000 << type)))
		continue;
	      wt = (*rw->meta_weight_array)[type][res->meta_subtype];
	      break;
	    case FT_URL:
	      wt = 0;
	      break;
	    default:
	      ASSERT(0);
	    }
	  wt -= v->penalty;
	  struct trail_entry *t = ft_trail_add(c);
	  t->pos = pos;
	  t->weight = wt;
	  t->word_index = v->word_id;
	  if (rw->is_outer)
	    {
	      int q = wt*word_weight_scale + rw->weight;
	      t->q = MIN(q, 65535);
	    }
	  else
	    t->q = 0;
	  w->weight = 1;
	  res->word_mask |= (1 << v->word_id);
	  res->bool_mask |= (1 << rw->boolean_id);
	}
    }
}

static void
lm_got_word(uns pos, uns type, word_id_t p, void *user UNUSED)
{
  pos++;	/* We want to be consistent with the chewer's numbering. Not needed, but nice. */
  ft_got_word(p, pos, type);
}

static void
lm_got_complex(uns pos, uns type, word_id_t root, word_id_t context, uns dir, void *user UNUSED)
{
  pos++;
  ft_got_word(root, pos, type);
  ASSERT(ft_current_results->words[context.ftw_index].pos == POS_NOWHERE ||
	 ft_current_results->words[context.ftw_index].pos == (dir ? pos+1 : pos-1));
}

#define LM_TRACK_TEXT
#define LM_MAP_URL
#include "indexer/lexmap.h"

static void
ft_init_bests(struct ref_context *c, struct ft_results *res)
{
  for (uns i=0; i<NUM_BESTS; i++)
    res->bests[i] = (struct ft_best) { .pos = POS_NOWHERE };
  bzero(res->best_break, sizeof(uns) * c->num_words);
}

static void
ft_record_best(struct ft_results *res, uns pos, uns weight, uns wid)
{
  struct ft_best *bests = res->bests;

  if (bests[NUM_BESTS-1].weight >= weight ||
      res->best_break[wid] >= weight)
    return;

  uns i = 0;
  uns cnt = 0;
  for (i=0; bests[i].weight > weight; i++)
    {
      if (res->bests[i].word_index == wid)
	cnt++;
    }

  ASSERT(i < NUM_BESTS && cnt < BESTS_PER_WORD);
  struct ft_best this = { .pos = pos, .weight = weight, .word_index = wid };
  struct ft_best next;
  while (i < NUM_BESTS)
    {
      next = bests[i];
      if (this.word_index == wid)
	{
	  cnt++;
	  if (cnt > BESTS_PER_WORD)
	    break;
	  else if (cnt == BESTS_PER_WORD)
	    res->best_break[wid] = this.weight;
	}
      bests[i++] = this;
      this = next;
    }
}

void
ft_match(struct ref_context *c, struct ft_results *res)
{
  struct query *q = c->query;
  ft_current_context = c;
  ft_current_results = res;

  ft_expand_matches(c, res, 1024);
  res->num_words = 0;
  res->word_mask = 0;
  res->bool_mask = 0;

  lm_doc_start(NULL);
  if (res->pos != POS_NOWHERE)
    lm_set_pos(res->pos);

  ft_trail_start(c);
  switch (res->input_type)
    {
    case FT_TEXT:
      DBG("FT: Matching TEXT");
      lm_map_text(res->text, res->text_end);
      break;
    case FT_META: ;
      byte *t = res->text;
      if (*t >= '0' && *t <= '3')
	res->meta_subtype = *t++ - '0';
      else
	res->meta_subtype = 0;
      ASSERT(*t >= 0x80 && *t < 0xa0);
      DBG("FT: Matching META (type %x, subtype %d)", *t & 0x0f, res->meta_subtype);
      lm_map_text(t, res->text_end);
      break;
    case FT_URL:
      DBG("FT: Matching URL");
      lm_map_url(res->text, res->text_end); /* FIXME: Decode escapes? */
      break;
    default:
      ASSERT(0);
    }
  ft_trail_end(c);

  uns ftw_index = ft_add_word();
  struct ft_word *w = res->words + ftw_index;
  w->pos = POS_NOWHERE;
  w->orig = res->text_end;
  w->olen = 0;
  w->weight = 0;

  /* There might remain some words in the list with pos==POS_NOWHERE;
   * lm_got_{word,context}() has not been called for them.  For example,
   * context words without a context (surrounded by two breaks) or
   * ignored words.  Let us fill the last known position there so that the
   * binary search will work.  */
  if (res->words[0].pos == POS_NOWHERE)
    res->words[0].pos = res->pos;
  for (uns i=0; i<res->num_words; i++)
    if (res->words[i].pos == POS_NOWHERE)
      res->words[i].pos = res->words[i-1].pos;
  res->pos = MIN(lm_get_pos(), POS_NOWHERE);

#ifdef LOCAL_DEBUG
  DBG("Constructed trail");
  for (struct trail_entry *t=c->trail; t->pos != POS_NOWHERE; t++)
    DBG("\t@%x $%d w%d q=%d", t->pos, t->weight, t->word_index, t->q);
#endif

  /* Match phrases */
  for (uns i=0; i<q->nphrases; i++)
    {
      struct phrase *p = &c->phrases[i];
      DBG("Matching phrase %d", i);
      if ((p->word_mask & res->bool_mask) != p->word_mask)
	DBG("\tMissing words (have %04x need %04x)", res->bool_mask, p->word_mask);
      else if (match_phrase(c, p) >= 0)
	{
	  DBG("\tMatched");
	  res->bool_mask |= 1 << p->boolean_id;
	}
      else
	DBG("\tNo match");
    }

  /* Match nears */
  for (uns i=0; i<q->nnears; i++)
    {
      struct phrase *p = &c->nears[i];
      DBG("Matching near %d", i);
      uns have_mask = p->word_mask & res->bool_mask;
      if (!(have_mask & (have_mask-1)))
	DBG("\tToo few words (have %04x need %04x)", res->bool_mask, p->word_mask);
      else if (match_near(c, p) > 0)
	DBG("\tMatched");
      else
	DBG("\tNo match");
    }

  /* Merge Q's from the trail to the word list */
  DBG("Finished trail");
  ft_init_bests(c, res);
  w = res->words;
  for (struct trail_entry *t=c->trail; t->pos != POS_NOWHERE; t++)
    {
      DBG("\t@%x $%d w%d q=%d", t->pos, t->weight, t->word_index, t->q);
      while (w->pos != t->pos)
	w++;
      w->weight = MAX(w->weight, t->q);
      ft_record_best(res, t->pos, t->q, t->word_index);
    }

#ifdef LOCAL_DEBUG
  DBG("Match results (word_mask=%x, bool_mask=%x)", res->word_mask, res->bool_mask);
  for (struct ft_word *w = res->words; w<res->words + res->num_words; w++)
    DBG("\t<%.*s> @%x type=%d w=%d", w->olen, w->orig, w->pos, w->type, w->weight);
  for (uns i=0; i<NUM_BESTS; i++)
    {
      struct ft_best *b = &res->bests[i];
      if (b->pos != POS_NOWHERE)
	DBG("Best %d: @%x w=%d wid=%d", i, b->pos, b->weight, b->word_index);
    }
  int see = simplified_eval_expr(q->prep_expr, res->bool_mask);
  DBG("Whole query match: %s", (see > 0) ? "yes" : (see < 0) ? "no" : "neutral");
#endif
}

void
fulltext_init(void)
{
  lm_init();
}
