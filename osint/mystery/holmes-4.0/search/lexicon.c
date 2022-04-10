/*
 *	Sherlock Search Engine -- Lexicon and Related Operations
 *
 *	(c) 1997-2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"
#include "ucw/unaligned.h"
#include "ucw/mempool.h"
#include "ucw/hashfunc.h"
#include "ucw/unicode.h"
#include "charset/unicat.h"
#include "search/sherlockd.h"
#include "search/lexicon.h"
#include "search/vocabolario.h"

#include <string.h>
#include <alloca.h>

/*** General functions ***/

uns
word_unaccent_utf8(byte *w, byte *to)
{
  uns u;
  byte *stop = to + MAX_WORD_BYTES;
  byte *buf = to;

  while (*w)
    {
      w = utf8_get(w, &u);
      u = Uunaccent(u);
      buf = utf8_put(buf, u);
      if (buf >= stop)
	return 0;
    }
  *buf = 0;
  return buf - to;
}

/*** The lexicon: all known words, order is described in doc/file-formats ***/

void
lex_extract(uns lex_id, byte *buf)
{
  struct lex_entry *l = lex_get(lex_id);
  ASSERT(l->length <= MAX_WORD_BYTES);
  memcpy(buf, l->w, l->length);
  buf[l->length] = 0;
}

void
lex_extract_noacc(uns lex_id, byte *buf)
{
  struct lex_entry *l = lex_get(lex_id);
  uns u;
  byte *w, *we;

  w = l->w;
  we = w + l->length;
  ASSERT(l->length <= MAX_WORD_BYTES);
  while (w < we)
    {
      w = utf8_get(w, &u);
      u = Uunaccent(u);
      buf = utf8_put(buf, u);
    }
  *buf = 0;
}

/*
 *  Find first lexicon entry greater or equal to the given word
 *  in accent-less ordering (it's primary order of the lexicon).
 */

uns
lex_find_first(uns len, byte *key)
{
  int l, r, m, res;
  byte w[MAX_WORD_BYTES+1];

  l = current_dbase->lex_by_len[len];
  r = current_dbase->lex_by_len[len+1];
  DBG("lex_find_first: len=%d, l=%d, r=%d", len, l, r);
  while (l < r)
    {
      m = (l+r)/2;
      lex_extract_noacc(m, w);
      res = strcmp(w, key);
      DBG("(%d,%d,%d) <%s> %c <%s>", l, r, m, w, (res ? (res < 0) ? '<' : '>' : '='), key);
      if (res < 0)
	l = m + 1;
      else
	r = m;
    }
  return l;
}

int
lex_find_exact(byte *w)
{
  byte wunacc[MAX_WORD_BYTES+1], lex[MAX_WORD_BYTES+1];
  uns id, len;

  if (!word_unaccent_utf8(w, wunacc))
    return -1;
  len = utf8_strlen(w);
  id = lex_find_first(len, wunacc);
  while (id < current_dbase->lex_by_len[len+1])
    {
      lex_extract_noacc(id, lex);
      if (strcmp(lex, wunacc))
	break;
      lex_extract(id, lex);
      if (!strcmp(lex, w))
	return id;
      id++;
    }
  return -1;
}

/*
 *  Lexical exceptions: for each lexicon we need to remember all words with
 *  class different from WC_NORMAL. This is needed for proper lexmapping
 *  of cards and also of queries.
 */

struct word_exc {
  snode n;
  byte *w;
  byte len;
  byte word_class;
};

static void
word_exc_add(struct database *db, byte *w, uns len, enum word_class class)
{
  struct word_exc *ex = mp_alloc(db->pool, sizeof(*ex));
  slist_add_tail(&db->wexc_list, &ex->n);
  ex->w = w;
  ex->len = len;
  ex->word_class = class;
}

static void
word_exc_voc_add(struct vocabolario *voc, struct word_exc *ex, byte *w)
{
  voc_key_t key;
  struct vocabolo *v;

  if (!voc_key_utf8(voc, &key, w))
    return;
  v = voc_find(voc, &key);
  if (v)
    {
      if (v->word_class != ex->word_class)
	log(L_ERROR, "Inconsistent lexicon: class clash on <%s> (%d != %d)", w, v->word_class, ex->word_class);
      return;
    }
  v = voc_insert(voc, &key);
  v->word_class = ex->word_class;
  v->word_id = 0xff;
  v->penalty = 0;
  v->noaccent_only = 0;
  v->lang_mask = 0;
}

static uns
word_exc_commit(struct database *db)
{
  uns cnt = 0;
  SLIST_FOR_EACH(struct word_exc *, ex, db->wexc_list)
    cnt++;
  db->wexc_vocabolario = voc_new(db->pool, 2*cnt, NULL);
  struct vocabolario *voc = db->wexc_vocabolario;
  SLIST_FOR_EACH(struct word_exc *, ex, db->wexc_list)
    {
      byte x[MAX_WORD_BYTES+1], y[MAX_WORD_BYTES+3];

      for (uns i=0; i<ex->len; i++)
	x[i] = ex->w[i];
      x[ex->len] = 0;

      word_exc_voc_add(voc, ex, x);
      if (word_unaccent_utf8(x, y))
	word_exc_voc_add(voc, ex, y);
    }
  return voc->num_items;
}

int
query_word_classify(struct database *db, byte *word)
{
  byte w[MAX_WORD_BYTES+3];
  struct vocabolo *v;
  voc_key_t key;

  if (!word_unaccent_utf8(word, w) ||
      !voc_key_utf8(db->wexc_vocabolario, &key, w))
    return WC_GARBAGE;

  if (v = voc_find(db->wexc_vocabolario, &key))
    return v->word_class;
  return -1;
}

/*** Loading of lexicon: words and complexes ***/

static void
lex_load(struct database *db)
{
  uns i, ecount, last_len;
  byte *lex, *lex_end;

  byte *fn_lex = db_file_name(db, "lexicon");
  lex = mmap_file(fn_lex, &db->lexicon_file_size, 0);
  if (db->lexicon_file_size < 8)
    die("Corrupted lexicon %s", fn_lex);
  lex_end = lex + db->lexicon_file_size;
  db->lexicon_words = ((u32*)lex)[0];
  db->lexicon_complexes = ((u32*)lex)[1];

  db->lex_array = xmalloc(sizeof(struct lex_entry *) * (db->lexicon_words + 2*HARD_MAX_WORDS));
#ifdef CONFIG_CONTEXTS
  byte ct_flags[lex_context_slots];
  bzero(ct_flags, sizeof(ct_flags));
#endif
  lex += 8;
  last_len = 0;
  for (i=0; i<db->lexicon_words; i++)
    {
      struct lex_entry *l = (struct lex_entry *) lex;
      uns len = utf8_strnlen(l->w, l->length);
      ASSERT(len <= MAX_WORD_CHARS);
      while (last_len <= len)
	db->lex_by_len[last_len++] = i;
      db->lex_array[i] = l;
      switch (l->class)
	{
	case WC_NORMAL:
	  if (len < lex_min_len)
	    word_exc_add(db, l->w, l->length, l->class);
	  break;
	case WC_COMPLEX:
	  ASSERT(0);
	case WC_CONTEXT:
#ifdef CONFIG_CONTEXTS
	  {
	    uns ctxt = GET_CONTEXT(&l->ctxt);
	    ASSERT(ctxt < lex_context_slots);
	    ct_flags[ctxt] = 1;
	  }
#else
	  ASSERT(0);
#endif
	  /* fall-thru */
	default:
	  word_exc_add(db, l->w, l->length, l->class);
	}
      lex += sizeof(struct lex_entry) + l->length;
    }
  while (last_len <= MAX_WORD_CHARS+1)
    db->lex_by_len[last_len++] = i;
  while (i < db->lexicon_words + HARD_MAX_WORDS)
    {
      /* We use HARD_MAX_WORDS positions after the last real word for temporary words in no particular order */
      db->lex_array[i++] = NULL;
    }

#ifdef CONFIG_CONTEXTS
  if (db->lexicon_complexes)
    {
      db->cplx_array = xmalloc(sizeof(struct lex_entry **) * lex_context_slots);
      i = 0;
      for (uns ct=0; ct<lex_context_slots; ct++)
	if (ct_flags[ct])
	  {
	    struct lex_entry **ca = db->cplx_array[ct] = xmalloc(sizeof(struct lex_entry *) * 2*lex_context_slots);
	    for (uns j=0; j < 2*lex_context_slots; j++)
	      {
		struct lex_entry *l = (struct lex_entry *) lex;
		ASSERT(l->class == WC_COMPLEX && !l->length);
		ASSERT(GET_CONTEXT(&l->ctxt) == ct);
		ca[j] = l;
		lex += sizeof(struct lex_entry);
		i++;
	      }
	  }
      ASSERT(i == db->lexicon_complexes);
    }
#endif
  ecount = word_exc_commit(db);
  log(L_INFO, "Loaded word index %s: %d words, %d complexes, %d exceptions",
      db->name, db->lexicon_words, db->lexicon_complexes, ecount);
  ASSERT(lex == lex_end);
}

/*** The stem array ***/

#ifdef CONFIG_LANG

static void
stems_load(struct database *db)
{
  clist_init(&db->stem_block_list);
  clist_init(&db->syn_block_list);
  u32 *ary = mmap_file(db_file_name(db, "stems"), &db->stems_file_size, 0);
  u32 *ary_end = ary + db->stems_file_size/4;
  uns stem_block_count = 0, syn_block_count = 0;
  struct syn_block *open_syn_block = NULL;
  while (ary < ary_end)
    {
      u32 id = *ary++;
      u32 lmask = *ary++;
      u32 *astart = ary;
      while (*ary != ~0U)
	{
	  ASSERT(ary < ary_end);
	  ary++;
	}
      uns aitems = ary - astart;
      ary++;

      if (id < 0x80000000)
	{
	  struct stemmer *st;
	  struct stem_block *sb = NULL;
	  CLIST_WALK(st, stemmer_list)
	    if (st->lang_mask == lmask && (uns)st->id == id)
	      {
		sb = mp_alloc(db->pool, sizeof(struct stem_block));
		sb->stemmer = st;
		sb->array = astart;
		sb->array_items = aitems;
		clist_add_tail(&db->stem_block_list, &sb->n);
		break;
	      }
	  if (sb)
	    {
	      stem_block_count++;
	      continue;
	    }
	}
      else if (id == 0x80000000 && !open_syn_block)
	{
	  open_syn_block = mp_alloc(db->pool, sizeof(struct syn_block));
	  open_syn_block->lang_mask = lmask;
	  open_syn_block->direct_array = astart;
	  open_syn_block->direct_items = aitems;
	  continue;
	}
      else if (id == 0x80000001 && open_syn_block)
	{
	  syn_block_count++;
	  ASSERT(open_syn_block->lang_mask == lmask);
	  open_syn_block->inverse_array = astart;
	  open_syn_block->inverse_items = aitems;
	  clist_add_tail(&db->syn_block_list, &open_syn_block->n);
	  open_syn_block = NULL;
	  continue;
	}
      die("Stemmer block (id=%08x,lm=%08x) unrecognized", id, lmask);
    }
  if (open_syn_block)
    log(L_ERROR, "Incomplete synonymic block encountered");
  log(L_INFO, "Loaded word mappings: %d stemmer blocks, %d synonymic blocks", stem_block_count, syn_block_count);
}

u32 *
stem_lookup_expansion(u32 *ary, uns nitems, u32 stem_id)
{
  int l, r, mstart, mstop;

  l = 0;
  r = nitems;
  while (l < r)
    {
      mstart = (l+r)/2;
      mstop = mstart+1;
      while (!(ary[mstart] & 0x80000000))
	mstart--;
      while (!(ary[mstop] & 0x80000000))
	mstop++;
      u32 key = ary[mstart] & 0x7fffffff;
      if (key == stem_id)
	return &ary[mstart];
      else if (key < stem_id)
	l = mstop;
      else
	r = mstart;
    }
  return NULL;
}

#endif

/*** Global initialization ***/

void
lexicon_init(struct database *db)
{
#ifdef CONFIG_LANG
  static int stemmers_inited;
  if (!stemmers_inited++)
    lang_init_stemmers();
#endif
  lex_load(db);
#ifdef CONFIG_LANG
  stems_load(db);
#endif
}
