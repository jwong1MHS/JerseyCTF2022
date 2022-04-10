/*
 *	Sherlock Search Engine -- Processing of References
 *
 *	(c) 1997--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG
#undef DEBUG_HEAP
#undef DEBUG_SLICES

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/unaligned.h"
#include "ucw/heap.h"
#include "ucw/prefetch.h"
#include "ucw/unicode.h"
#include "ucw/threads.h"
#include "sherlock/index.h"
#include "indexer/params.h"
#include "search/sherlockd.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <alloca.h>
#include <pthread.h>

#ifdef DEBUG_HEAP
#define HDBG(x...) log(L_DEBUG, x)
#else
#define HDBG(x...) do { } while(0)
#endif

#ifdef DEBUG_SLICES
#define SDBG(x...) log(L_DEBUG, x)
#else
#define SDBG(x...) do { } while(0)
#endif

/* Declare all internal data structures and emit code for dealing with them */
#define R_EMIT_CODE
#include "search/refs.h"

#define INFTY 1000000		/* All Q's and weights are between -INFTY and INFTY */
#define PEN_INFTY 1024		/* The same for word bonuses and penalties */

/* The ref_heap is used for merging of reference chains on oid level */

struct ref_heap_entry {
  oid_t oid;
  struct chain *chain;
};

#define REF_HEAP_LESS(a,b) (a.oid < b.oid)
#define REF_HEAP_SWAP(heap,a,b,t) (t=heap[a], heap[a]=heap[b], heap[b]=t)

/* And here we remember the chains we are currently working on */

struct chain_match {
  struct chain *chain;
  byte *pos, *stop;
};

/* Allocation of buffers */

struct ref_buffers *ref_buffers;

static void
init_ref_buffers(struct ref_buffers *b)
{
  bb_init(&b->ref_heap);
  bb_init(&b->raw_chains);
  bb_init(&b->sliced_chains);
  bb_init(&b->matched_chains);
  bb_init(&b->fulltext_words);
  b->trail_buf_size = 1024;
  b->trail_buf = xmalloc(b->trail_buf_size * sizeof(struct trail_entry));
  bb_init(&b->image_sims_chains);
}

void *
ref_buf_alloc(bb_t *b, uns size)
{
  return (void *) bb_grow(b, size);
}

/* Building and sorting the trail */

static inline void
trail_init(struct ref_context *c)
{
  c->trail = c->buffers->trail_buf;
  c->trail_stop = c->trail + c->buffers->trail_buf_size;
}

struct trail_entry *
trail_enlarge(struct ref_context *c, struct trail_entry *t)
{
  struct ref_buffers *buf = c->buffers;
  uns pos = t - c->trail;
  if (pos >= buf->trail_buf_size)
    {
      /* The above condition can be false if we have multiple trails sharing a common buffer. */
      buf->trail_buf_size *= 2;
      buf->trail_buf = xrealloc(buf->trail_buf, buf->trail_buf_size * sizeof(struct trail_entry));
    }
  trail_init(c);
  return c->trail + pos;
}

#define ASORT_PREFIX(x) trail_##x
#define ASORT_KEY_TYPE uns
#define ASORT_ELT(i) trail[i].pos
#define ASORT_SWAP(i,j) do { struct trail_entry tmp=trail[i]; trail[i]=trail[j]; trail[j]=tmp; } while (0)
#define ASORT_EXTRA_ARGS , struct trail_entry *trail
#include "ucw/sorter/array-simple.h"

static inline struct trail_entry *
get_trail(struct ref_context *c)
{
  if (!c->trail_sorted)
    {
      DBG("\tSorting trail:");
      c->trail_sorted = 1;
      trail_sort(c->trail_size, c->trail);
      c->trail[c->trail_size].pos = POS_NOWHERE;
#ifdef LOCAL_DEBUG
      for (uns i=0; c->trail[i].pos != POS_NOWHERE; i++)
	DBG("\t\t@%x $%d w%d", c->trail[i].pos, c->trail[i].weight, c->trail[i].word_index);
#endif
    }
  return c->trail;
}

/* The reference context and mapping of chains */

static void
init_ref_word(struct ref_context *c, struct word *w, struct ref_word *r)
{
  r->boolean_id = w->boolean_id;
  r->type_mask = w->type_mask;
  r->doc_count = 0;
  r->weight = w->weight;
  r->is_outer = w->is_outer;
  r->is_string = w->is_string;
  r->meta_weight_array = &c->dbase->meta_weights;
  r->weight_array = (w->is_string ? c->dbase->string_weights : c->dbase->word_weights);
  r->query_word = w;
}

static void
map_chains(struct ref_context *c)
{
  struct query *q = c->query;
  uns n = 0;

  for (uns i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      SLIST_FOR_EACH(struct variant *, v, w->variants)
	if (v->refchain_len)
	  n++;
    }
  c->num_raw_chains = q->stat_num_chains = n;
  c->raw_chains = ref_buf_alloc(&c->buffers->raw_chains, (c->num_raw_chains + q->nimage_sims) * sizeof(struct chain));

  struct mmap_request *mm = alloca(sizeof(struct mmap_request) * n);
  DBG("Reference chains requested:");
  uns j = 0;
  q->stat_len_chains = 0;
  for (uns i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      SLIST_FOR_EACH(struct variant *, v, w->variants)
	if (v->refchain_len)
	  {
	    struct chain *ch = &c->raw_chains[j];
	    ch->lang_mask = v->lang_mask;
	    ch->noaccent_only = v->noaccent_only;
	    ch->penalty = v->penalty;
	    ch->word_index = i;
	    ch->bool_index = w->boolean_id;
	    DBG("\t%d: @%llx+%x word=%d bool=%d nonacc=%d pen=%d lmask=%08x", j, (long long)v->refchain_start,
		  v->refchain_len, ch->word_index, ch->bool_index, ch->noaccent_only, ch->penalty, ch->lang_mask);
	    mm[j].u.req.fd = q->dbase->fd_refs;
	    mm[j].u.req.start = v->refchain_start;
	    mm[j].u.req.end = MIN(v->refchain_start + (ucw_off_t)v->refchain_len, q->dbase->ref_file_size);
	    mm[j].userdata = j;
	    w->ref_total_len += v->refchain_len;
	    q->stat_len_chains += v->refchain_len;
	    j++;
	  }
    }
  ASSERT(j == n);
  if (mmap_regions(q, mm, n) < 0)
    {
      add_err("-117 Too many documents match");
      eval_err(317);
    }
  for (uns i=0; i<n; i++)
    {
      uns j = mm[i].userdata;
      c->raw_chains[j].pos = mm[i].u.map.start;
    }
}

static void
select_slices_trivial(struct ref_context *c)
{
  /*
   * Set up chain pointers if the index contains only a single slice.
   * I.e., just use the chains as they are.
   */
  c->chains = c->raw_chains;
  c->num_chains = c->num_raw_chains;
  c->start_oid = 0;
  c->end_oid = c->dbase->params->cards_out;
}

static void
select_slices(struct ref_context *c, uns slice)
{
  /*
   * Set up chain pointers in the given ref context to point to the beginning of the
   * selected slice, filtering out empty slices.
   */
  c->chains = ref_buf_alloc(&c->buffers->sliced_chains, c->num_raw_chains * sizeof(struct chain));
  c->num_chains = 0;
  c->start_oid = c->dbase->slice_start[slice];
  c->end_oid = c->dbase->slice_start[slice + 1];
  uns num_slices = c->dbase->params->num_slices;
  DBG("Selecting slice #%d:", slice);
  for (uns i=0; i<c->num_raw_chains; i++)
    {
      struct chain *orig = &c->raw_chains[i];
      struct chain *new = &c->chains[c->num_chains];
      byte *p = orig->pos;
      uns slice_mask = *p++;
      if (slice_mask & (1 << slice))
	{
	  uns pos = 0;
	  for (uns j=0; j<num_slices; j++)
	    if (slice_mask & (1 << j))
	      {
		slice_mask &= ~(1 << j);
		if (slice_mask)
		  {
		    uns size;
		    p = utf8_32_get(p, &size);
		    if (j < slice)
		      pos += size;
		  }
	      }
	  DBG("\t%d: @%d", i, (uns)(p-orig->pos) + pos);
	  *new = *orig;
	  new->pos = p + pos;
	  c->num_chains++;
	}
      else
	DBG("\t%d: empty", i);
    }
}

struct ref_context *
init_bare_ref_context(struct query *q, struct ref_buffers *buffers)
{
  /*
   * Initialize basic parts of a ref_context, enough to make high-level matching
   * functions like phrase matching work. Multiple bare ref_contexts can share
   * a single set of ref_buffers, as long as the trails are not used simultaneously.
   */

  struct ref_context *c = mp_alloc(q->pool, sizeof(*c) + q->nwords*sizeof(struct ref_word));
  DBG("Creating new ref context %p", c);
  bzero(c, sizeof(*c));
  c->query = q;
  c->dbase = q->dbase;
  c->attrs = q->dbase->card_attrs;
  c->buffers = buffers;
  init_stats(q, &c->stats);
  trail_init(c);

  c->num_words = q->nwords;
  for (uns i=0; i<q->nwords; i++)
    init_ref_word(c, q->words[i], &c->words[i]);
  c->phrases = mp_alloc(q->pool, q->nphrases * sizeof(struct phrase));
  for (uns i=0; i<q->nphrases; i++)
    c->phrases[i] = *q->phrases[i];
  c->num_phrases = q->nphrases;
  c->nears = mp_alloc(q->pool, q->nnears * sizeof(struct phrase));
  for (uns i=0; i<q->nnears; i++)
    c->nears[i] = *q->nears[i];
  c->num_nears = q->nnears;
  image_bare_ref_context_init(c);

  return c;
}

static struct ref_context *
init_ref_context(struct query *q, struct ref_buffers *buffers, struct ref_context *clone)
{
  /* Initialize a full ref context, including reference chains and result heaps. */

  struct ref_context *c = init_bare_ref_context(q, buffers);
#ifdef CONFIG_SITES
  c->site_only = site_find_id(&q->dbase->sites, q->site_hash);
#endif
  c->match_heap.max_matches = q->results->max_matches;
  c->match_heap.site_max = q->site_max;
  local_match_init(q->pool, &c->match_heap);

  if (!clone)
    map_chains(c);
  else
    {
      c->raw_chains = clone->raw_chains;
      c->num_raw_chains = clone->num_raw_chains;
    }
  image_ref_context_init(c, clone);
  c->ref_heap = ref_buf_alloc(&c->buffers->ref_heap, sizeof(struct ref_heap_entry) * (c->num_raw_chains+1));
  c->matched_chains = ref_buf_alloc(&c->buffers->matched_chains, sizeof(struct chain_match) * (c->num_raw_chains+1));

  CUSTOM_REF_INIT(c);

  return c;
}

/* Merging of results and statistics */

static void
merge_ref_context(struct ref_context *c)
{
  struct query *q = c->query;
  HDBG("Merging ref context %p to global results (%d results)", c, c->match_heap.num_matches);

  /* Add all local statistics to the global ones */
  merge_stats(q, &c->stats, &q->stats);
  for (uns i=0; i<c->num_words; i++)
    q->words[i]->doc_count += c->words[i].doc_count;
  for (uns i=0; i<c->num_phrases; i++)
    q->phrases[i]->matches += c->phrases[i].matches;
  for (uns i=0; i<c->num_nears; i++)
    q->nears[i]->matches += c->nears[i].matches;

  /* Copy all results from local match heap to the global one */
  struct local_match_heap *lh = &c->match_heap;
  struct global_match_heap *gh = q->match_heap;
  struct local_match_note *ln;
  struct global_match_note *gn;
#ifdef CONFIG_SITES
  /*
   * If we have two documents from the same site, clear the second one's
   * site compression counter to avoid adding it twice to the global total.
   */
  if (lh->site_max)
    for (uns i=1; i<=lh->num_matches; i++)
      {
	ln = lh->heap[i];
	if (ln->hash_next && ln->hash_next->n.attr->site_id == ln->n.attr->site_id)
	  ln->hash_next->n.site_compressed = 0;
      }
  gh->site_compressed += lh->site_compressed;
#endif
  while (ln = local_match_delete_min(lh))
    {
      HDBG("Match with Q=%d, ssk=%x", ln->n.q, ln->n.sec_sort_key);
      gn = global_match_get_note(gh);
      gn->n = ln->n;
#ifdef CONFIG_SITES
      gn->site_hash = site_find_hash(&q->dbase->sites, ln->n.attr->site_id);
      HDBG("\tTranslated site ID %08x -> hash %016Lx (compressed %d)", ln->n.attr->site_id, gn->site_hash, gn->n.site_compressed);
#endif
      global_match_insert(gh, gn);
    }

  CUSTOM_REF_CLEANUP(c);
}

/* Explanation mode */

#ifdef CONFIG_EXPLAIN
#define EXPLAIN(c, x...) do { if (unlikely(c->do_explain)) add_reply(".E" x); } while(0)
#define IF_EXPLAINING(x) x

static void
explain_lookup_type(byte *buf, uns e, uns is_string)
{
  int l;
  if (e & 0x10)
    l = snprintf(buf, 64, "%s[%d]", mt_names[e&15], e>>16);
  else
    l = snprintf(buf, 64, "%s", (is_string ? st_names : wt_names)[e]);
  ASSERT(l > 0 && 64 > l);
}

#else
#define EXPLAIN(c, x...) do { } while(0)
#define IF_EXPLAINING(x) do { } while(0)
#endif

/* Matching of phrases and proximity matching */

int
match_phrase(struct ref_context *c, struct phrase *p)
{
  int wpos[MAX_PHRASE_LEN], wwt[MAX_PHRASE_LEN];
  uns last_idx = p->length - 1;
  int bestq = prox_limit-1 + p->weight;

  for (uns idx=0; idx<p->length; idx++)
    {
      wpos[idx] = 0;
      wwt[idx] = -INFTY;
    }

  for (struct trail_entry *trail = get_trail(c); trail->pos != POS_NOWHERE; trail++)
    {
      uns idx = p->word_to_idx[trail->word_index];
      while (idx)
	{
	  idx--;
	  uns pos = trail->pos;
	  int wt = trail->weight * word_weight_scale;
	  DBG("\t\t@%x idx=%d wt=%d", pos, idx, wt);

	  /* Try to extend the current part of the phrase */
	  int wrel = wwt[idx];
	  if (p->prox_map & (1 << idx))
	    {
	      if (idx)
		wt += wwt[idx-1] - prox_penalty*(uns)(pos - (wpos[idx-1] + p->relpos[idx]));
	      wrel -= prox_penalty*(uns)(pos - (wpos[idx] + 1));
	    }
	  else
	    {
	      wrel = -INFTY;
	      if (idx)
		{
		  if (pos == wpos[idx-1] + p->relpos[idx])
		    wt += wwt[idx-1];
		  else
		    wt = -INFTY;
		}
	    }
	  if (wrel <= wt && wt >= -INFTY)
	    {
	      wpos[idx] = pos;
	      wwt[idx] = wt;
	    }
	  DBG("\t\t\t-> pos=%d wt=%d", wpos[idx], wwt[idx]);

	  /* If it was the last word of the phrase, record the weight */
	  if (idx == last_idx && wwt[idx] > -INFTY)
	    {
	      int qq = wwt[idx] + p->weight;
	      if (qq > bestq)
		bestq = qq;
	      if (qq >= 0)
		trail->q = MIN(trail->q + qq, 65535);
	    }

	  /* Go to the next word */
	  idx = p->next_same_word[idx];
	}
    }

  if (bestq < prox_limit + p->weight)
    return -1;
  else
    {
      p->matches++;
      return bestq;
    }
}

/* Near matching */

int
match_near(struct ref_context *c, struct phrase *p)
{
  /* Currently examined word cluster: a circular buffer */
  uns cr = 0, cw = 0;			/* Read and write index */
  struct {
    uns idx;				/* Word index */
    int q;				/* Q of the word itself */
    int joiner;				/* Weight of space before the word */
    uns mul;                            /* Word weight */
    uns pos;				/* Position in the text */
  } cluster[MAX_PHRASE_LEN];
  uns cluster_word_mask = 0;		/* Which words are present in the cluster */
  int running_weight = 0;		/* Near points of the cluster */
  int running_q = 0;			/* Total Q of the cluster */
  uns last_pos = -10000;		/* Position of previous word */
  uns last_idx = -10000;		/* Which word it was */
  int joiner;
  uns running_mul = 0;                  /* Sum of word weights in the cluster */
  uns running_div = 0;                  /* Number of words in the cluster */

  /* Current maxima */
  int bestw = -1;
  int bestq = -1;

  /*
   *  How does the near matcher work:
   *
   *  (1) Assign each position in the document near points which is either:
   *	     near_bonus_word for matched words
   *	     +near_bonus_connect more if previous word is also matched
   *	     -near_penalty_gap for no match
   *  (2) Among intervals containing each word at most once, find the one
   *      with maximum Q (that is, Q of words inside according to standard
   *      rules + the near points gathered in this interval multiplied by
   *      the average weight of matched words in this interval) and record
   *      it as a normal match covering the whole interval.
   *  (3) Do the same for the second best interval.
   *
   *  The code below does all these steps in parallel and returns maximum number
   *  of near points gained which is then added to the overall Q of the page
   *  (Q's of word occurences are already accounted for).
   */

  for (struct trail_entry *trail = get_trail(c); trail->pos != POS_NOWHERE; trail++)
    {
      uns pos = trail->pos;
      uns idx = p->word_to_idx[trail->word_index];
      struct ref_word *word = &c->words[trail->word_index];
      uns word_mul = CLAMP(trail->weight, (int)near_min_weight, (int)near_max_weight);
      while (idx)
        {
	  idx--;
	  DBG("\t\t@%x idx=%d", pos, idx);

	  /* Calculate gap weight and adjust the running sum */
	  if (pos - last_pos > 100 || idx == last_idx)
	    goto reset_matcher;
          joiner = (last_idx + 1 == idx) ? near_bonus_connect : 0;
          if (pos > last_pos)
	    joiner += (last_pos - pos) * near_penalty_gap;
	  running_weight += joiner;
	  if (running_weight < 0)
	    goto reset_matcher;

	  /* Check for duplicate words */
	  if (cluster_word_mask & (1 << idx))
	    {
	      /* Remove all words until first occurence of the offending one */
	      while (cluster[cr].idx != idx)
	        {
	          running_div--;
	          running_mul -= cluster[cr].mul;
	          cr = (cr + 1) % MAX_PHRASE_LEN;
	        }
	      running_div--;
	      running_mul -= cluster[cr].mul;
	      cr = (cr + 1) % MAX_PHRASE_LEN;

	      /* Recalculate weight and word mask */
	      running_weight = running_q = 0;
	      cluster_word_mask = 0;
	      uns ct = cr;
	      for (uns ci = cr; ci != cw; ci = (ci + 1) % MAX_PHRASE_LEN)
	        {
	          if (ci != ct)
	            {
	              running_weight += cluster[ci].joiner;
	              if (running_weight < 0)
		        {
			  ct = ci;
			  running_weight = running_q = 0;
			  cluster_word_mask = 0;
			}
		    }
		  running_weight += near_bonus_word;
		  running_q += cluster[ci].q;
		  cluster_word_mask |= (1 << cluster[ci].idx);
	        }
	      while (cr != ct)
	        {
	          running_div--;
	          running_mul -= cluster[cr].mul;
	          cr = (cr + 1) % MAX_PHRASE_LEN;
	        }
	    }

	  /* Add the new word */
	  cluster[cw].joiner = joiner;
	  running_mul += word_mul;
	  running_div++;
	  cluster[cw].q = word->weight + trail->weight * word_weight_scale;
	  running_q += cluster[cw].q;

	  /* Update current maxima */
	  int total_weight = running_weight * running_mul / running_div;
	  int qq = running_q + total_weight;
	  if (qq >= 0)
	    trail->q = MAX(trail->q, MIN(qq, 65535));
	  if (qq > bestq)
	    {
	      bestq = qq;
	      bestw = total_weight;
	    }

	  running_weight += near_bonus_word;
	  goto continue_matcher;

reset_matcher:
	  /* Reset matcher to the current word only */
	  cr = cw = 0;
	  cluster_word_mask = 0;
	  running_q = cluster[cw].q = word->weight + trail->weight * word_weight_scale;
	  running_weight = near_bonus_word;
	  running_mul = word_mul;
	  running_div = 1;
	  last_pos = -10000;

continue_matcher:
	  cluster[cw].pos = pos;
	  cluster[cw].idx = idx;
	  cluster[cw].mul = word_mul;
	  cluster_word_mask |= (1 << idx);
	  cw = (cw+1) % MAX_PHRASE_LEN;

	  /* And go further */
	  last_pos = pos + 1;
	  last_idx = idx;

	  DBG("\t\t-> rw=%d rq=%d rm=%d rd=%d cr=%d cw=%d words=%x (best: %d, bestw: %d)",
	      running_weight, running_q, running_mul, running_div, cr, cw, cluster_word_mask,
	      bestq, bestw);

	  /* Go to the next word */
	  idx = p->next_same_word[idx];
	}
    }

  DBG("\t\tNear matcher score: %d", bestw);
  if (bestw <= 0)
    return 0;
  p->matches++;
  return bestw;
}

#define MATCH_INT_ATTR(id,keywd,gf,pf)				\
  {								\
    u32 a = gf(attrs);						\
    if (a < q->id##_min || a > q->id##_max)			\
      return 0;							\
  }
#define MATCH_SMALL_SET_ATTR(id,keywd,gf,pf)			\
  {								\
    uns a = gf(attrs);						\
    if (!(q->id##_set & (1 << a)))				\
      return 0;							\
  }

static inline int
match_early_card_attrs(struct ref_context *c UNUSED, struct query *q UNUSED, struct card_attr *attrs UNUSED, u32 oid UNUSED)
{
#ifdef CONFIG_EXPLAIN
  if (unlikely(q->explain_id))
    {
      if (q->explain_id != oid)
	return 0;
      c->do_explain = 1;
    }
  else
    c->do_explain = 0;
#endif

#define INT_ATTR(id,keywd,gf,pf) MATCH_INT_ATTR(id,keywd,gf,pf)
#define SMALL_SET_ATTR(id,keywd,gf,pf) MATCH_SMALL_SET_ATTR(id,keywd,gf,pf)
#define LATE_INT_ATTR(id,keywd,gf,pf)
#define LATE_SMALL_SET_ATTR(id,keywd,gf,pf)
  EXTENDED_ATTRS
#undef INT_ATTR
#undef SMALL_SET_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR

#ifdef CONFIG_LASTMOD
  if (attrs->age < q->age_min || attrs->age > q->age_max)
    return 0;
#endif
#ifdef CONFIG_SITES
  if (c->site_only && c->site_only != attrs->site_id)
    return 0;
#endif
  if (attrs->flags & CARD_FLAG_DUP)
    return 0;
  return 1;
}

static inline int
match_late_card_attrs(struct query *q UNUSED, struct card_attr *attrs UNUSED)
{
#define INT_ATTR(id,keywd,gf,pf)
#define SMALL_SET_ATTR(id,keywd,gf,pf)
#define LATE_INT_ATTR(id,keywd,gf,pf) MATCH_INT_ATTR(id,keywd,gf,pf)
#define LATE_SMALL_SET_ATTR(id,keywd,gf,pf) MATCH_SMALL_SET_ATTR(id,keywd,gf,pf)
  EXTENDED_ATTRS
#undef INT_ATTR
#undef SMALL_SET_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR
  return 1;
}

static void
get_sec_sort_key(struct result_note *note, struct ref_context *c, struct card_attr *attrs UNUSED, oid_t oid)
{
  struct query *q = c->query;
  u32 key = 0;
  switch (q->custom_sorting)
    {
    case PARAM_CARDID:
      key = oid;
      break;
    case PARAM_SITE:
#ifdef CONFIG_SITES
      key = attrs->site_id;
#else
      key = 0;
#endif
      break;
#ifdef CONFIG_LASTMOD
    case PARAM_AGE:
      key = attrs->age;
      break;
#endif
#define INT_ATTR(id,keywd,gf,pf)			\
    case OFFSETOF(struct query, id##_min):		\
      key = gf(attrs);					\
      break;
#define SMALL_SET_ATTR(id,keywd,gf,pf)			\
    case OFFSETOF(struct query, id##_set):		\
      key = gf(attrs);					\
      break;
#define LATE_INT_ATTR INT_ATTR
#define LATE_SMALL_SET_ATTR SMALL_SET_ATTR
  EXTENDED_ATTRS
#undef INT_ATTR
#undef SMALL_SET_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR

#define CUSTOM_MATCH_KWD(id,kwd,pf)			\
    case OFFSETOF(struct query, id##_placeholder):	\
      key = c->id##_value;				\
      break;
CUSTOM_MATCH_PARSE
#undef CUSTOM_MATCH_KWD
    }
  note->sec_sort_key = key ^ q->custom_sort_reverse;
}

static inline void
refs_card_record(struct ref_context *c, struct card_attr *attr, oid_t oid, int qq, uns static_weight_mul)
{
  struct local_match_note *note = local_match_get_note(&c->match_heap);
  /* static_weight_mul can be zero for ANY or IMAGESIM query */
  qq += attr->weight * doc_weight_scale * static_weight_mul;
  if (c->query->custom_sort_only)
    qq = 0;
  EXPLAIN(c, "Static weight %d x %d (Q=%d) => total Q=%d", attr->weight, static_weight_mul,
    attr->weight * doc_weight_scale * static_weight_mul, qq);
  HDBG("Matched OID %08x with Q=%d", oid, qq);
  note->n.attr = attr;
  note->n.q = qq;
#ifdef CONFIG_SITES
  note->n.site_compressed = 0;
#endif
  get_sec_sort_key(&note->n, c, attr, oid);
  local_match_insert(&c->match_heap, note);
}

static void UNUSED
image_sim_explain_msg(byte *s UNUSED, void *param UNUSED)
{
  EXPLAIN(((struct ref_context *)param), "%s", s);
}

static void
refs_card(struct ref_context *c, struct card_attr *attr, oid_t oid, u32 words_found)
{
  struct query *q = c->query;
  int qq = 0;
  uns bool = 0;
  uns static_weight_mul = 0;

#ifdef CUSTOM_HACK_WORD_INIT
  CUSTOM_HACK_WORD_INIT(q);
#endif

  /* Skim over all matched words and record their matches */
  for (uns wid=0; words_found; wid++)
    if (words_found & (1 << wid))
      {
	words_found &= ~(1 << wid);
	struct ref_word *word = &c->words[wid];
	if (word->q > -PEN_INFTY)	/* Really matched */
	  {
	    word->doc_count++;
	    if (word->is_outer)
	      {
		int wq1 = (uns)word->q*word_weight_scale;
		int wq2 = 0;
		int wq = word->weight + wq1;
		if (wq >= 0)
		  {
		    DBG("\tWord %d: best %d (@%x), 2nd best %d (@%x)", wid, word->q, word->pos, word->q2, word->pos2);
		    if (word->q2 >= 0)
		      {
		        wq2 = (uns)word->q2*word_weight_scale / second_best_reduce;
		        wq += wq2;
		      }
		    DBG("\t\t=> Q=%d (%d+%d+%d)", wq, word->weight, wq1, wq2);
		    IF_EXPLAINING(
		      if (unlikely(c->do_explain))
		        {
		          add_reply(".EWord <%s>: Q=%d", word->query_word->word, wq);
		          add_reply(".E\tBase: %d", word->weight);
		          byte typebuf[64];
		          explain_lookup_type(typebuf, word->explain_ref, word->is_string);
		          add_reply(".E\t1st match: %s (%d) => %d", typebuf, word->q, wq1);
		          if (word->q2 >= 0)
			    {
			      explain_lookup_type(typebuf, word->explain_ref2, word->is_string);
			      add_reply(".E\t2nd match: %s (%d) => %d", typebuf, word->q2, wq2);
			    }
		        }
		    );
		    qq += wq;
		    if (word->weight)
		      static_weight_mul++;
		  }
	      }
	    bool |= 1 << word->boolean_id;
#ifdef CUSTOM_HACK_WORD_MATCH
	    CUSTOM_HACK_WORD_MATCH(q, wid, word);
#endif
	  }
      }

  /* Check optimistic boolean expression again */
  DBG("2nd optimistic bool mask: %04x", bool);
  if (!(q->optimistic_bool_map[bool >> 5] & (1 << (bool & 31))))
    {
      DBG("No match: !optimistic2");
      return;
    }

#ifdef LOCAL_DEBUG
  get_trail(c);
#endif

  /* Process phrases */
  for (uns i=0; i<q->nphrases; i++)
    {
      struct phrase *p = &c->phrases[i];
      int pq;
      DBG("\tMatching phrase #%d", i);
      if ((p->word_mask & bool) != p->word_mask)
	DBG("\t\tMissing words (have %04x need %04x)", bool, p->word_mask);
      else if ((pq = match_phrase(c, p)) >= 0)
	{
	  EXPLAIN(c, "Phrase #%d: Q=%d", i, pq);
	  qq += pq;
	  static_weight_mul++;
	  bool |= 1 << p->boolean_id;
#ifdef CUSTOM_HACK_PHRASE_MATCH
	  CUSTOM_HACK_PHRASE_MATCH(q, p);
#endif
	}
    }

#ifdef CONFIG_IMAGES_SIM
  /* Process images */
  for (uns i = 0; i<c->nimage_sims; i++)
    {
      struct image_sim *s = c->image_sims + i;
      int sq;
      if (c->image_sims_bools_seen & (1 << s->boolean_id))
        {
	  sq = image_sim_q(s);
	  IF_EXPLAINING(
            if (unlikely(c->do_explain))
	      {
	        EXPLAIN(c, "ImageSim #%d: Q=%d", i, sq);
	        image_sim_explain(c, s, oid, image_sim_explain_msg, c);
	      }
	  );
	  qq += sq;
	  /* static_weight_mul++; */
	  bool |= 1 << s->boolean_id;
	}
    }
#endif

  /* Check boolean expression */
  DBG("Final bool mask: %04x", bool);
  if (!(q->bool_map[bool >> 5] & (1 << (bool & 31))))
    {
      DBG("No match: !bool");
      return;
    }

  /* Check custom matchers */
#ifdef CUSTOM_MATCH
  int custom_Q = 0;
  if (!CUSTOM_MATCH(q, c, attr, custom_Q))
    {
      DBG("No match: custom");
      return;
    }
  EXPLAIN(c, "Custom matcher: Q=%d", custom_Q);
  qq += custom_Q;
#endif

  /* Update custom statistics and process late attribute matchers */
  EXTENDED_EARLY_STATS(q, (&c->stats), attr);
  if (!match_late_card_attrs(q, attr))
    {
      DBG("No match: late attrs");
      return;
    }
  c->stats.matching_docs++;
  EXTENDED_LATE_STATS(q, (&c->stats), attr);

  /* Process near-matchers to get the final weight */
  for (uns i=0; i<q->nnears; i++)
    {
      struct phrase *p = &c->nears[i];
      u32 have_mask = p->word_mask & bool;
      DBG("\tMatching near #%d", i);
      /* This test is tricky. You are not expected to understand it. */
      if (!(have_mask & (have_mask-1)))
        {
          DBG("\t\tToo few words (have %04x want %04x)", bool, p->word_mask);
          EXPLAIN(c, "Near #%d: Unmatched", i);
        }
      else
        {
          int nq = match_near(c, p);
          EXPLAIN(c, "Near #%d: Q=%d", i, nq);
          qq += nq;
        }
    }

  /* Record the match */
  refs_card_record(c, attr, oid, qq, static_weight_mul);
}

#ifdef CONFIG_ALLOW_ANY
static void
refs_card_any(struct ref_context *c, struct card_attr *attr, oid_t oid)
{
  struct query *q = c->query;
  int qq = 0;
  uns static_weight_mul = 0;

  if (!match_early_card_attrs(c, q, attr, oid))
    {
      DBG("\tEarly attributes don't match");
      return;
    }

  /* Check custom matchers */
#if defined(CUSTOM_MATCH_ANY) || defined(CUSTOM_MATCH)
  int custom_Q = 0;
#ifdef CUSTOM_MATCH_ANY
  if (!CUSTOM_MATCH_ANY(q, c, attr, custom_Q))
#else
  if (!CUSTOM_MATCH(q, c, attr, custom_Q))
#endif
    {
      DBG("No match: custom");
      return;
    }
  EXPLAIN(c, "Custom matcher: Q=%d", custom_Q);
  qq += custom_Q;
#endif

  /* Update custom statistics and process late attribute matchers */
  EXTENDED_EARLY_STATS(q, (&c->stats), attr);
  if (!match_late_card_attrs(q, attr))
    {
      DBG("No match: late attrs");
      return;
    }
  c->stats.matching_docs++;
  EXTENDED_LATE_STATS(q, (&c->stats), attr);

  /* Record the match */
  refs_card_record(c, attr, oid, qq, static_weight_mul);
}
#endif

static uns
refs_slice_find_type_mask(byte **refchain, uns want_mask, uns want_all, uns *found_mask)
{
  byte *pos = *refchain;
  uns card_id, found = 0;
  while (card_id = GET_U32(pos) & 0x0fffffff)
    {
      uns len = get_chain_len(&pos);
      byte *end = pos + len;
      while (pos < end)
        {
	  uns x = *pos++, type, mask;
	  if (x < 0xe0) /* Delta-encoded word types */
	    {
	      if (x < 0x80) /* 0tpp pppp */
		type = x >> 6;
	      else if (x < 0xc0) /* 10pp pttt +u8 */
	        {
		  type = x & 7;
		  pos++;
		}
	      else /* 110p pttt +u16 */
	        {
		  type = x & 7;
		  pos += 2;
		}
	    }
	  else if (x < 0xf8) /* Meta types */
	    {
	      if (x < 0xf0) /* 1110 tttt wwpp pppp */
	        {
		  type = x & 0x0f;
		  pos++;
		}
	      else /* 1111 0tww pppp pttt +u8 */
	        {
		  type = ((x & 4) << 1) | (*pos & 7);
		  pos += 2;
		}
	      type += 16;
	    }
#ifdef CONFIG_32BIT_REFERENCES
	  else if (x < 0xfc) /* 1111 10tt tppp pppp +u16 (large word delta) */
	    {
	      type = (x & 3) | ((*pos & 0x80) >> 5);
	      pos += 3;
	    }
	  else if (x < 0xfe) /* 1111 110w wppp tttt +u16 (large meta absolute) */
	    {
	      type = (*pos & 0x0f) + 16;
	      pos += 3;
	    }
#endif
	  else
	    ASSERT(0);
	  if (mask = (1 << type) & want_mask)
	    {
	      found |= mask;
	      if (!want_all)
		break;
	    }
	}
      if (found)
	break;
    }
  *refchain = pos;
  *found_mask = found;
  return card_id;
}

uns
refs_chain_find_type_mask(byte **refchain, uns want_mask, uns want_all, uns *found_mask, uns have_slices)
{
  if (!have_slices)
    return refs_slice_find_type_mask(refchain, want_mask, want_all, found_mask);
  else
    {
      byte *p = *refchain;
      uns slice_mask = *p++, slice_count = 0, card_id = 0, size;
      for (; slice_mask; slice_mask >>= 1)
        if ((slice_mask & 1) && slice_count++)
	  p = utf8_32_get(p, &size);
      for (uns i = 0; i < slice_count; i++)
        {
	  card_id = refs_slice_find_type_mask(&p, want_mask, want_all, found_mask);
	  if (card_id)
	    break;
	  p += 4;
	}
      *refchain = p;
      return card_id;
    }
}

static u32
refs_chains(struct ref_context *c, struct card_attr *attr, struct chain_match *mch)
{
  uns is_accented = attr->flags & CARD_FLAG_ACCENTED;
  struct trail_entry *trail = trail_start(c);
  u32 words_seen = 0;
#ifdef CONFIG_LANG
  u32 lang_mask = 1 << CA_GET_FILE_LANG(attr);
#else
  u32 lang_mask = ~0U;
#endif

  for (; mch->chain; mch++)
    {
      struct chain *ch = mch->chain;
      byte *p = mch->pos;
      byte *stop = mch->stop;
      uns word_index = ch->word_index;
      struct ref_word *w = &c->words[word_index];
      int *weight_array = w->weight_array;
      uns (*meta_weight_array)[16][4] = w->meta_weight_array;

      uns type_mask = tweak_type_mask(w->type_mask, ch->lang_mask, ch->noaccent_only, lang_mask, is_accented);
      DBG("\tRef chain %d: word %d, type_mask %x", (int)(ch-c->chains), word_index, type_mask);

      if (!(words_seen & (1 << word_index)))
	{
	  /* bit set in words_seen doesn't necessarily imply the word's been really matched */
	  words_seen |= 1 << word_index;
	  w->q = w->q2 = -PEN_INFTY;
	  w->seen_types = 0;
	}

      uns last_wpos = 0;
      while (p < stop)
	{
	  uns x = *p++;
	  uns pos, type, meta_weight;
	  int weight;
	  IF_EXPLAINING(uns explain_ref);
	  if (x < 0xe0)				/* Delta-encoded word types */
	    {
	      uns delta;
	      if (x < 0x80)				/* 0tpp pppp */
		{
		  type = x >> 6;
		  delta = x & 0x3f;
		}
	      else if (x < 0xc0)			/* 10pp pttt +u8 */
		{
		  type = x & 7;
		  delta = ((x & 0x38) << 5) | *p++;
		}
	      else					/* 110p pttt +u16 */
		{
		  type = x & 7;
		  delta = ((x & 0x18) << 13);
		  delta |= GET_U16(p);
		  p += 2;
		}
	      weight = weight_array[type];
	      if (!delta)
		pos = POS_NOWHERE;
	      else
		pos = (last_wpos += delta);
	      IF_EXPLAINING(explain_ref = type);
	    }
	  else if (x < 0xf8)			/* Meta types */
	    {
	      if (x < 0xf0)				/* 1110 tttt wwpp pppp */
		{
		  type = x & 0x0f;
		  meta_weight = *p >> 6;
		  pos = *p++ & 0x3f;
		}
	      else					/* 1111 0tww pppp pttt +u8 */
		{
		  meta_weight = x & 3;
		  type = ((x & 4) << 1) | (*p & 7);
		  pos = GET_U16(p) >> 3;
		  p += 2;
		}
	      weight = (*meta_weight_array)[type][meta_weight];
	      pos += POS_FIRST_META | (type << POS_META_SHIFT);
	      type += 16;
	      IF_EXPLAINING(explain_ref = type | (meta_weight << 16));
	    }
#ifdef CONFIG_32BIT_REFERENCES
	  else if (x < 0xfc)			/* 1111 10tt tppp pppp +u16 (large word delta) */
	    {
	      type = (x & 3) | ((*p & 0x80) >> 5);
	      uns delta = (*p++ & 0x7f) << 16;
	      delta |= GET_U16(p);
	      p += 2;
	      pos = (last_wpos += delta);
	      weight = weight_array[type];
	      IF_EXPLAINING(explain_ref = type);
	    }
	  else if (x < 0xfe)			/* 1111 110w wppp tttt +u16 (large meta absolute) */
	    {
	      meta_weight = (x & 1) | ((*p & 0x80) >> 6);
	      type = *p & 0x0f;
	      pos = (*p++ & 0x70) << 12;
	      pos |= GET_U16(p);
	      p += 2;
	      weight = (*meta_weight_array)[type][meta_weight];
	      pos += POS_FIRST_META | (type << POS_META_SHIFT);
	      type += 16;
	      IF_EXPLAINING(explain_ref = type | (meta_weight << 16));
	    }
#endif
	  else
	    ASSERT(0);

	  if (!(type_mask & (1 << type)))
	    {
	      DBG("\t\t@%x t%d UNMATCHED", pos, type);
	      continue;
	    }
	  DBG("\t\t@%x t%d w%d-p%d", pos, type, weight, ch->penalty);
	  w->seen_types |= 1 << type;
	  weight -= ch->penalty;

	  if (pos == POS_NOWHERE)
	    weight -= blind_match_penalty;
	  else
	    {
	      trail = trail_add_entry(c, trail);
	      trail->pos = pos;
	      trail->weight = weight;
	      trail->word_index = word_index;
	      trail++;
	    }
	  if (w->q < weight)
	    {
	      w->q2 = w->q;
	      w->pos2 = w->pos;
	      w->q = weight;
	      w->pos = pos;
	      IF_EXPLAINING(w->explain_ref2 = w->explain_ref);
	      IF_EXPLAINING(w->explain_ref = explain_ref);
	    }
	  else if (w->q != weight && w->q2 < weight)
	    {
	      w->q2 = weight;
	      IF_EXPLAINING(w->explain_ref2 = explain_ref);
	    }
	}
    }

  trail_end(c, trail);
  return words_seen;
}

static void
refs_go(struct ref_context *c)
{
  struct query *q = c->query;
  struct card_attr *attrs = c->attrs;
  uns rcnt = c->num_chains;
  struct ref_heap_entry *rheap = c->ref_heap;

  DBG("Initializing chains:");
  for (uns i=0; i<rcnt; i++)
    {
      struct chain *ch = &c->chains[i];
      rheap[i+1].oid = GET_U32(ch->pos) & 0x0fffffff;
      rheap[i+1].chain = ch;
      DBG("\t%d: first oid is %08x", i, rheap[i+1].oid);
    }
  HEAP_INIT(struct ref_heap_entry, rheap, rcnt, REF_HEAP_LESS, REF_HEAP_SWAP);

#ifdef CONFIG_ALLOW_ANY
  oid_t last_oid = c->start_oid;
#endif

  while (rcnt > 0)
    {
      oid_t oid = rheap[1].oid;

#ifdef CONFIG_ALLOW_ANY
      if (q->bool_map[0] & 1)
	while (++last_oid != oid)
	  refs_card_any(c, attrs + last_oid, last_oid);
      last_oid = oid;
#endif

      struct card_attr *attr = &attrs[oid];
      prefetch(attr);
      DBG("### OID %x", oid);

      uns bool = 0;
      struct chain_match *mch = c->matched_chains;
      while (rcnt > 0 && rheap[1].oid == oid)
	{
	  struct chain *ch = rheap[1].chain;
	  byte *p = ch->pos;
	  uns len = get_chain_len(&p);

	  DBG("\tFound in ref chain %d size %d", (int)(ch-c->chains), len);
#ifdef CONFIG_IMAGES_SIM
	  if (c->image_sims_bool_mask & (1 << ch->bool_index))
	    c->image_sims[ch->word_index].dist = *(u32 *)p;
	  else
#endif
	    {
	      mch->chain = ch;
	      mch->pos = p;
	      mch->stop = p + len;
	      mch++;
	    }
	  bool |= (1 << ch->bool_index);

	  p += len;
	  oid_t next_oid = GET_U32(p) & 0x0fffffff;
	  if (next_oid)
	    {
	      DBG("\t\t... next OID is %08x", next_oid);
	      prefetch(&attrs[next_oid]);
	      ch->pos = p;
	      ASSERT(rheap[1].oid < next_oid);
	      rheap[1].oid = next_oid;
	      HEAP_INCREASE(struct ref_heap_entry, rheap, rcnt, REF_HEAP_LESS, REF_HEAP_SWAP, 1);
	    }
	  else
	    {
	      DBG("\t\t... chain end");
	      HEAP_DELMIN(struct ref_heap_entry, rheap, rcnt, REF_HEAP_LESS, REF_HEAP_SWAP);
	    }
	}
      DBG("Optimistic bool mask: %04x", bool);

      if (!match_early_card_attrs(c, q, attr, oid))
	{
	  DBG("\tEarly attributes don't match");
	  continue;
	}

      /* Check optimistic boolean expression */
      if (!(q->optimistic_bool_map[bool >> 5] & (1 << (bool & 31))))
	{
	  DBG("No match: !optimistic");
	  continue;
	}

      mch->chain = NULL;
      u32 words_seen = refs_chains(c, attr, c->matched_chains);
#ifdef CONFIG_IMAGES_SIM
      c->image_sims_bools_seen = bool;
#endif
      refs_card(c, attr, oid, words_seen);
    }

#ifdef CONFIG_ALLOW_ANY
  if (q->bool_map[0] & 1)
    for (struct card_attr *attr = attrs + ++last_oid; last_oid < c->end_oid; last_oid++, attr++)
      refs_card_any(c, attr, last_oid);
#endif
}

static void
process_refs_single(struct ref_context *c)
{
  uns num_slices = c->dbase->params->num_slices;
  if (num_slices == 1)
    {
      SDBG("--> Processing the only slice");
      select_slices_trivial(c);
      refs_go(c);
    }
  else
    {
      for (uns i=0; i<num_slices; i++)
	{
	  SDBG("--> Processing slice %d", i);
	  select_slices(c, i);
	  if (c->num_chains)
	    refs_go(c);
	}
    }
  merge_ref_context(c);
}

static void *
ref_thread(void *arg)
{
  struct ref_context *c = arg;

  for (uns i=0; i < c->dbase->params->num_slices; i++)
    if (c->thread_slice_mask & (1 << i))
      {
	SDBG("--> Thread #%d processing slice %d", c->thread_id, i);
	select_slices(c, i);
	refs_go(c);
      }
  SDBG("--> Thread #%d finished", c->thread_id);
  return c;
}

static void
process_refs_threaded(struct ref_context *c)
{
  uns num_slices = c->dbase->params->num_slices;
  uns num_threads = MIN(num_slices, slice_threads);
  pthread_t threads[num_threads];
  struct ref_context *ctxt[num_threads];

  pthread_attr_t attr;
  if (pthread_attr_init(&attr) < 0 ||
      pthread_attr_setstacksize(&attr, ucwlib_thread_stack_size) < 0)
    ASSERT(0);

  /*
   * The pthread interface is pretty silly, because it doesn't allow to wait
   * for an arbitrary thread. Therefore we don't try to balance the load and
   * we assign a fixed set of slices to each thread instead.
   */

  for (uns i=0; i<num_threads; i++)
    {
      struct ref_context *tc = i ? init_ref_context(c->query, &ref_buffers[i], c) : c;
      ctxt[i] = tc;
      tc->thread_id = i;
      tc->thread_slice_mask = 0;
      for (uns s=i; s<num_slices; s+=num_threads)
	tc->thread_slice_mask |= (1 << s);
      SDBG("--> Thread #%d started with slice_mask=%08x", i, tc->thread_slice_mask);
      if (pthread_create(&threads[i], &attr, ref_thread, tc) < 0)
	die("Unable to create thread: %m");
    }
  for (uns i=0; i<num_threads; i++)
    {
      void *ret;
      if (pthread_join(threads[i], &ret) < 0)
	die("Unable to join thread: %m");
      SDBG("--> Thread #%d joined", i);
      ASSERT(ret == ctxt[i]);
      merge_ref_context(ctxt[i]);
    }

  pthread_attr_destroy(&attr);
}

void
process_refs(struct query *q)
{
  /* Initialize the main ref_context and map all chains */
  prof_t *oldp = profiler_switch(&prof_reff);
  struct ref_context *c = init_ref_context(q, &ref_buffers[0], NULL);

  /* Process references, possibly in several passes if there are multiple slices */
  profiler_switch(&prof_refs);
  if (slice_threads <= 1 || c->dbase->params->num_slices == 1)
    process_refs_single(c);
  else
    process_refs_threaded(c);

  profiler_switch(oldp);
}

void
refs_init(void)
{
  ASSERT(slice_threads);
  ref_buffers = xmalloc(slice_threads * sizeof(struct ref_buffers));
  for (uns i=0; i<slice_threads; i++)
    init_ref_buffers(&ref_buffers[i]);
}

void
query_init_refs(struct query *q)
{
  struct global_match_heap *h = mp_alloc(q->pool, sizeof(*h));
  h->max_matches = q->results->max_matches;
  h->site_max = q->site_max;
  global_match_init(q->pool, h);
  q->match_heap = h;
}

void
query_finish_refs(struct query *q)
{
  struct results *r = q->results;
  struct global_match_heap *h = q->match_heap;

  /* Extract all entries from the global match heap, sort them and store them */
  uns num = h->num_matches;
  r->matches = mp_alloc(r->pool, sizeof(struct result_note) * num);
  r->num_matches = h->num_matches;
  struct global_match_note *gn;
  uns i = r->num_matches;
  HDBG("Sorting %d matches", num);
  while (gn = global_match_delete_min(h))
    {
      struct result_note *n = &r->matches[--i];
      *n = gn->n;
#ifdef DEBUG_HEAP
      oid_t oid;
      attr_to_db(n->attr, &oid);
      log(L_DEBUG, "%3d %08x %6d ssk=%x sc=%d", i, oid, n->sec_sort_key, n->q,
#ifdef CONFIG_SITES
	  n->site_compressed
#else
	  0
#endif
	  );
#endif
    }
  add_reply("N%d", r->num_matches);
#ifdef CONFIG_SITES
  add_reply("m%d", h->site_compressed);
#endif
}
