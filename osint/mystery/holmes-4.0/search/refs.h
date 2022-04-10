/*
 *	Sherlock Search Engine -- Processing of References
 *
 *	(c) 1997--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#include "ucw/bbuf.h"
#include "ucw/unaligned.h"
#include "ucw/unicode.h"

#ifdef CONFIG_LANG
#include "lang/lang.h"
#endif

/*
 * We keep most buffers pre-allocated in growing arrays, to avoid re-allocating
 * them for each query or even during processing of the query.
 *
 * If we run multiple threads for slices of a single index in parallel, we use
 * a separate buffer set for each thread to avoid locking.
 */

struct ref_buffers {
  bb_t ref_heap;
  bb_t raw_chains;
  bb_t sliced_chains;
  bb_t matched_chains;
  bb_t fulltext_words;
  struct trail_entry *trail_buf;
  uns trail_buf_size;
  bb_t image_sims_chains;
};

extern struct ref_buffers *ref_buffers;

void *ref_buf_alloc(bb_t *b, uns size);

/*
 * We keep the current status of the reference processing in our local structures.
 * The key reasons are optimizing cache usage and, more importantly, that we might
 * want to run several threads in parallel, each working on a part of the index.
 *
 * Each ref_context also contains a local heap for keeping the best matches
 * and there heaps are later merged to a single global result heap, which is
 * a bit more powerful and which can merge results coming from different databases.
 */

#define R(x) local_match_##x		/* struct local_match_heap et al. */
#define R_NOTE_FIELDS
#define R_SITE_TYPE uns
#define R_GET_SITE(_n) ((_n)->n.attr->site_id)
#include "search/results.h"

#define R(x) global_match_##x		/* struct global_match_heap et al. */
#define R_NOTE_FIELDS u64 site_hash;
#define R_SITE_TYPE u64
#define R_GET_SITE(_n) (_n)->site_hash
#include "search/results.h"

struct ref_word {
  uns boolean_id;			/* Copied from struct word */
  uns type_mask;
  uns doc_count;
  int weight;
  byte is_outer;
  byte is_string;
  byte rfu[2];
  int q, q2;				/* Best matches recorded for the current OID */
  int pos, pos2;
  uns seen_types;			/* In which types has this word been seen */
  uns (*meta_weight_array)[16][4];	/* Pointers to the relevant weight arrays */
  int *weight_array;
#ifdef CONFIG_EXPLAIN
  uns explain_ref, explain_ref2;	/* Ref entries remembered for EXPLAINing */
#endif
  struct word *query_word;		/* Used only outside hot paths */
};

struct ref_context {
  struct query *query;
  struct database *dbase;
  struct card_attr *attrs;
  uns site_only;			/* site id corresponding to site hash in this database */
  uns num_words;
  uns num_chains;
  struct chain *chains;
#ifdef CONFIG_EXPLAIN
  uns do_explain;
#endif
#define CUSTOM_MATCH_KWD(id,keywd,parse) u32 id##_value;
  CUSTOM_MATCH_PARSE
#undef CUSTOM_MATCH_KWD
  struct local_match_heap match_heap;
  struct ref_heap_entry *ref_heap;
  struct chain_match *matched_chains;
  struct stats stats;
  struct trail_entry *trail, *trail_stop;
  uns trail_size;
  uns trail_sorted;
  struct phrase *phrases;
  uns num_phrases;
  struct phrase *nears;
  uns num_nears;
  struct ref_buffers *buffers;
  struct chain *raw_chains;		/* Raw chains before slice selection takes place */
  uns num_raw_chains;
  uns thread_id;
  uns thread_slice_mask;
  uns start_oid;
  uns end_oid;

  /* Variables internal to fulltext.c */
  struct vocabolario *ft_voc;		/* Vocabolario used during fulltext matching */
  uns ft_lang_mask;
  uns ft_is_accented;

#ifdef CONFIG_IMAGES_SIM
  /* Similar images */
  uns nimage_sims;
  struct image_sim *image_sims;
  uns image_sims_bool_mask;
  uns image_sims_bools_seen;
#endif

  CUSTOM_REF_VARS

  struct ref_word words[];
};

struct ref_context *init_bare_ref_context(struct query *q, struct ref_buffers *buffers);

/* Internal representation of reference chains */

struct chain {
  byte *pos;
  u32 lang_mask;		/* Extracted from struct variant */
  byte noaccent_only;
  byte penalty;
  byte word_index;		/* Which word does this chain belong to */
  byte bool_index;		/* And its boolean ID for quick lookup */
};

/* Parser of chain lengths */

static inline uns
get_chain_len(byte **pp)
{
  byte *p = *pp;
  uns len = GET_U32(p) >> 28;
  p += 4;
  if (!len)
    p = utf8_32_get(p, &len);
  *pp = p;
  return len;
}

/*
 * The trail buffer contains a restriction of the current document to words
 * present in the query, that is all reference chains of interest merged,
 * weighted and filtered. Behind-the-edge entries are not stored.
 * Position POS_NOWHERE marks the end of the trail.
 */

struct trail_entry {
  ref_pos_t pos;
  s8 weight;
  byte word_index;
  u16 q;		/* This is not used by refs.c, but with 32-bit ref_pos_t it just occupies padding. */
};

struct trail_entry *trail_enlarge(struct ref_context *c, struct trail_entry *t);

static inline struct trail_entry *
trail_start(struct ref_context *c)
{
  return c->trail;
}

static inline void
trail_end(struct ref_context *c, struct trail_entry *t)
{
  c->trail_size = t - c->trail;
  c->trail_sorted = 0;
}

static inline struct trail_entry *
trail_add_entry(struct ref_context *c, struct trail_entry *t)
{
  if (unlikely(t >= c->trail_stop))
    t = trail_enlarge(c, t);
  return t;
}

/* fulltext.c uses a slightly different interface for its convenience */

static inline void
ft_trail_start(struct ref_context *c)
{
  c->trail_size = 0;
}

static inline struct trail_entry *
ft_trail_add(struct ref_context *c)
{
  return trail_add_entry(c, &c->trail[c->trail_size++]);
}

static inline void
ft_trail_end(struct ref_context *c)
{
  ft_trail_add(c)->pos = POS_NOWHERE;
  c->trail_sorted = 1;
}

/* Matchers */

int match_phrase(struct ref_context *c, struct phrase *p);
int match_near(struct ref_context *c, struct phrase *p);

/* Correction of word type mask according to lanuages and accents */

static inline uns
tweak_type_mask(uns type_mask, uns lang_mask UNUSED, uns noaccent_only, uns doc_lang_mask UNUSED, uns doc_is_accented)
{
#ifdef CONFIG_LANG
  if (!(lang_mask & doc_lang_mask))
    {
      uns lang_type_mask = WORD_TYPES_ALL_LANGS | (META_TYPES_ALL_LANGS << 16);
      if (lang_mask & (1 << LANG_NONE))
	lang_type_mask |= WORD_TYPES_NO_LANG | (META_TYPES_NO_LANG << 16);
      type_mask &= lang_type_mask;
    }
  else if (!(lang_mask & (1 << LANG_NONE)))
    type_mask &= ~(WORD_TYPES_NO_LANG | (META_TYPES_NO_LANG << 16));
#endif
  if (noaccent_only)
    {
      type_mask &= ~(WORD_TYPES_AUTO_ACCENT_ALWAYS_STRICT | (META_TYPES_AUTO_ACCENT_ALWAYS_STRICT << 16));
      if (doc_is_accented)
	type_mask &= WORD_TYPES_AUTO_ACCENT_ALWAYS_STRIP | (META_TYPES_AUTO_ACCENT_ALWAYS_STRIP << 16);
    }
  return type_mask;
}
