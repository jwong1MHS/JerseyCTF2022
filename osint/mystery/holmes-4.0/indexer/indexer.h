/*
 *	Sherlock Indexer
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2002--2006 Robert Spalek <robert@ucw.cz>
 */

#ifndef _SHERLOCK_INDEXER_INDEXER_H
#define _SHERLOCK_INDEXER_INDEXER_H

#include "sherlock/index.h"
#include "ucw/clists.h"

/* iconfig.c */

/* File names */
extern char *fn_directory;
extern char *fn_fingerprints, *fn_fp_splits, *fn_labels_by_id, *fn_attributes, *fn_checksums, *fn_card_info;
extern char *fn_links, *fn_urls, *fn_url_index, *fn_skel_urls, *fn_graph_obj, *fn_graph_skel, *fn_sites, *fn_labels, *fn_merges, *fn_signatures, *fn_matches;
extern char *fn_word_index, *fn_string_index, *fn_references, *fn_string_map, *fn_card_prints;
extern char *fn_string_hash, *fn_cards, *fn_card_attrs, *fn_parameters, *fn_ref_texts;
extern char *fn_lexicon, *fn_lex_raw, *fn_lex_ordered, *fn_lex_words, *fn_lex_by_freq;
extern char *fn_stems, *fn_stems_ordered, *fn_lex_classes, *fn_notes, *fn_notes_skel, *fn_keywords, *fn_feedback_gath;
extern char *fn_blacklist;
extern char *fn_admin_export;

char *index_name(const char *file);
int index_name_defined(const char *file);
#define index_bopen(file, flags, stream) bopen_file(index_name(file), flags, !(stream) ? &indexer_fb_params : &indexer_stream_params)
#define index_maybe_bopen(file, flags, stream) (index_name_defined(file) ? index_bopen(file, flags, stream) : NULL)
#define index_bopen_tmp(stream) bopen_tmp_file(!(stream) ? &indexer_fb_params : &indexer_stream_params)
#define index_atomic_open(file, clone, reclen) fbatomic_open((clone) ? NULL : index_name(file), clone, indexer_fb_size, reclen)
#define index_maybe_atomic_open(file, clone, reclen) (index_name_defined(file) ? index_atomic_open(file, clone, reclen) : NULL)

#define FN_GRAPH_INDEX "-index"
#define FN_GRAPH_REAL "-real"
#define FN_GRAPH_GOES "-goes"
#define FN_GRAPH_DEG "-deg"

/* Sources */
extern clist indexer_sources;

/* Miscellaneous */
extern struct attr_set label_attr_set, link_attr_set, ref_link_attr_set;
extern struct attr_set override_label_attr_set, override_body_attr_set, card_attr_set;
extern struct fb_params indexer_fb_params, indexer_stream_params;
extern uns string_avg_bucket, indexer_fb_size, sort_delete_src;
extern uns progress, progress_screen, progress_status_line;
extern uns ref_max_length, ref_min_length, ref_max_count;
extern uns matcher_signatures, matcher_context, matcher_min_words, matcher_threshold, matcher_passes, matcher_block;
extern uns max_num_objects, min_summed_size, frameset_to_redir, num_slices;
extern uns raw_stage2_input;
extern uns indexer_trace;
extern uns indexer_threads, indexer_thread_stack_size;
extern uns default_weight;
extern uns reject_empty;

/* Filters */
extern char *indexer_filter_name;

/* Images */
extern char *fn_image_thumbnails;
extern char *fn_image_signatures_unsorted, *fn_image_signatures, *fn_image_clusters;
extern uns image_sig_max_cluster_count;

#define PROGRESS_REPORT(i) (progress && !((i) % progress))
#define PROGRESS_PRINT(msg, args...) do { \
	if (progress_status_line) setproctitle(msg, args); \
	if (progress_screen) { printf(msg "\r", args); fflush(stdout); } \
	} while (0)
#define PROGRESS(i, msg, args...) do { if (PROGRESS_REPORT(i)) PROGRESS_PRINT(msg, args); } while (0)
#define PROGRESS_LOCKED(i, lock, msg, args...) do { if (PROGRESS_REPORT(i)) { \
	pthread_mutex_lock(&(lock)); PROGRESS_PRINT(msg, args); pthread_mutex_unlock(&(lock)); } } while (0)

#define ITRACEN(level, msg, args...) do { if (indexer_trace >= level) log(L_DEBUG, msg,##args); } while(0)
#define ITRACE(msg, args...) ITRACEN(1, msg,##args)

#ifdef CONFIG_SHEPHERD_PROTOCOL
/* IConnect configuration */
extern uns ic_connect_timeout, ic_reply_timeout, ic_retry_count, ic_retry_delay, ic_send_feedback;
#endif

/* Subindices */

#define HARD_MAX_SUBINDICES 8		/* Need to update INDEX_ID_SHIFT in chewer.c when increasing this */

struct subindex {
  struct cnode n;
  char *name;
  uns type_mask;
  uns id_mask;
};

extern struct clist subindices;

/* getbuck.c */

struct get_buck {
  struct mempool *pool;			/* Filled in by the caller */

  u32 oid;				/* oid of the current bucket */
  u32 type;				/* type of the current bucket */
  struct odes *o;			/* parsed content of the current bucket */
  uns progress_count;			/* object counter */
  uns progress_current, progress_max;	/* percentage */
  uns num_sources, cur_source;		/* source counters */
  uns index;				/* global oid */

  /* Set up by get_buck_init() */
  struct buck2obj_buf *buck_buf;
};

extern uns gb_max_count;

void get_buck_init(struct get_buck *gb);		/* Init and cleanup are synchronous, get on different contexts is thread-safe */
void get_buck_cleanup(struct get_buck *gb);
int get_buck_next(struct get_buck *gb, uns index);

/* Structure of files */

struct csum {
  byte md5[16];
  u32 cardid;
};

struct link {
  struct fingerprint fp;
  u32 src;
};

#include "indexer/sites.h"

struct card_note {
#ifdef CONFIG_INDEXER_STORE_OID
  u32 oid;				/* ID in the original source (i.e. bucket ID) */
#endif
  u32 useful_size;			/* Useful size (number of alnum characters) */
  area_t area;				/* We need the area for non-downloaded entries as well */
  s16 card_bonus;			/* Bonus assigned by the filter */
  /* These fields track how did the card weight evolve */
  byte weight_scanner;			/* Weight assigned by the scanner */
#ifdef CONFIG_WEIGHTS
  byte weight_dynamic;			/* Resulting dynamic weight */
#endif
  byte weight_merged;			/* Weight after card merging (includes merger penalties) */
  byte flags;				/* CARD_NOTE_xxx */
  site_hash_t site_hash;		/* Hash of the site name */
  byte footprint[16];
};

enum card_note_flag {
  CARD_NOTE_GIANT = 1,			/* Belongs to a very large class, subject to penalties */
  CARD_NOTE_HAS_LINKS = 2,		/* even the unknown ones */
  CARD_NOTE_IS_LINKED = 4,
  CARD_NOTE_IMAGE = 8,			/* Is an image object [set by scanner] */
  CARD_NOTE_REDIRECT = 16,		/* the object is just a redirect */
  CARD_NOTE_AUDIO = 32,			/* Is an audio object [set by scanner] */
  CARD_NOTE_HAS_TARGET = 64,		/* The redirect has a valid destination (direct or indirect) */
};

struct card_info {			/* Card information passed from stage 1 to stage 2 */
  u32 orig_card;			/* Stage 1 ID */
  struct card_attr attr;
  struct card_note note;
};

static inline uns			/* id_mask works according to these id's */
get_subindexing_id(uns card_id UNUSED, struct card_note *note)
{
#ifdef CONFIG_SITES
  return note->site_hash[0] & 0x0f;
#else
  for (uns i=0; i<sizeof(note->footprint); i++)
    if (note->footprint[i])
      return note->footprint[0] & 0x0f;
  return card_id & 0x0f;
#endif
}

typedef float rank_t;

/* Export for Admin */

#define ADMIN_EXPORT_VERSION 0x030d0000

struct admin_export {
  byte footprint[16];
  u32 id;				/* Final ID in subindex */
  byte subindex;			/* Final subindex */
  byte flags;				/* CARD_FLAG_x */
  byte note_flags;			/* CARD_NOTE_x */
  byte weight;				/* Final weight */
  byte weight_scanner;			/* Weights from notes */
  byte weight_oook;			/* ... */
  byte weight_dynamic;
  byte weight_merged;
  byte age;				// card_attr.age */
  byte file_flags;			/* card_attr.type_flags */
  byte file_type;			/* CA_GET_FILE_TYPE */
  byte file_lang;			/* CA_GET_FILE_LANG */
  u32 site;				/* card_attr.site */
  byte rfu[4];
};

/* Labels */

struct lab {				/* Header of a label block */
  u32 merged_id, url_id, redir_id;
  u32 count;
  byte flags;
} PACKED;

#define LABEL_TYPE_BODY		0x01	/* Will be attached to card body (or ignored if the card is empty/dup) */
#define LABEL_TYPE_URL		0x02	/* Will be attached to a per-URL block */
#define LABEL_FLAG_MERGED_ONLY	0x04	/* Ignore if the card isn't merged */
#define LABEL_FLAG_OVERRIDE    	0x08	/* Override attribute of the same name in the card instead of appending */

/* access.c -- helper function for access to indexer data structures */

#include "ucw/partmap.h"

extern uns card_count;			/* Number of cards in the index */
extern uns skel_count;			/* Number of skeletons in the index */

void set_card_count(uns cc);
void set_skel_count(uns sc);

extern struct partmap *notes_partmap, *notes_skel_partmap;

void notes_part_map(uns rw);
void notes_part_unmap(void);
void notes_skel_part_map(uns rw);
void notes_skel_part_unmap(void);

static inline struct card_note *
bring_note_from(struct partmap *map, oid_t card)
{
  return partmap_map(map, sizeof(struct card_note) * (ucw_off_t)card, sizeof(struct card_note));
}

static inline struct card_note *
bring_note(oid_t card)
{
  return bring_note_from(notes_partmap, card);
}

static inline struct card_note *
bring_skel_note(oid_t card)
{
  return bring_note_from(notes_skel_partmap, card);
}

extern struct card_attr *attrs;
extern struct partmap *attrs_partmap;

void attrs_map(uns rw);
void attrs_unmap(void);
void attrs_part_map(uns rw);
void attrs_part_unmap(void);

static inline struct card_attr *
bring_attr(oid_t card)
{
  return partmap_map(attrs_partmap, sizeof(struct card_attr) * (ucw_off_t)card, sizeof(struct card_attr));
}

#define READ_ATTR(var, field) do {						\
	ASSERT(sizeof(*var) == sizeof(((struct card_attr *)0)->field));		\
	var = big_alloc(card_count * sizeof(*var));				\
	for (uns i=0; i<card_count; i++)					\
	  var[i] = bring_attr(i)->field;					\
	} while(0)

#define READ_ATTR_BIT(var, field, mask) do {					\
	var = big_alloc(BIT_ARRAY_BYTES(card_count));				\
	bit_array_zero(var, card_count);					\
	for (uns i=0; i<card_count; i++)					\
	  if (bring_attr(i)->field & (mask))					\
	    bit_array_set(var, i);						\
	} while(0)

#define WRITE_ATTR(var, field) do { 						\
	for (uns i=0; i<card_count; i++)					\
	  bring_attr(i)->field = var[i];					\
	} while(0)

#define FREE_ATTR(var) do { big_free(var, card_count * sizeof(*var)); } while(0)
#define FREE_ATTR_BIT(var) do { big_free(var, BIT_ARRAY_BYTES(card_count)); } while(0)

struct index_params;
void params_load(struct index_params *params);
void params_save(struct index_params *params);

uns alloc_read_ary(const char *name, const char *suffix, void *Ptr, uns count, uns record);
void write_free_ary(const char *name, const char *suffix, void *Ptr, uns count, uns record);

/* fetch.c */

extern uns fetch_id, fetch_num_ids;
void fetch_cards(void (*got_card)(struct card_info *info, struct odes *obj));

/* resolve.c */

extern uns resolve_trace, resolve_threads, resolve_prefetch, resolve_batch_size;
extern double resolve_max_hash_density;

#define RESOLVE_SKIP_UNKNOWN	1
#define RESOLVE_SKIP_SKEL	2
#define RESOLVE_SPLIT		0x80000000

#define FIRST_ID_SKEL		0x80000000
  /* Not downloaded documents are numbered from this ID (flags are independent
   * of ETYPE_*, because they are applied to a different vertex) */

struct fastbuf *resolve_fastbuf(struct fastbuf *in, uns flags, uns add_size);
uns resolve_start(uns flags, uns add_size);
struct fastbuf *resolve_finish(struct fastbuf **in);

/* feedback-gath.c */

struct feedback_gatherer {
  byte footprint[16];
  uns cardid;
  byte flags;				/* the same as card_note.flags */
  byte weight;
};

#endif
