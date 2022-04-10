/*
 *	Sherlock Shepherd -- Interface for Manual Control
 *
 *	(c) 2004--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006--2007 Pavel Charvat <pchar@ucw.cz>
 */

#ifndef _SHERLOCK_GATHER_SHEPHERD_MAN_H
#define _SHERLOCK_GATHER_SHEPHERD_MAN_H

#include "ucw/fastbuf.h"
#include "sherlock/object.h"
#include "gather/shepherd/shepherd.h"

#include <time.h>

/*** man-main.c ***/

extern struct mempool *man_pool;
extern time_t man_ref_time;

extern void (*man_usage_callback)(byte *msg);

extern struct man_options {
  byte *state;
  byte *server;
  int verbose;

  uns hist_num_boxes;
  uns hist_box_width;
  uns age_display_unit;

  uns show_lastmod;
  uns multi_age;
  uns bare;
  uns show_idx;
  uns show_urls;
  uns show_sites;
  uns show_plan;

  int set_weight;
  int set_freq;
  int set_section;
  uns set_flag_mask;
  uns set_flag_val;
  area_t set_area;
#ifdef CONFIG_AREAS
  int set_area_soft;
  int set_area_hard;
  int set_area_plan;
#endif
} man_opt, man_opt_defaults;

void man_init(void);
void man_reset(void);
uns man_raw_url_age(struct url_state *s);
uns man_url_age(struct url_state *s);

#define MAN_VERBOSE(msg...) do { if (man_opt.verbose) log(L_INFO, msg); } while (0)

void man_usage(byte *cmd, ...) NONRET;

/*** man-resolve.c ***/

extern struct odes *resolve_object;

struct fastbuf *resolve_add(struct fastbuf *f, struct url_state *s);
struct fastbuf *resolve_go(struct fastbuf *f, int want_body, int order);
int resolve_read(struct fastbuf *f, struct url_state *s, byte *url);
void resolve_close(struct fastbuf *f);
uns resolve_non_url(byte *url, struct url_state *s);

/*** man-sel.c ***/

/* Selector output formats */
enum sel_type {
  ST_UNDEF,
  ST_CONTRIB,
  ST_INDEX_ENTRIES,
  ST_SITES,
  ST_FILTERING,
  ST_AREAS,
  ST_URL_HASH,
  ST_MIXED
};

/* Selector option types */
enum sel_opt {
  SEL_UNDEF,
  SEL_FIRST = 0x200,
  SEL_ALL,
  SEL_URL,
  SEL_URLS,
  SEL_QKEY,
  SEL_FP,
  SEL_NORM_FP,
  SEL_FPS,
  SEL_ONLY_TYPES,
  SEL_ONLY_FLAGS,
  SEL_ONLY_NEWER,
  SEL_ONLY_OLDER,
  SEL_ONLY_AREA,
  SEL_AREA
};

struct selector {
  enum sel_type result_type;		/* ST_x */
  struct mempool *pool;
  uns need_all;				/* We need to iterate over all records including the unmatched ones */
  uns areas_rw;				/* Open areas for R/W (ST_AREAS only) */

  /* Only for ST_MIXED selectors */
  clist lsrc;				/* List of sources (sel_src) */
  struct sel_src *idx_src;		/* Source of struct url_state (or NULL) */
  uns bool_mask;			/* Sum of src->bool_mask over all sources that contain the current record */
  byte *url;				/* Current URL (NULL if unknown) */
  struct footprint fp;			/* Current footprint */
};

struct selector *selector_init(enum sel_type result_type);
void selector_cleanup(struct selector *selector);
byte *selector_opt(struct selector *selector, enum sel_opt opt_type, byte *value);
byte *selector_check(struct selector *selector);

int sel_index(struct selector *selector, void (*f)(struct url_state *s, byte *url, int matching), int resolve);

struct site *sel_site_next(struct selector *selector, struct site *s);

area_t area_next_id(struct selector *selector);

struct sel_hash_entry {
  struct footprint fp;
  byte *url;
  int seen;
};

uns sel_hash_count(struct selector *selector);
struct sel_hash_entry *sel_hash_find(struct selector *selector, struct footprint fp);
void sel_hash(struct selector *selector, void (*f)(struct sel_hash_entry *e));

struct sel_src;
void sel_mixed_add_source(struct selector *selector, struct sel_src *src, uns flags);
int sel_mixed(struct selector *selector, void (*f)(struct selector *selector));

/*** man-cmd.c ***/

void cmd_list(struct selector *selector);
void cmd_buckets(struct selector *selector);
void cmd_sites(struct selector *selector);
void cmd_sites_filter(struct selector *selector);

void cmd_set(struct selector *selector);
void cmd_delete(struct selector *selector);
void cmd_turn_to_zombie(struct selector *selector, uns error_code);

void cmd_insert(struct selector *selector);
void cmd_insert_refs(struct selector *selector);

void cmd_histogram(struct selector *selector);

void cmd_qkey_stats(struct selector *selector, uns refresh);

void cmd_remote_set(uns command, uns arg);
void cmd_borrow(uns command, byte *phase);
void cmd_remote_unlock(void);

#ifdef CONFIG_AREAS
void cmd_areas(struct selector *selector);
void cmd_set_area_params(struct selector *selector);
#endif

void cmd_replace_init(struct selector *selector);

void cmd_dump(struct selector *selector);

void list_fp_header(struct fastbuf *output);
void list_plan_header(struct fastbuf *output);
void list_plan_entry(struct fastbuf *output, struct url_state *s);
void list_index_header(struct fastbuf *output);
void list_index_entry(struct fastbuf *output, struct url_state *s);
void list_site_entry(struct fastbuf *output, struct url_state *s);

/*** man-src.c ***/

struct sel_src {
  cnode n;				/* Node in selector->lsrc */
  cnode resolve_node;
  uns flags;
  uns bool_id;
  uns bool_mask;
  void *record;
  void *user;

  int (*cmp)(void *record, void *key);

  void (*close)(struct sel_src *src);

  /* Return the first record or NULL if the source is empty. */
  void *(*find_first)(struct sel_src *src);

  /* Return the next record or NULL if the source is empty or if we have already read the last record. */
  void *(*find_next)(struct sel_src *src);

  /* Find the first record not lower than key or NULL if there is no one. */
  void *(*find)(struct sel_src *src, void *key, uns *gt);

  /* Similar to find() but the search starts from the last read record.
   * Note than repeated calls to find_forward() with the same key may lead to an infinite loop,
   * so don't forget to call find_next(). */
  void *(*find_forward)(struct sel_src *src, void *key, uns *gt);

  /* Resolve current record's URL */
  byte *(*resolve)(struct sel_src *src);

  void (*dump)(struct sel_src *src, struct fastbuf *out);
};

void sel_src_close(struct sel_src *src);
void sel_src_rewind(struct sel_src *src);

enum sel_mixed_flags {
  SEL_MIXED_NEEDED = 0x1,
  SEL_MIXED_MAYBE = 0x2,
  SEL_MIXED_AUTO_CLOSE = 0x4,
  SEL_MIXED_INDEX = 0x8,
};

#define SEL_MIXED_MATCHED 0x80000000

/*** man-binary.c ***/

extern uns man_binary_forward_limit;
extern uns man_binary_first_skip;
extern uns man_binary_split_limit;

struct sel_binary_src;

struct sel_binary_src *sel_binary_open_file(byte *file_name, uns record_size, uns header_size);
struct sel_binary_src *sel_binary_try_open_file(byte *file_name, uns record_size, uns header_size);
void *sel_binary_find_first(struct sel_binary_src *s);
void *sel_binary_find_next(struct sel_binary_src *s);
void *sel_binary_find(struct sel_binary_src *s, void *key, uns *gt);
void *sel_binary_find_forward(struct sel_binary_src *s, void *key, uns *gt);
void sel_binary_close(struct sel_binary_src *s);
uns sel_binary_count(struct sel_binary_src *s);

struct sel_binary_src *sel_binary_open_index(byte *file_name);

/*** man-text.c ***/

extern uns man_text_block_limit;

struct sel_text_src;

struct sel_text_record {
  struct footprint fp;
  struct odes *o;
};

struct sel_text_src *sel_text_open_file(byte *file_name);
struct sel_text_src *sel_text_try_open_file(byte *file_name);
struct sel_text_record *sel_text_find_first(struct sel_text_src *s);
struct sel_text_record *sel_text_find_next(struct sel_text_src *s);
struct sel_text_record *sel_text_find(struct sel_text_src *s, struct footprint *key, uns *gt);
struct sel_text_record *sel_text_find_forward(struct sel_text_src *s, struct footprint *key, uns *gt);
void sel_text_close(struct sel_text_src *s);

struct sel_text_src *sel_text_open_urls(byte *file_name);

struct sel_text_writer;

struct sel_text_writer *sel_text_create(void);
void sel_text_write(struct sel_text_writer *s, struct footprint *fp, struct odes *o);
struct fastbuf *sel_text_finish(struct sel_text_writer *s);
void sel_text_merge_dups(struct sel_text_writer *s);

#endif
