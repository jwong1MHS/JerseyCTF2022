/*
 *	Sherlock Search Engine
 *
 *	(c) 1997--2007 Martin Mares <mj@ucw.cz>
 */

#include "ucw/clists.h"
#include "ucw/slists.h"
#include "ucw/bitarray.h"
#include "sherlock/index.h"
#include "indexer/sites.h"
#include "search/images.h"

#define PROFILE_TOD
#include "ucw/profile.h"

/* compile-time parameters */

#define MAX_PHRASE_LEN 8	/* Must be a power of two */
#define HARD_MAX_WORDS 32	/* Limited to 32 by refs.c */
#define HARD_MAX_SYNONYMA 63	/* Limited to 63 by bits in syn_expand */
#define HARD_MAX_DATABASES 32	/* Limited to 32 by db_mask bits */
#define NUM_BESTS 8		/* How many best matches are kept for displaying of match context */
#define BESTS_PER_WORD 2	/* No more than this number of matches can be recorded for a single word */

/* config.c */

extern char *log_name, *status_name;
extern uns log_incoming, log_rejected, log_requests, log_replies, log_fetches;
extern uns port, listen_queue, connection_timeout, hydra_processes, slice_threads;
extern char *control_password;
extern clist databases;
extern clist spell_common_pairs;
extern clist spell_phrases;
extern clist spell_kb_trans;
extern uns num_matches, max_matches, cache_size, max_output_matches;
extern uns max_words, max_word_matches, max_phrases, max_nears, max_bools;
extern uns global_accent_mode, wildcard_asterisks, wildcard_qmarks, max_wildcard_zone, min_wildcard_prefix_len;
extern uns global_context_chars, global_intervals, highlight_substring, global_site_max;
extern uns global_meta_chars[16];
extern char *url_attributes;
extern uns global_url_max, global_max_redir_brack, global_max_cat_brack, global_morphing, global_spelling, global_synonyming;
extern uns doc_weight_scale, word_weight_scale, word_bonus;
extern uns global_allow_approx, prox_penalty;
extern int prox_limit;
extern uns global_partial_answers, default_word_types, global_debug, global_sorting, global_sort_reverse;
extern uns mem_map_zone_size, mem_map_elide_gaps, mem_map_prefetch;
extern uns fetch_threads;
extern uns query_watchdog, second_best_reduce;
extern uns magic_complexes, magic_merge_words, magic_merge_classes;
extern uns magic_keyphrases, magic_keyphrases_classes, magic_keyphrases_string_types, magic_keyphrases_bonus;
extern uns magic_near, magic_merge_bonus;
extern uns near_bonus_word, near_penalty_gap, near_bonus_connect, near_min_weight, near_max_weight;
extern uns blind_match_penalty, misaccent_penalty, stem_penalty, morph_penalty, synonymum_penalty;
extern uns spell_good_freq, spell_min_len, spell_margin, spell_dwarf, spell_dwarf_margin, global_syn_expand, spell_common_penalty;
extern uns spell_add_penalty, spell_del_penalty, spell_mod_penalty, spell_xpos_penalty, spell_accent_penalty;
extern uns filter_repeated_nonalpha, filter_repeated_alpha;
extern clist access_list;

struct database {
  cnode node;
  char *name;
  char *directory;
  enum {
    DB_PART_WORDS = 1,
    DB_PART_STRINGS = 2,
    DB_PART_PRINTS = 4,
    DB_PART_IMAGE_SIGNATURES = 8
  } parts;
  uns is_optional;
  char **blacklists;
  struct mempool *pool;			/* Contains all information local to this database */
  bitarray_t dup_flags;
  uns slice_start[HARD_MAX_SLICES + 1];

  /* Parameters */
  struct index_params *params;

  /* Cards */
  oid_t num_ids;
  int fd_cards, fd_refs;
  struct card_attr *card_attrs, *card_attrs_end;
  ucw_off_t card_file_size, ref_file_size;

  /* Words */
  uns word_weights[8];
  uns meta_weights[16][4];
  uns lexicon_words, lexicon_complexes, lexicon_file_size;
  struct lex_entry **lex_array;
  uns lex_by_len[MAX_WORD_CHARS+2];
  struct lex_entry ***cplx_array;
  uns stems_file_size;
  clist stem_block_list, syn_block_list;
  slist wexc_list;
  struct vocabolario *wexc_vocabolario;

  /* Strings */
  uns string_weights[8];
  u32 *string_hash;
  uns string_buckets, string_hash_order, string_count;
  int fd_string_map;
  uns string_hash_file_size, string_map_file_size;

  /* Fingerprints */
  struct fastbuf *fb_card_prints;

#ifdef CONFIG_SITES
  /* Sites */
  struct site_mapping_table sites;
#endif

  /* Similar images */
  int fd_image_signatures;
  uns image_clusters_depth;
  struct image_cluster *image_clusters;
};

struct spell_pair {
  cnode n;
  uns x, y;
};

struct spell_phrase {
  cnode n;
  char *src, *dest;
  uns penalty;
};

struct spell_kb_tran {
  cnode n;
  uns penalty;
  clist pairs;
  void *table;
};

/*** Reply buffers: we keep fragments of the reply in them (reply.c) ***/

struct reply {
  struct reply *next;
  uns len;
  byte text[1];
};

struct reply_buf {
  struct reply *first, **last;
  struct mempool *pool;
};

struct query;

void init_reply_buf(struct reply_buf *, struct mempool *);
void add_reply_to(struct reply_buf *, char *, ...) FORMAT_CHECK(printf,2,3);
void ship_reply_buf(struct query *, struct reply_buf *);
void flush_reply_buf(struct query *, struct reply_buf *);
struct reply *first_reply_last(struct reply_buf *);

void add_reply(char *fmt, ...) FORMAT_CHECK(printf,1,2);
void add_err(char *fmt, ...) FORMAT_CHECK(printf,1,2);
void add_footer(char *fmt, ...) FORMAT_CHECK(printf,1,2);

void send_reply(char *fmt, ...) FORMAT_CHECK(printf,1,2);
void send_reply_string(char *msg);
void send_reply_block(char *start, uns len);

/*** Syntax tree of the query ***/

enum expr_type {
  /*
   *  Many of the types have limited occurence:
   *	P = parsing and preprocessing
   *	I = input of analyse_query()
   *	O = output of analyse_query()
   *    A = internal to analyse_query()
   */
  EX_RESERVED,				/* ...  Undefined node type */
  EX_MATCH,				/* PI.  Match word or phrase */
  EX_OPTIONS,				/* P..  Set option defaults */
  EX_AND,				/* PIO  Boolean operators */
  EX_OR,
  EX_NOT,
  EX_ANY,				/* PIO  Logical 1 */
  EX_NONE,				/* PIO  Logical 0 */
  EX_IGNORE,				/* .A.  Non-indexed word */
  EX_WORD,				/* ..O  Match a word */
  EX_PHRASE,				/* ..O  Match a phrase */
  EX_IMAGE_SIM_MATCH,			/* PI.  Match a similar image */
  EX_IMAGE_SIM,				/* ..O  A bit larger structure used in refs.c */
};

struct options {
  int weight;
  s8 accent_mode;
  s8 morphing;
  s8 spelling;
  s8 synonyming;
  u64 syn_expand;
  uns default_word_types;		/* ~0U = unset */
};

#define OPT_DEFAULT -1
#define WEIGHT_DEFAULT 65535		/* unset */

struct expr {
  enum expr_type type;
  union {
    struct {
      struct expr *l, *r;
    } op;
    struct {
      struct options o;
      byte *word;
      uns type_mask;			/* Can be ~0U (unset) before propagation of options */
      byte is_string;			/* Set after option propagation */
      s8 sense;				/* Matching sense: 1=YES, -1=NOT, 0=MAYBE */
      struct expr *next_simple;		/* Next in simple search chain */
    } match;
    struct {
      struct expr *inside;
      struct options o;
    } options;
    struct {
      struct word *w;
    } word;
    struct {
      struct phrase *p;
    } phrase;
    struct image_sim_match image_sim_match;
    struct image_sim *image_sim;
  } u;
};

#define ACCENT_AUTO 0			/* magic auto mode */
#define ACCENT_STRIP 1			/* strip all accents before comparing */
#define ACCENT_STRICT 2			/* compare with accents */
#define ACCENT_AUTO_LOCAL 3		/* magic auto mode done per word */

/* lex.c */

void lex_init(byte *);
int yylex(void);
int lookup_custom_attr(byte *);

/* parse.y */

void err(char *, ...) NONRET FORMAT_CHECK(printf,1,2);
byte *parse_query(byte *);
struct expr *new_node(enum expr_type t);
struct expr *new_op(enum expr_type t, struct expr *l, struct expr *r);
void merge_options(struct options *dest, struct options *old, struct options *new);

struct set_node {			/* Syntactic representation of integer or string sets */
  snode n;
  uns min, max;
  byte *text;
};

struct db_node {			/* Syntactic representation of database lists */
  snode n;
  struct database *db;
  int id;
};

struct dump_node {			/* ... and of sets of cards to dump */
  snode n;
  oid_t id;
  int db_id;
  struct database *db;
};

enum out_mode {				/* Output modes */
  OUT_STATS,
  OUT_LIST,
  OUT_SHOW,
  OUT_DUMP
};

/* sherlockd.c */

void write_reply(struct query *, byte *, uns);

/*** Internal representation of queries ***/

struct stats {				/* Statistics */
  uns matching_docs;			/* Matching documents for this dbase */
  EXTENDED_STAT_VARS			/* Custom statistics */
};

struct query {				/* Keep the most frequently used parts near the top */
  cnode n;
  struct mempool *pool;			/* Memory pool for _query_ data (use results->pool for results to be cached) */

  /* Query parameters */
  u32 db_mask;				/* Database mask */
  slist *db_list;			/* A list of databases requested */
  uns debug;				/* Debug flags */
  struct options default_options;	/* Default word options */
  u64 site_hash;			/* Only this site */
  uns site_max;				/* Number of documents per site */
  uns allow_approx;			/* Allow approximation */
  uns partial_answers;			/* Allow partial answers */
  struct expr *expr;			/* Query expression (NULL if it's a command) */
  byte *cmd;				/* Command */

#define INT_ATTR(id,keywd,gf,pf) u32 id##_min, id##_max;
#define SMALL_SET_ATTR(id,keywd,gf,pf) u32 id##_set;
#define LATE_INT_ATTR INT_ATTR
#define LATE_SMALL_SET_ATTR SMALL_SET_ATTR
  EXTENDED_ATTRS			/* Extended attributes */
#undef INT_ATTR
#undef SMALL_SET_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR

  CUSTOM_MATCH_VARS			/* Include what custom matchers need */
#define CUSTOM_MATCH_KWD(id,keywd,parse) byte id##_placeholder;
  CUSTOM_MATCH_PARSE
#undef CUSTOM_MATCH_KWD

  int custom_sorting;			/* Sort on custom attribute */
  u32 custom_sort_reverse;		/* ~0 if sorting reverse, else 0 */
  int custom_sort_only;			/* should Q be ignored? */
  uns age_raw_min, age_raw_max;		/* Limits on document age */
  u32 explain_id;			/* Which object we'd like to explain for [CONFIG_EXPLAIN] */

  /* Query processing status */
  struct results *results;		/* Result structure for this query */
  int cache_age;			/* Age of cached reply, -1 if not cached */
  uns contains_accents;			/* There is an accent anywhere in the query */
  uns needed_results;			/* How many results do we have to calculate */
  uns results_to_show;			/* How many results are requested to be shown */
  struct global_match_heap *match_heap;	/* refs.c: Temporary heap used for keeping the best matches */
  slist ft_contexts;			/* fulltext.c: Fulltext contexts for all databases */

  /* Local processing status for the current database */
  struct database *dbase;		/* Database we're currently examining */
  struct expr *prep_expr;		/* Preprocessed expression */
  struct word **words;			/* Words found in the query */
  uns nwords;
  struct phrase **phrases, **nears;	/* Phrase matchers and near matchers */
  uns nphrases, nnears;
  u32 *bool_map;			/* Truth tables of boolean expressions */
  u32 *optimistic_bool_map;
  uns n_bool_ids;			/* Number of words/phrases involved in boolean expression */
  int age_min, age_max;			/* Document age relative to ref_time of current database */
  struct stats stats;			/* Statistics */
  uns stat_num_chains;			/* Statistics: total number of reference chains seen */
  uns stat_len_chains;			/* Statistics: total length of reference chains seen */
  uns query_word_count;			/* The number of query words */
  struct image_sim **image_sims;	/* Similar images matcher */
  uns nimage_sims;

  /* Display parameters */
  enum out_mode out_mode;		/* Type of output requested */
  slist *out_range;			/* List of results to display */
  uns context_chars;			/* Number of context chars to print */
  uns meta_chars[16];			/* Number of meta chars to print for each meta type */
  uns intervals;			/* Number of intervals to print */
  uns url_max;				/* Maximum # of URL's to print */

  /* Reply */
  struct reply_buf reply_header;	/* Reply header */
  struct reply_buf reply_footer;	/* Reply footer */
  struct reply_buf *current_reply_buf;	/* Either query header or cached results header */

  /* Timing statistics */
  uns time_total;
  char *profile_stats;

  /* Memory mappings */
  uintptr_t last_mapping;

  /* Card prefetching */
  struct card_fetch *card_fetches;	/* Running asynchronous card fetches */
  uns ncard_fetches;

  /* Connection */
  ucw_time_t established;		/* Time this connection was established */
  int fd;				/* Socket file descriptor */
  int fd_err;				/* Set in case an error has occured */
  int q_status;				/* Error code returned */
  byte ipaddr[16];			/* IP address of the other end */
  byte *iobuf;				/* I/O buffering */
  byte *ibptr, *ibend;
  byte *obptr, *obend;
};

#define CONTEXT_FULL 1000000000		/* context_chars when CONTEXT FULL is asked for */

/* Internal parameters masking themselves as extended attributes */
/* Beware, they should not collide with extended attr offsets in struct query */
#define PARAM_SITE		0
#define PARAM_AGE		1
#define PARAM_CARDID		2

/* Debug flags (global_debug, query->debug, DEBUG) */

#define DEBUG_NOCACHE		1	/* Disable reply caching */
#define DEBUG_ANALYSE		2	/* Debug query analyser */
#define DEBUG_DUMPING		4	/* Dumping the context */
#define DEBUG_WORDS		8	/* Debug processing of words */
#define DEBUG_CARD_INFO		16	/* Show result notes and card attributes (used by mux, too) */
#define DEBUG_FETCH		32	/* Show which parts of files are fetched */
#define DEBUG_IMAGES		64	/* Debug processing of images */

/*** Words and phrases ***/

/* When processing a simple search query, we represent it by a list of these structures */

struct simple {
  cnode n;
  struct expr *raw;			/* "raw" version, that is an EX_MATCH node */
  struct expr *cooked;			/* "cooked" version */
  clist phrase;				/* words.c: phrase expansion */
};

/*
 *   For each unique entity we match (words, word complexes, strings), we create
 *   a single struct word holding all the relevant information and pointing
 *   to reference chains corresponding to the expansion of this word. Multiple
 *   occurences of the same entity (that is, if all the key attributes marked
 *   with [K] are equal) get mapped to the same struct word.
 */

struct word {
  /* stuff used by refs.c goes first to improve caching */
  int boolean_id;			/* ID used in boolean expression, <0 if known to be unmatchable */
  uns type_mask;			/* [K] Bitmap of allowed word/string types, upper 16 bits are meta types */
  uns doc_count;			/* Number of documents matched */
  int weight;				/* Word weight */
  uns is_string;			/* [K] Match strings, not words */
  uns is_outer;				/* At least once outside a phrase */
  struct options options;		/* [K] Local word matching options; translate_accent_mode()'d */
  uns status;				/* 0 for OK or error code */
  byte *word;				/* [K] The word itself */
  uns expanded;				/* Already expanded (0=not, 1=partially, 2=with refs) */
  uns word_class;			/* words.c: Word class */
  slist variants;			/* The list of word variants (struct variant) */
  uns var_count;			/* The number of variants */
  uns ref_total_len;			/* refs.c: Total length of all reference chains read */
  uns cover_count;			/* words.c: Number of occurences inside a complex */
  uns use_count;			/* Number of times this word was looked up */
  uns hide_count;			/* If hide_count == use_count, don't report the word unless it was matched */
  uns is_wild;				/* Contains wildcards */
  struct word *root;			/* If it's a complex, then its root word, else NULL */
  uns index;				/* Index of this word in word array */
};

struct variant {
  snode n;
  uns lex_id;				/* words.c: Word ID in the lexicon */
  u32 lang_mask;			/* Language mask */
  byte noaccent_only;			/* Use in accentless documents only */
  byte penalty;				/* Extra negative weight */
  byte flags;				/* VF_xxx */
  byte rfu;
  ucw_off_t refchain_start;		/* Refchain corresponding to this variant */
  uns refchain_len;			/* (can be empty) */
};

enum var_flags {
  VF_SOURCE_MASK = 0x1f,		/* These bits indicate source of the variant */
  VF_QUERY = 1,				/* Query word and its accent/wildcard expansions */
  VF_SYNTHETIC = 2,			/* Query word which isn't present in the lexicon, so we've synthesized a lex_entry for it */
  VF_MORPH = 4,				/* Generated by morphological expansion */
  VF_ACCENTS = 8,			/* Generated by accent expansion */
  VF_SYNONYMUM = 16,			/* Generated by synonymic expansion */
  VF_LEMMA = 32,			/* This variant is a lemma */
  VF_ACCENTIFIED = 64,			/* Searching for accent variants already applied to this word */
  VF_MORPHED = 128,			/* Morphological expansion already applied */
};

struct phrase {				/* Used for both phrases and near-matchers */
  uns word[MAX_PHRASE_LEN];		/* Words the phrase consists of */
  uns relpos[MAX_PHRASE_LEN];		/* Relative positions of the words */
  byte word_to_idx[HARD_MAX_WORDS];	/* Maps word id to position in phrase, offset by 1 */
  byte next_same_word[MAX_PHRASE_LEN];	/* Next occurence of the same word, offset by 1 */
  u32 prox_map;				/* Where do we allow proximity */
  uns length;				/* Number of words */
  int weight;
  uns matches;
  int boolean_id;
  u32 word_mask;
  uns index;				/* Index of this phrase in the phrase array */
};

/*** Cached query results ***/

struct result_note {
  struct card_attr *attr;		/* Attributes of this card */
  int q;
  u32 sec_sort_key;
#ifdef CONFIG_SITES
  int site_compressed;			/* Number of documents ignored from this site */
#endif
};

struct results {			/* Query results we cache */
  /* Cache control */
  cnode h;				/* In hash queue (_must_ be first!) */
  cnode n;				/* In LRU queue */
  int status;				/* -1=uninitialized, 0=OK, else error code */
  byte *request;			/* Normalized request */
  u32 db_mask;				/* A set of databases */
  uns max_matches;			/* Result limit */
  ucw_time_t create_time, access_time;	/* Time created/last used */

  /* The results */
  struct mempool *pool;			/* Pool holding this structure and everything referenced by it */
  struct reply_buf reply_header;	/* Reply header */
  struct result_note *matches;		/* Matching documents */
  uns num_matches;
  clist interims;			/* List of cached_interims for all databases involved in the query */
};

struct cached_interims {		/* Cached preprocessed parts of query used when dumping results (per database) */
  cnode n;
  struct database *db;			/* Which database is this related to */
  struct expr *expr;			/* Preprocessed expression */
  struct word **words;			/* Words, phrases, nears and images */
  struct phrase **phrases, **nears;
  struct image_sim **image_sims;
  uns nwords, nphrases, nnears, nimage_sims;
};

/* query.c */

extern struct query *current_query;
extern struct database *current_dbase;

void cache_init(void);
void query_init(void);
void process_query(struct query *q);
struct word *lookup_word(struct query *q, struct expr *e, byte *w);
void eval_err(int code) NONRET;

void init_stats(struct query *q, struct stats *st);
void merge_stats(struct query *q, struct stats *f, struct stats *t);
int simplified_eval_expr(struct expr *x, uns bool_mask);

int restore_interims(struct query *q, struct results *r, struct database *db);

/* Profiling counters */
#define PROFILERS(sep) P(analyse) sep P(reff) sep P(refs) sep P(resf) sep P(results)
#define COMMA ,
#define P(x) prof_##x
extern prof_t PROFILERS(COMMA);
extern prof_t prof_send;
#undef P
prof_t *profiler_switch(prof_t *p);

/* dbase.c */

void db_init(int merge_only);
void db_switch_config(struct database *db);
byte *db_file_name(struct database *db, byte *fn);
struct database *attr_to_db(struct card_attr *attr, oid_t *ooid);

/* cards.c */

void cards_init(void);
void cards_init_process(void);
int check_result_set(struct query *q);
void show_results(struct query *q);
void prefetch_results(struct query *q);
void prefetch_results_cleanup(struct query *q);

extern const char * const wt_names[];
extern const char * const mt_names[];
extern const char * const st_names[];

/* words.c */

void words_init(struct database *db);
void word_analyse_simple(struct query *q, clist *l);
int word_contains_accents(byte *s);
void word_dump_variants(struct word *w);
void word_ensure_variants(struct word *w);
void word_extract_variant(struct word *w, struct variant *v, byte *buf);

/* strings.c */

void strings_init(struct database *db);
struct expr *string_analyse(struct query *q, struct expr *e);
void string_analyse_simple(struct query *q, clist *l);
int string_db_find_refchain(struct query *q, struct database *db, byte *s, ucw_off_t *refchain_start, uns *refchain_len);

/* spell.c */

void spell_init(void);
void spell_check(struct query *q);

/* memory.c */

struct mmap_request {
  union {
    struct {
      int fd;
      ucw_off_t start, end;
    } req;
    struct {
      void *start, *end;
    } map;
  } u;
  uintptr_t userdata;
};

void memory_init(void);
void memory_setup(struct query *q);
void memory_flush(struct query *q);
void *mmap_region(struct query *q, int fd, ucw_off_t start, ucw_off_t end);
int mmap_regions(struct query *q, struct mmap_request *reqs, int count);
void log_fetch(struct query *q, int fd, ucw_off_t start, ucw_off_t size);

extern struct lizard_buffer *liz_buf;

/* refs.c */

void refs_init(void);
void process_refs(struct query *q);
void query_init_refs(struct query *q);
void query_finish_refs(struct query *q);
uns refs_chain_find_type_mask(byte **refchain, uns want_mask, uns want_all, uns *found_mask, uns have_slices);

/* cmds.c */

void do_command(struct query *q);
