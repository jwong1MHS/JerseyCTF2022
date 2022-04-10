/*
 *	Sherlock Shepherd Daemon -- Global Declarations
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#ifndef _SHERLOCK_GATHER_SHEPHERD_SHEPHERD_H
#define _SHERLOCK_GATHER_SHEPHERD_SHEPHERD_H

#include "ucw/url.h"
#include "ucw/clists.h"
#include "sherlock/bucket.h"

/* config.c */

struct unsrange;

extern char *db_dir, *state_dir, *shepherd_filter_name;
extern uns std_server_delay, min_server_delay, autoreply_delay, server_overtake;
extern uns req_err_retry, site_err_retry, site_err_expire, conn_err_delay;
extern uns trace_refs, ignore_refs, auto_go_root, max_resolvers, max_flushers, doc_change_mix;
extern uns contrib_cache_size, refresh_cycle, default_soft_limit, dead_soft_limit, default_fresh_limit;
extern uns avg_bucket_size, avg_url_size;
extern uns reap_cycle, null_cycle, contrib_gap, select_hysteresis, stable_time_unit;
extern uns min_robots_frequency, min_eq_frequency, max_err_frequency;
extern uns default_insert_weight, safety_brake_limit;
extern uns anticipated_refresh_age;
extern uns planner_stats, select_stats;
extern double hard_limit_factor, duty_factor;
extern double reap_optimism_factor, reap_slowdown_factor, estimated_raw_performance;
extern double global_frequent_factor;
extern double stability_factor;
extern u64 expected_bucket_file_size;
extern struct unsrange *zombie_gerr_ranges;
extern uns zombie_expire, redirect_to_zombie_timeout;
extern struct unsrange *prune_site_gerr_ranges;
extern char *url_database_file, *url_sorted_file;
extern uns auto_sort_index;

struct section_config {
  cnode n;
  uns section;
  double limit;
  uns select_bonus;
};
extern clist section_configs;

extern struct refresh_schema {
  uns num;
  uns frequencies[SHERLOCK_NUM_FREQS+1];
  double allocations[SHERLOCK_NUM_FREQS+1];
  double frequent_factor;
} *refresh_schemas[];

static inline uns
double_to_uns(double x)
{
  if (x > ~0U - 1)
    return ~0U;
  else if (x < 0)
    return 0;
  else
    return (uns) x;
}

/* footprint.c */

struct site_fp {
  u32 x[2];
};

struct rest_fp {
  u32 x[2];
};

struct footprint {
  struct site_fp site;
  struct rest_fp rest;
};

struct fastbuf;

int urlrec_site_fp(struct site_fp *fp, struct url *ur);
int urlrec_rest_fp(struct rest_fp *fp, struct url *ur);
int urlrec_footprint(struct footprint *fp, struct url *ur);
int url_footprint(struct footprint *fp, byte *url);
void random_footprint(struct footprint *fp);
void footprint_array_sort(uns n, struct footprint *fps);
struct fastbuf *footprint_sort(struct fastbuf *src, struct fastbuf *dest);
struct fastbuf *url_state_by_fp_sort(struct fastbuf *src, struct fastbuf *dest);

#define FP_PAIR(f) (f).x[0], (f).x[1]
#define FP_QUAD(f) FP_PAIR((f).site), FP_PAIR((f).rest)

#define ROBOTS_TXT_FOOTPRINT ((struct rest_fp) {{ 0xc3ced96e, 0x5d3e1f56 }})
#define ROOT_FOOTPRINT ((struct rest_fp) {{ 0x76cd6666, 0x465669f9 }})
#define SKEY_FOOTPRINT ((struct rest_fp) {{ 0, 0 }})
#define MAX_FOOTPRINT ((struct footprint) {{{ ~0U, ~0U }}, {{ ~0U, ~0U }}})

static inline int
site_fp_cmp(struct site_fp *x, struct site_fp *y)
{
  COMPARE(x->x[0], y->x[0]);
  COMPARE(x->x[1], y->x[1]);
  return 0;
}

static inline int
rest_fp_cmp(struct rest_fp *x, struct rest_fp *y)
{
  COMPARE(x->x[0], y->x[0]);
  COMPARE(x->x[1], y->x[1]);
  return 0;
}

static inline int
fp_cmp(struct footprint *x, struct footprint *y)
{
  int r = site_fp_cmp(&x->site, &y->site);
  if (!r)
    r = rest_fp_cmp(&x->rest, &y->rest);
  return r;
}

/* site-hash.c */

enum site_flags {
  SITE_REJECTED = 0x1,		/* Site is rejected by filters */
  SITE_WRITTEN = 0x2,		/* Temporary flag in site_hash_save() */
  SITE_TEMP = 0x4,		/* The site should be deleted by site_hash_reset() */
};

struct site {
  struct site_fp fp;
  struct site *hash_next;
  /* Temporary variables of different modules */
  union {
    struct {
      oid_t robot_oid;
      uns cnt;
      struct qkey_hash *qkey;
    } plan;
    struct {
      uns last_written;
      uns last_index;
    } merge;
    struct {
      struct select_qkey *qkey;
      uns num_useful;
      uns skey_found;
    } select;
    struct {
      u32 total_download_time;
      u32 entries_seen;
      u32 update_mask;
    } cork;
    struct {
      uns flags;
      struct site_matcher *first_matcher;
      struct qkey_stats *qkey_stats;
    } ctrl;
    struct site *delete_next;
  } u;
  /* Variables set by the filters */
  uns soft_limit;			/* How many URL's we are willing to gather */
  uns hard_limit;			/* How many we are willing to remember */
  uns fresh_limit;			/* How many URL's with only approximate weight can be active */
  uns min_delay;			/* Local value of MinServerDelay */
  uns queue_bonus;			/* Bonus points in gatherer planning */
  byte monitor;				/* Monitor activity on this site to state log */
  byte select_bonus;			/* Prefer this site when selecting (currently 0/1) */
  byte max_conn;			/* Maximum number of simultaneous downloads */
  byte flags;				/* See SITE_x above */
  byte refresh_schema;			/* ID of the selected refresh schema */
  byte refresh_boost;			/* A divisor to automatically detected stable time */
  byte unused[2];
  /* These are stored in the site list [and which module computes them] */
  u32 num_active;			/* Number of active URL's in this site [select] */
  u32 num_inactive;			/* ... and of inactive ones [select] */
  u32 num_fresh;			/* ... and of URL's with only approximate weight [select] */
  u32 num_gathered;			/* How many from the active URL's are really gathered [select] */
  u32 num_oscillations;			/* Number of pages "ungathered" [select] */
  u32 skey;				/* Cached server key [plan, cork] */
  struct site_fp norm_fp;		/* Normalized FP [equiv] */
  u16 port;
  byte proto;
  byte avg_download_time;		/* Average of download_time's at this site */
  u32 error_cycles;			/* The number of consecutive failed reap cycles */
  byte hostname[1];
};

/* Parameters to site_hash_load() */
enum site_hash_flags {
  SITE_HASH_NO_URLS = 0x1,	/* Do not load host names (set them to "") */
  SITE_HASH_FILTER = 0x2,	/* Apply filters */
};

void site_hash_init(void (*constructor)(struct site *site));
void site_hash_load(byte *state, uns flags);
void site_hash_save(byte *state); /* org_state is needed only with SITE_HASH_NO_URLS */
struct site *site_lookup(struct site_fp *fp);
struct site *site_create(struct site_fp *fp, uns proto, byte *host, uns port);
struct site *site_next(struct site *site);
void site_delete(struct site *site); /* safe to call on the current entry when iterating site_next */
void site_hash_cleanup(void);
void site_hash_reset(void);
void maybe_load_sites(byte *state, int filter);
int num_sites(void); /* return the number of sites or -1 if sites has not been loaded yet */

struct site_list_entry {		/* Site list file: persistent parts of struct site */
  struct site_fp fp;
  u32 num_active;
  u32 num_inactive;
  u32 num_gathered;
  u32 skey;
  struct site_fp norm_fp;
  u16 port;
  byte proto;
  byte avg_download_time;
  u32 num_oscillations;
  u32 num_fresh;
  u32 error_cycles;
  u32 rfu;
  byte hostname[0];
} PACKED;

#define SITE_LIST_MAGIC 0xb4b6b293

/* site-filter.c */

uns site_filter(struct site *site, byte *hostname);
void site_hash_filter(void);

/*
 * Queue keys are 64-bit and they are composed from a 32-bit server key,
 * a 16-bit port number and in some cases also a 8-bit channel number.
 * Some server keys are reserved for special purposes, see reap-queue.c
 * and gather_create_key() for more comments.
 */

#define SKEY_UNRESOLVED		0x00000000	/* So far, no skey is known */
#define SKEY_NONIP		0x7f010000	/* Non-IP sites and sites with strange addresses */
#define SKEY_NONEXISTENT	0x7f020000	/* Unresolvable (typically nonexistent) hosts */
#define SKEY_TYPE_MASK		0xffff0000

static inline u64
make_qkey(u32 skey, uns port)
{
  return ((u64)port << 32) | skey;
}

static inline u64
qkey_set_channel(u64 qkey, uns chan)
{
  return (qkey & 0xff00ffffffffffff) | ((u64)chan << 48);
}

static inline u32
qkey_to_skey(u64 qkey)
{
  return qkey & 0xffffffff;
}

static inline uns
qkey_to_port(u64 qkey)
{
  return (qkey >> 32) & 0xffff;
}

static inline uns
qkey_to_channel(u64 qkey)
{
  return (qkey >> 48) & 0xff;
}

static inline u64
site_qkey(struct site *site)
{
  u32 skey = site->skey;
  u32 top = skey & SKEY_TYPE_MASK;
  if (top == SKEY_UNRESOLVED)
    return make_qkey(SKEY_UNRESOLVED | (site->fp.x[0] % max_resolvers), 0);
  else if (top == SKEY_NONEXISTENT)
    return make_qkey(SKEY_NONEXISTENT | (site->fp.x[0] % max_flushers), 0);
  else if (top == SKEY_NONIP)
    return make_qkey(skey, 0);
  else
    return make_qkey(skey, site->port);
}

#define QK_PAIR(q) qkey_to_port(q), qkey_to_skey(q)
#define QK_TRIPLE(q) QK_PAIR(q), qkey_to_channel(q)

/* State records */

struct url_state {
  struct footprint fp;			/* Footprint of this URL */
  oid_t oid;				/* Bucket where it is stored */
  ucw_time_t last_seen;			/* Timestamp of last check */
  byte retry_count;			/* Number of consecutive soft errors encountered since last download */
  byte weight;
  byte flags;				/* USF_... */
  byte type;				/* UTYPE_... */
  byte stable_time;			/* Time between last significant change and last check (in stable_time_units) */
  byte refresh_freq;			/* How many times per refresh_cycle should it be refreshed */
  byte download_time;			/* How long did last download take: In deciseconds, 0 if unknown, >= 0xf0 are temp. errors */
  byte section;				/* Section ID */
  area_t area;				/* Area ID */
};

#define OID_UNDEFINED OBUCK_OID_DELETED
#define OID_ERROR OBUCK_OID_FIRST_SPECIAL

enum url_state_flags {
  USF_INIT = 1,				/* Belongs to the initial set */
  USF_ROBOTS = 2,			/* robots.txt file or a skey */
  USF_UNREF = 4,			/* Known to be unreferenced */
  USF_NEEDED_BY_EQ = 8,			/* Needed by the equivalence finder */
  USF_REGATHER = 16,			/* Priority regathering requested */
  USF_CONTRIB = 32,			/* This is a recent contribution, oid is not a real oid, but position in contrib file or
					 * OID_UNDEFINED if it's a auto_go_root contribution.
					 */
  USF_TRUE_WEIGHT = 64,			/* Weight has been assigned by the indexer, not faked */
  USF_SELECT_PRIORITY = 128		/* This entry belongs to a site with non-zero select_bonus */
};

enum url_state_type_flags {
  UST_MASK = 15,			/* Mask for UTYPE_... enum */
  UST_NO_TARGET = 16			/* A redirect with unknown or zombie target (set by shep-feedback) */
};

#define USF_IS_SACRED(x) ((x) & (USF_INIT | USF_ROBOTS | USF_NEEDED_BY_EQ))	/* Must not be dropped nor merged */
#define USF_IS_SACRISIMMUS(x) ((x) & (USF_INIT | USF_ROBOTS))			/* ... even if proven to not exist */

enum url_state_types {
  UTYPE_SLEEPING,			/* Just remember the URL, don't gather */
  UTYPE_NEW,				/* Not gathered yet, but willing to */
  UTYPE_OK,				/* Gathered */
  UTYPE_ERROR,				/* Unable to gather -- hard error */
  UTYPE_SKEY,				/* Queue key placeholder (state->bucket is the skey value) */
  UTYPE_TEMP_ERROR,			/* Temporary error marker (occurs only in journal file; section contains error type) */
  UTYPE_ZOMBIE,				/* URL's which we have decided to remember, but never refresh.
					   Used for some kinds of errors (state->bucket is the error code). */
  UTYPE_MAX
};

#define URL_STATE_TYPE_NAMES { "SLP", "NEW", "OK", "ERR", "QKY", "TER", "ZOM" }

static inline uns
ustate_type(struct url_state *s)
{
  return s->type & UST_MASK;
}

static inline void
ustate_set_type(struct url_state *s, uns type)
{
  s->type = (s->type & ~UST_MASK) | type;
}

static inline uns
ustate_all_flags(struct url_state *s)
{
  return s->flags | ((s->type >> 4) << 8);
}
#define URL_STATE_ALL_FLAG_NAMES "IRUEFCWPT---"

enum error_type {			/* Used for UTYPE_TEMP_ERROR and also inside shep-reap */
  ERRT_NONE,
  ERRT_TEMP_REQUEST,
  ERRT_TEMP_CONNECTION,
  ERRT_TEMP_SITE,
  ERRT_TEMP_PROXY,
  ERRT_PERM
};

/* Contribution file */

#define CONTRIB_SHIFT 4U
#define CONTRIB_ALIGN (1U << CONTRIB_SHIFT)

struct contrib {
  struct footprint fp;
  area_t area;
  u16 url_len;
  byte weight;				/* Initial weight of the contribution */
  byte section;				/* Initial section */
  byte flags;				/* USF_... */
  byte url[0];				/* [url_len] */
					/* and padding to a multiple of CONTRIB_ALIGN bytes */
};

/* Plan file */

struct plan_entry {
  u32 oid;
  u32 priority;				/* ~0U for synthetic robots.txt request */
  byte retry_count;
  byte weight;				/* Weight of the original url_state record */
  byte flags;
  byte section;
  area_t area;
};

enum plan_entry_flags {
  PEF_REFRESH = 1,			/* It's a refresh of already existing entry */
  PEF_SYNTH_ROBOTS = 2,			/* Synthetic robots.txt request, oid corresponds to another URL from that site */
  PEF_ANTICIPATED = 4,			/* Anticipated refresh, can be dropped with no harm [not used by shep-reap, only for statistics] */
  PEF_OVER_AGED = 8,			/* Over-aged refresh [also only stats] */
  PEF_ROBOTS = 16,			/* Real robots.txt (projection of USF_ROBOTS), don't allow filter rejects */
  PEF_SACRISIMMUS = 32,			/* Informs shep-reap that it must not create zombies (for example when we want
					   to download robots.txt). */
};
#define PLAN_ENTRY_FLAG_NAMES "RSAOTs**"

struct plan_site_entry {		/* The plan file is a sequence of these */
  u64 qkey;
  u32 robot_oid;			/* Oid of the robots.txt file if already gathered */
  u32 delay;				/* This one is calculated for qkey, but to simplify the structure we store it per site */
  u32 entry_count;			/* Number of entries of the following format (sorted by priority) */
  struct plan_entry e[0];
};

/* Checkpoint file */

struct checkpoint_entry {
  ucw_time_t time;
  ucw_off_t buckets_pos;
  ucw_off_t journal_pos;
  ucw_off_t contrib_pos;
  ucw_off_t urls_pos;
};

/* state-log.c */

struct state_log_entry {
  struct footprint fp;
  byte source, action;
  byte arg1, arg2;
};

void state_log_open(byte *state);
void state_log_close(void);
void do_state_log(struct state_log_entry *le);

static inline void
state_log(struct site *site, struct url_state *st, uns source, uns action, uns arg1, uns arg2)
{
  if (unlikely(site->monitor))
    {
      struct state_log_entry e = { .fp = st->fp, .source = source, .action = action, .arg1 = arg1, .arg2 = arg2 };
      do_state_log(&e);
    }
}

enum state_log_source {
  LOG_SRC_UNDEFINED,
  LOG_SRC_MERGE,			/* action=LOG_MERGE_xxx, arg1=0, arg2=weight */
  LOG_SRC_SELECT			/* action=LOG_SELECT_xxx, arg1=cause, arg2=select_weight */
};

enum state_log_merge_action {
  LOG_MERGE_CONTRIB,
  LOG_MERGE_DUP_CONTRIB,
  LOG_MERGE_DUP,
  LOG_MERGE_REGATHER,
};

enum state_log_select_action {
  LOG_SELECT_WAKEUP,
  LOG_SELECT_SLEEP,
  LOG_SELECT_DISCARD,
  LOG_SELECT_UNREF,
};

/* state-dir.c */

byte *create_new_state(void);
void delete_state(byte *state);
void clone_state(byte *old, byte *new);

/* state-file.c */

struct fastbuf;

byte *state_file_name(byte *state, byte *name);
struct fastbuf *create_state_file(byte *state, byte *name);
struct fastbuf *append_state_file(byte *state, byte *name);
struct fastbuf *read_state_file(byte *state, byte *name);
struct fastbuf *temp_state_file(void);
void put_state_file(byte *state, byte *name, struct fastbuf *fb, uns clear_flags);

/* state-params.c */

enum state_flags {
  STATE_FLAG_SORTED = 0x1,	/* The index is sorted by footprints */
  STATE_FLAGS_ALL = ~0U,
};
#define STATE_FLAG_NAMES "S*******************************"

struct state_params {
  u32 params_magic;		/* PARAMS_MAGIC */
  u32 format_version;		/* Format of the state */
  u32 flags;			/* STATE_FLAG_... */
};

#define PARAMS_MAGIC 0xaa8a9b55
#define PARAMS_VERSION_CURRENT 0x3b00

void state_params_init(struct state_params *par);
void state_params_new(byte *state);
char *state_params_try_read(byte *fname, struct state_params *par);
void state_params_read(byte *state, struct state_params *par);
void state_params_write(byte *state, struct state_params *par);
void state_flags_change(byte *state, uns mask, uns set);
void state_flags_set(byte *state, uns mask);
void state_flags_clear(byte *state, uns mask);
uns state_flags_get(byte *state);

/* areas.c */

#ifdef CONFIG_AREAS

struct area_info {
  u32 soft_limit;
  u32 hard_limit;
  u32 num_active;
  u32 num_inactive;
  u32 num_gathered;
  u32 plan_limit;
  u32 num_planned;
  u32 rfu[1];
};

extern uns areas_max_id;

void areas_init(byte *state, int writeable);
void areas_cleanup(void);
struct area_info *area_lookup(area_t id, int what_if_nonex);

#endif

/* url-db.c */

enum index_order {
  INDEX_ORDER_UNDEF,
  INDEX_ORDER_BY_OID,
  INDEX_ORDER_BY_FP,
};

#define URL_DB_MAGIC 0x9a2736ab
#define URL_DB_VERSION 0x3b00

struct url_db;

struct url_db_hdr {
  u32 magic;			/* URL_DB_MAGIC */
  u32 version;			/* URL_DB_VERSION */
  ucw_time_t time;		/* Creation time (used to detect cleaned up databases) */
  u32 rfu;			/* Currently zero */
};

struct url_record {
  byte zero;
  byte flags;
  u16 len;
  u32 oid;
  struct footprint fp;
  byte url[0];
};

struct url_db *url_db_open(int mode, uns open_try);
struct url_db *url_db_open_file(byte *path, int mode, uns open_try);
void url_db_close(struct url_db *db);

ucw_off_t url_db_get_size(struct url_db *db);
void url_db_sync(struct url_db *db);
void url_db_write(struct url_db *db, uns oid, byte *url);

struct fastbuf *url_db_sort_records(struct fastbuf *fb);

struct url_record *url_db_find_first(struct url_db *db);
struct url_record *url_db_find_next(struct url_db *db);
struct url_record *url_db_find_last(struct url_db *db);
struct url_record *url_db_find(struct url_db *db, uns oid, uns *gt);
struct url_record *url_db_find_forward(struct url_db *db, uns oid, uns *gt);
ucw_off_t url_db_tell(struct url_db *db);

void url_db_check_header(struct url_db_hdr *hdr);
void url_record_decode(struct url_record *rec);

#endif
