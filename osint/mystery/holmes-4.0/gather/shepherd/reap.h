/*
 *	Sherlock Shepherd Daemon -- The Reaper
 *
 *	(c) 2003--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006--2009 Pavel Charvat <pchar@ucw.cz>
 */

#ifndef _SHERLOCK_GATHER_SHEPHERD_REAP_H
#define _SHERLOCK_GATHER_SHEPHERD_REAP_H

#include "ucw/binheap-node.h"
#include "ucw/url.h"
#include "ucw/workqueue.h"
#include "ucw/mainloop.h"

#include <signal.h>

/* Fake reaper structures */

enum reap_request_type {
  REAP_REQ_RESOLVE,
  REAP_REQ_REFRESH,
  REAP_REQ_GATHER,
};

enum reap_reply_type {
  REAP_REPLY_GATHERED,
};

struct reap_request_hdr {
  u32 type;
  u32 id;
  u32 data_length;
  u32 data_format;
  u32 complexity;
  s32 user_mark;
};

struct reap_reply_hdr {
  u32 type;
  u32 id;
  u32 control_length;
  u32 control_format;
  u32 data_length;
  u32 data_format;
};

/* shep-reap.c */

extern char *url_log_file;
extern struct buck2obj_buf *global_buck_buf;
extern struct mempool *global_buck_pool;
extern char *current_state;
extern uns prefetch_limit;
extern uns prefetch_threads;
extern uns robots_cache_threshold;
extern volatile sig_atomic_t shut_down;

void reset_signals(void);

/* reap-jobs.c */

extern uns downloaded_count, refreshed_count, anticipated_count, num_active_jobs;

struct job {
  cnode n;
  struct qsite *site;
  struct plan_entry *plane;
  u32 request_id;			/* Request id on the connection */
  uns complexity;			/* Estimated complexity */
  struct mempool *pool;			/* This request and everything related to it dwell in this pool */
  struct odes *orig_hdr;		/* Header of the original bucket */
  byte *url;				/* URL we are processing */
  struct url_state journal_entry;	/* Journal entry corresponding to this job */
  struct odes *ctrl;			/* Control part of the reply */
  struct work work;			/* Internal structure of workqueue */
  struct fastbuf *prefetched_bucket;	/* Prefetched bucket fastbuf */
  struct obuck_header prefetched_hdr;	/* ... and its header */
  struct fastbuf *prefetched_robots;	/* Prefetched robots.txt */
  struct obuck_context prefetched_robots_ctx;
  struct odes *robots;			/* Loaded robots.txt */
  uns want_xform;			/* Enable transformation of links */

  struct reap_request_hdr req_hdr;	/* Request */
  struct fastbuf *req_data_fb;

  struct reap_reply_hdr *reply_hdr;	/* Reply */
  byte *reply_ctrl, *reply_data;
  char *reply_src;			/* Reaper id */
};

struct buck2obj_buf;

void jobs_init(void);
void jobs_cleanup(void);
struct job *job_alloc(void);
void job_return(struct job *j);
uns job_prepare_request(struct job *j);
void job_process_reply(struct job *j);
void job_fake_reply(struct job *j, const char *msg);
uns jobs_prefetched_count(void);

/* reap-queue.c */

struct qsite {
  union {				/* Linkage of the site to the outside world */
    cnode list_node;
    struct bh_node heap_node;
  } link;
  oid_t robot_id;			/* Object describing robot file for this site */
  struct odes *robot_cache;		/* Cached robot file */
  struct qnode *qnode;			/* Qnode this site is currently attached to */
  uns qpriority;			/* Priority of the first item in the queue */
  u32 sequence;				/* Sequence number for secondary round-robin ordering */
  struct plan_entry *plan_start, *plan_end; /* Pointers to planned objects */
  u32 new_skey;				/* New skey (0=keep the old one) */
  uns state;
  byte skey_change_cnt;			/* How many times did the skey change recently? */
  byte rfu[3];
};

enum qsite_state {
  SS_IDLE,
  SS_ACTIVE,
  SS_WAITING
};

struct qnode {
  union {				/* Linkage of the node to the outside world */
    cnode list_node;
    struct bh_node heap_node;
    int heap_index;
  } link;
  struct qnode *hash_next;
  u64 qkey;
  u32 qpriority, sequence;		/* Those of the best of the queued sites */
  ucw_time_t last_access;
  ucw_time_t wait_until;
  uns conn_err_count;			/* Number of temporary connection errors seen lately */
  struct bh_heap site_heap;		/* A heap of sites */
  struct qsite *active_site;
  uns std_delay;			/* Standard delay between two accesses */
  uns last_was_autoreply;		/* Last request was processed by an autoreply */
  uns state;
};

enum qnode_state {
  QS_IDLE,
  QS_ACTIVE,
  QS_WAITING,
  QS_READY
};

extern uns initial_plan_entry_count, current_plan_entry_count;

struct qsite *get_site(struct qnode **pnode);
void put_site(struct qsite *h);
int queue_time_step(ucw_time_t *p_wait_seconds);
void load_plan(void);
struct plan_entry *plan_get_next(struct qsite *s);
void plan_unget_next(struct qsite *s);
void plan_reset_site(struct qsite *s);
uns plan_count_qkeys(void);
ucw_off_t jobs_checkpoint_journal(void);
ucw_off_t jobs_checkpoint_buckets(void);
ucw_off_t jobs_checkpoint_urls(void);
uns queue_ready_count(void);

/* reap-contrib.c */

struct cfilter_data {
  byte *url;				/* Set by the caller */
  struct url url_s;			/* Broken-down URL */
  uns want_xform;			/* Enable transformation of links */
  byte *url_xform;			/* Transformed link */
  byte *src_url;			/* Link source */
  uns section;				/* Section number */
  area_t area;				/* Area number */
  uns error_code;			/* Rejection error code */
  int user_mark;			/* Will be passed to reapd */
  byte *content_type;			/* Guessed content-type */
  byte *content_encoding;		/* Guessed content-encoding */
  int stable_time;			/* Time between last significant change and last check */
  uns filter_robots;			/* Enable filtering of robots.txt */
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], buf3[MAX_URL_SIZE];
};

void contrib_flush(void);
byte *verify_contrib(struct cfilter_data *d, uns want_xform);
byte *add_contrib(byte *url, byte *src_url, uns want_xform, int weight, uns flags, int section, area_t area, area_t parent_area);
void write_contribs_obj(struct odes *o, uns want_xform, int parent_weight, area_t parent_area);
void write_contribs(struct job *j);
void contrib_init(byte *new_state);
void contrib_cleanup(void);
ucw_off_t contrib_checkpoint(void);

/* reap-robots.c */

void prefetch_robots(struct job *job);
void fetch_robots(struct job *job, struct buck2obj_buf *buck_buf, struct mempool *buck_pool, struct mempool *robots_pool);
uns check_robots(struct job *job);

/* reap-children.c */

uns child_init(void);
void child_cleanup(void);
uns child_stats(void);

/* reap-conns.c */

uns conn_init(void);
void conn_cleanup(void);
uns conn_stats(void);

#endif
