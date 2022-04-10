/*
 *	Sherlock Gatherer Daemon -- Declarations
 *
 *	(c) 1997--2003 Martin Mares <mj@ucw.cz>
 */

#include "gather/gather.h"
#include "ucw/clists.h"
#include "sherlock/bucket.h"
#include "sherlock/index.h"
#include "ucw/binheap-node.h"

#define OID_FIRST_ERROR		OBUCK_OID_FIRST_SPECIAL
#define OID_UNDEFINED		OBUCK_OID_DELETED

/* gatherd.c */

extern ucw_time_t now;

/* config.c */

extern char *log_name, *host_file_name, *host_bak_name, *queue_file_name;
extern char *lock_name, *urldb_name, *md5db_name;
extern uns max_bucket_file_size, max_host_count;
extern uns max_threads, trace_threads, trace_refs;
extern uns min_server_delay, max_run_time;
extern uns rec_err_dly1, rec_err_dly2, rec_err_limit;
extern uns ignore_refs, soft_max_obj_count, hard_max_obj_count;
extern uns max_rec_err, auto_enqueue_root;
extern uns queue_cache_size, urldb_cache_size, md5db_cache_size;
extern uns dump_full_objects, auto_sync, max_resolvers;
extern uns host_hash_size, key_hash_size, doc_change_mix;
extern uns trickster_err_prob, trickster_step, allow_hard_shutdown;
extern int compress_level;

/* db.c */

void db_sync(void);
void gather_lock(uns allow_override);
void gather_unlock(void);
void gather_note_state(byte *state);

/*
 *  For each URL, we remember the following items to be able to map URL's
 *  to buckets or error codes, to manage the queue and to control refreshing
 *  and expiration.
 *
 *  Initially, http_last_mod is equal to the 'L' attribute in the bucket,
 *  but if we try to refresh the page and we find a new version with identical
 *  contents, we change only 'L' and keep the bucket unchanged. It is in
 *  local time of the HTTP server, hence we should never compare it with
 *  any our timestamps.
 *
 *  Caveat: The "queued" bit doesn't necessarily mean that the URL is really
 *  present in the queue. The exceptions are URL's from overflowed hosts which
 *  need to be recorded in the URL database for later requeueing by the expirer
 *  after something else from the same host gets deleted. Also the expirer
 *  fills the queue only with items which have a chance of being gathered in
 *  the next gatherd run.
 */

struct urlrec {
  u32 access;				/* Time of last access (of queueing if not gathered yet) */
  oid_t oid;				/* Object ID or error code if error */
  u32 http_last_mod;			/* Time of last modification as reported by HTTP */
  u32 avg_change_time;			/* Average time between last two changes */
  byte flags;				/* Various flags: see URF_* below */
  byte retries;				/* Retry count for this URL */
} PACKED;

enum url_flags {
  URF_INITIAL = 1,			/* Part of the initial URL set */
  URF_QUEUED = 2,			/* Queued for gathering */
  URF_REGATHER = 4,			/* Asked for regathering manually */
};

struct md5rec {
  oid_t oid;				/* Can be OID_UNDEFINED when unknown */
};

void urldb_open(void);
void urldb_close(void);
int urldb_lookup(byte *, struct urlrec *);
void urldb_rewind(void);
byte *urldb_get_next(byte *, struct urlrec *);
int urldb_exists(byte *);
int urldb_delete(byte *);
void urldb_store(byte *, struct urlrec *);

void md5db_open(void);
void md5db_close(void);
int md5db_lookup(byte *, struct md5rec *);
void md5db_rewind(void);
byte *md5db_get_next(byte *, struct md5rec *);
int md5db_exists(byte *);
int md5db_delete(byte *);
void md5db_store(byte *, oid_t);

/* queue.c */

#define QUEUE_PAGE_SIZE 2048		/* Must be minimally URL_SIZE - 5 bytes for "xx://" + 8 bytes for service info */
#define QUEUE_PAGE_MASK (QUEUE_PAGE_SIZE-1)

void queue_init(uns flat_mode);
void queue_cleanup(void);
void queue_sync(void);
void queue_reset(void);

struct qhost {				/* Representation of queued host ([*] marks persistent items) */
  union {				/* Linkage of the host to the outside world */
    cnode list_node;
    struct bh_node heap_node;
  } link;
  struct qhost *hash_next;
  uns qf_pos;				/* [*] Reference to first object in queue file */
  uns qf_last;				/* [*] Reference to last object in queue file */
  oid_t robot_id;			/* [*] Object describing robot file for this host */
  u32 robot_time;			/* [*] Time of last robot file refresh */
  uns obj_count[SHERLOCK_NUM_SECTIONS];	/* [*] Number of objects known for this host */
  uns rec_err_count;			/* [*] Number of recoverable errors seen lately */
  uns ex_qh_qcount;			/* Expirer: Number of physically queued items */
  uns hf_pos;				/* Reference to qf_pos instance in host file */
  u32 qkey;				/* [*] Queueing key (e.g., an IP address) */
  uns qpriority;			/* [*] Priority of the first item in the queue */
  u32 sequence;				/* Sequence number for secondary round-robin ordering */
  u16 port;
  byte protocol;
  byte flags;
  byte state;
  byte name[1];
};

#define QHF_DIRTY 		0x01	/* Needs to be flushed */
#define QHF_ALLOCATE		0x04	/* Not assigned a position yet */

enum qhost_state {
  HS_IDLE,
  HS_ACTIVE,
  HS_WAITING
};

struct qnode {
  union {				/* Linkage of the node to the outside world */
    cnode list_node;
    struct bh_node heap_node;
    int heap_index;
  } link;
  struct qnode *hash_next;
  u32 qkey;
  u32 qpriority, sequence;		/* Those of the best of the queued hosts */
  ucw_time_t wait_until;
  uns rec_err_count;			/* Number of recoverable errors seen lately */
  struct bh_heap host_heap;		/* A heap of hosts */
  struct qhost *active_host;
  byte state;
};

enum qnode_state {
  QS_IDLE,
  QS_ACTIVE,
  QS_WAITING,
  QS_READY
};

#define NUM_RESOLVER_KEYS	0x01000000	/* Number of keys reserved for resolvers */

struct qitem {				/* Representation of queued data */
  uns aux;				/* Used internally */
  uns priority;				/* Queue priority */
  byte text[MAX_URL_SIZE];
};

extern uns host_count;

struct qhost *new_host(uns proto, byte *name, uns port);
struct qhost *find_host(uns proto, byte *host, uns port);
int host_time_step(ucw_time_t *p_wait_seconds);
struct qhost *dequeue_host(struct qnode **pnode);	/* Get first ready host */
void finish_host(struct qhost *h, uns delay, u32 new_qkey); /* Done with a dequeued host */
void touch_host(struct qhost *h);			/* Call whenever you've changed persistent host settings */
void put_host(struct qhost *h);				/* Done with a non-dequeued host */
void walk_hosts(void (*f)(struct qhost *h));

struct qitem *dequeue_item(struct qhost *h);
struct qitem *peek_item(struct qhost *h);
void enqueue_item(struct qhost *h, byte *name, uns priority);
void requeue_item(struct qhost *h);
uns queue_walk_start(struct qhost *h);
uns queue_walk_next(uns *pos, struct qitem *qi);

/* refs.c */

struct rfilter_data {
  byte *url;				/* Set by the caller */
  struct url url_s;			/* Broken-down URL */
  uns section;				/* Section number */
  uns section_soft_max;			/* Maximum URL's gathered for a host in this section */
  uns section_hard_max;			/* Maximum URL's remembered for a host in this section */
  uns queue_bonus;			/* Per-URL bonus to queue priority */
  uns qkey;				/* Queueing key */
  byte *content_type;			/* Guessed content-type */
  byte *content_encoding;		/* Guessed content-encoding */
  byte *url_key;			/* URL to be used for detection of already known pages */
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], kbuf[URL_KEY_BUF_SIZE];	/* Internal buffers */
};

void refs_init(uns quick);
byte *ref_filter(struct rfilter_data *data);

void add_ref(byte *r, uns flags);
