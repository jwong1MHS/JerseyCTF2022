/*
 *	Sherlock Library -- Bucket Files
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#ifndef _SHERLOCK_BUCKET_H
#define _SHERLOCK_BUCKET_H

#include "ucw/fastbuf.h"

/*
 * Format: The object pool is merely a sequence of object buckets.
 * Each bucket starts with struct obuck_header and it's padded
 * by zeros to a multiple of OBUCK_ALIGN bytes.
 *
 * Locking: Each operation on the pool is protected by a flock.
 *
 * The buckets emulate fastbuf streams. Read streams act as normal files,
 * but there can be only one write stream which is non-seekable and you
 * also shouldn't open new read streams when writing.
 *
 * fork()'ing if you don't have any bucket open is safe.
 *
 * You can use the bucket interface from multiple threads in parallel,
 * but since fcntl() locking has no effect on threads, you have to use
 * the OBUCK_NO_LOCK flag and take care of locking yourself. In particular,
 * combining reads of existing buckets and a single append of a new bucket
 * is always safe.
 */

#define OBUCK_SHIFT CONFIG_BUCKET_SHIFT
#define OBUCK_ALIGN (1<<OBUCK_SHIFT)
#define OBUCK_MAGIC 0xdeadf00d
#define OBUCK_INCOMPLETE_MAGIC 0xdeadfee1
#define OBUCK_TRAILER 0xfeedcafe
#define OBUCK_OID_DELETED (~(oid_t)0)
#define OBUCK_OID_ANY (~(oid_t)0)
#define OBUCK_OID_FIRST_SPECIAL (~(oid_t)0xffff)

/* Bucket file parameters */
struct obuck_params {
  uns io_buflen;
  int shake_buflen;
  uns shake_security;
  uns slurp_buflen;
  uns prefetch_size;
  u64 max_size;
};

/* Bucket header */
struct obuck_header {
  u32 magic;			/* OBUCK_MAGIC should dwell here */
  oid_t oid;			/* ID of this object or OBUCK_OID_DELETED */
  u32 length;			/* Length of data in the bucket */
  u32 type;			/* Data type */
  /* Bucket data continue here */
};

/* Contexts (for reentrant reading) */
struct obuck_context {
  ucw_off_t pos;
  struct obuck_header hdr;
};

/* Bucket file handle */
struct obuck {
  /* Parameters */
  struct obuck_params p;
  char *name;

  /* Internals */
  int fd;
  struct fastbuf *write_fb;
  struct obuck_header create_hdr;
  ucw_off_t max_size;

  /* Fast reading of the whole pool */
  struct fastbuf *rpf;
  struct fastbuf limiter;
  uns slurp_remains;
  ucw_off_t slurp_start, slurp_current, slurp_end;
};

/* Special parameters to obuck_find_x functions */
enum obuck_flags {
  OBUCK_FULL = 1,				/* Don't skip deleted buckets */
  OBUCK_NO_LOCK = 2,				/* Do not lock and rely on the caller locking explicitly */
};

void obuck_init(struct obuck *obuck, char *name, int writeable);	/* Initialize the bucket module */
void obuck_cleanup(struct obuck *obuck);	/* Clean up the bucket module */
void obuck_sync(struct obuck *obuck);		/* Flush all buffers to disk */
void obuck_lock_read(struct obuck *obuck);	/* Explicit locking to make sure other threads don't touch buckets now */
void obuck_lock_write(struct obuck *obuck);
void obuck_lock_scan(struct obuck *obuck);
void obuck_unlock(struct obuck *obuck);
void obuck_unlock_scan(struct obuck *obuck);
oid_t obuck_predict_last_oid(struct obuck *obuck); /* Get OID corresponding to the next to be created bucket (i.e., bucket file size estimate) */

/* Searching for buckets */
void obuck_find_by_oid(struct obuck *obuck, struct obuck_context *ctx, uns flags);
int obuck_find_first(struct obuck *obuck, struct obuck_context *ctx, uns flags);
int obuck_find_next(struct obuck *obuck, struct obuck_context *ctx, uns flags);

/* Reading the current bucket */
struct fastbuf *obuck_fetch(struct obuck *obuck, struct obuck_context *ctx);

/*
 * Faster random access to buckets: find_by_oid and fetch combined to a single call
 * which loads the whole bucket into memory at once and serves it as a fastbuf.
 */
struct fastbuf *obuck_fetch_oid(struct obuck *obuck, struct obuck_context *ctx, uns flags);

/* Creating buckets */
struct fastbuf *obuck_create(struct obuck *obuck);
void obuck_create_end(struct obuck *obuck, struct fastbuf *b, uns type, struct obuck_header *hdrp);

/* Deleting buckets */
void obuck_delete(struct obuck *obuck, oid_t oid);

/* Fast reading of the whole pool */
struct fastbuf *obuck_slurp_pool(struct obuck *obuck, struct obuck_header *hdrp, oid_t next_oid);
void obuck_slurp_end(struct obuck *obuck);

/* Convert bucket ID to file position (for size limitations etc.) */

static inline ucw_off_t obuck_get_pos(oid_t oid)
{
  return ((ucw_off_t) oid) << OBUCK_SHIFT;
}

/* Calculate size of bucket which contains given amount of data */

static inline uns obuck_bucket_size(uns len)
{
  return ALIGN_TO(sizeof(struct obuck_header) + len + 4, OBUCK_ALIGN);
}

/* Shaking down bucket file */
void obuck_shakedown(struct obuck *obuck, int (*kibitz)(struct obuck_header *old, oid_t new, byte *buck));

/* A simple interface to the default bucket file */

extern char *bucket_file_name;			/* Buckets.BucketFile (default bucket file) */
extern struct obuck bucket_file;		/* Global variable for default bucket file */

void bucket_open(int writeable);		/* Initialize the default bucket file */
void bucket_close(void);			/* Clean up the default bucket file */
void bucket_set_name(char *name);		/* Change name of the default bucket file */

#endif
