/*
 *	Sherlock Library -- Object Buckets
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2004--2005 Robert Spalek <robert@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/bucket.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/lfs.h"
#include "ucw/conf.h"

#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <alloca.h>

/*** Configuration ***/

static struct obuck_params obuck_params = {
  .io_buflen = 65536,
  .shake_buflen = 1048576,
  .shake_security = 0,
  .slurp_buflen = 65536,
  .prefetch_size = 8192,
  .max_size = ~(u64)0,
};

char *bucket_file_name = "db/objects";
struct obuck bucket_file;

static struct cf_section obuck_config = {
  CF_ITEMS {
    CF_STRING("BucketFile", &bucket_file_name),
    CF_UNS("BufSize", &obuck_params.io_buflen),
    CF_INT("ShakeBufSize", &obuck_params.shake_buflen),
    CF_UNS("ShakeSecurity", &obuck_params.shake_security),
    CF_UNS("SlurpBufSize", &obuck_params.slurp_buflen),
    CF_UNS("PrefetchSize", &obuck_params.prefetch_size),
    CF_U64("MaxSize", &obuck_params.max_size),
    CF_END
  }
};

static void CONSTRUCTOR obuck_init_config(void)
{
  cf_declare_section("Buckets", &obuck_config, 0);
}

/*** Internal operations ***/

static void
obuck_broken(struct obuck *obuck, char *msg, ucw_off_t pos)
{
  die("Object pool %s corrupted: %s (pos=%llx)", obuck->name, msg, (long long) pos);
}

/*
 *  We need several types of locks:
 *
 *	Read lock	reading parts of bucket file
 *	Write lock	any write operations
 *	Append lock	appending to the end of the file
 *	Scan lock	reading parts which we are certain they exist
 *
 *  Multiple read and scan locks can co-exist together.
 *  Scan locks can co-exist with an append lock.
 *  There can be at most one write/append lock at a time.
 *  File handle position (and thus seek) is locked by write/append.
 *
 *  These lock types map to a pair of normal read-write locks which
 *  we represent as fcntl() locks on the first and second byte of the
 *  bucket file. [We cannot use flock() since it happily permits
 *  locking a shared fd (e.g., after fork()) multiple times at it also
 *  doesn't offer multiple locks on a single file.]
 *
 *			byte0		byte1
 *	Read		<read>		<read>
 *	Write		<write>		<write>
 *	Append		<write>		-
 *	Scan		-		<read>
 */

static void
obuck_do_lock(struct obuck *obuck, int type, int start, int len)
{
  struct flock fl;

  fl.l_type = type;
  fl.l_whence = SEEK_SET;
  fl.l_start = start;
  fl.l_len = len;
  if (fcntl(obuck->fd, F_SETLKW, &fl) < 0)
    die("fcntl lock: %m");
}

void
obuck_lock_read(struct obuck *obuck)
{
  obuck_do_lock(obuck, F_RDLCK, 0, 2);
}

void
obuck_lock_write(struct obuck *obuck)
{
  obuck_do_lock(obuck, F_WRLCK, 0, 2);
}

static void
obuck_lock_append(struct obuck *obuck)
{
  obuck_do_lock(obuck, F_WRLCK, 0, 1);
}

void
obuck_lock_scan(struct obuck *obuck)
{
  obuck_do_lock(obuck, F_RDLCK, 1, 1);
}

void
obuck_unlock(struct obuck *obuck)
{
  obuck_do_lock(obuck, F_UNLCK, 0, 2);
}

void
obuck_unlock_scan(struct obuck *obuck)
{
  obuck_do_lock(obuck, F_UNLCK, 1, 1);
}

static void
obuck_unlock_append(struct obuck *obuck)
{
  obuck_do_lock(obuck, F_UNLCK, 0, 1);
}

static void
obuck_relock_read_to_scan(struct obuck *obuck)
{
  obuck_unlock_append(obuck);
}

/*** FastIO emulation ***/

struct fb_bucket {
  struct fastbuf fb;
  struct obuck *obuck;
  ucw_off_t start_pos;
  uns bucket_size;
  byte buffer[0];
};
#define FB_BUCKET(f) ((struct fb_bucket *)(f)->is_fastbuf)

static void
obuck_fb_close(struct fastbuf *f)
{
  xfree(f);
}

/* We need to use pread/pwrite since we work on fd's shared between processes */

static int
obuck_fb_refill(struct fastbuf *f)
{
  uns remains, bufsize, size, datasize;

  remains = FB_BUCKET(f)->bucket_size - (uns)f->pos;
  if (!remains)
    return 0;
  f->buffer = FB_BUCKET(f)->buffer;	/* Could have been trimmed by bdirect_read_commit_modified() */
  bufsize = f->bufend - f->buffer;
  ucw_off_t start = FB_BUCKET(f)->start_pos;
  ucw_off_t pos = start + sizeof(struct obuck_header) + f->pos;
  if (remains <= bufsize)
    {
      datasize = remains;
      size = start + obuck_bucket_size(FB_BUCKET(f)->bucket_size) - pos;
    }
  else
    size = datasize = bufsize;
  int l = ucw_pread(FB_BUCKET(f)->obuck->fd, f->buffer, size, pos);
  if (l < 0)
    die("Error reading bucket: %m");
  if ((unsigned) l != size)
    obuck_broken(FB_BUCKET(f)->obuck, "Short read", FB_BUCKET(f)->start_pos);
  f->bptr = f->buffer;
  f->bstop = f->buffer + datasize;
  f->pos += datasize;
  if (datasize < size)
    {
      if (GET_U32(f->buffer + size - 4) != OBUCK_TRAILER)
	obuck_broken(FB_BUCKET(f)->obuck, "Missing trailer", FB_BUCKET(f)->start_pos);
    }
  return datasize;
}

static int
obuck_fb_seek(struct fastbuf *f, ucw_off_t pos, int whence)
{
  ASSERT(whence == SEEK_SET || whence == SEEK_END);
  if (whence == SEEK_END)
    pos += FB_BUCKET(f)->bucket_size;
  ASSERT(pos >= 0 && pos <= (ucw_off_t) FB_BUCKET(f)->bucket_size);
  f->pos = pos;
  return 1;
}

static void
obuck_fb_spout(struct fastbuf *f)
{
  int l = f->bptr - f->buffer;
  char *c = f->buffer;
  ucw_off_t start = FB_BUCKET(f)->start_pos;
  ucw_off_t pos = start + sizeof(struct obuck_header) + f->pos;
  struct obuck *obuck = FB_BUCKET(f)->obuck;
  if (l >= obuck->max_size - pos)
    {
      if (ucw_ftruncate(obuck->fd, start) < 0)
	obuck_broken(obuck, "Maximum bucket file size exceeded and failed to truncate incomplete bucket", start);
      obuck_broken(obuck, "Maximum bucket file size exceeded", start);
    }
  f->pos += l;
  while (l)
    {
      int z = ucw_pwrite(obuck->fd, c, l, pos);
      if (z <= 0)
	die("Error writing bucket: %m");
      pos += z;
      l -= z;
      c += z;
    }
  f->bptr = f->buffer;
}

/*** Exported functions ***/

void
bucket_open(int writeable)
{
  obuck_init(&bucket_file, bucket_file_name, writeable);
}

void
bucket_close(void)
{
  obuck_cleanup(&bucket_file);
}

void
bucket_set_name(char *name)
{
  CF_JOURNAL_VAR(bucket_file_name);
  bucket_file_name = name;
}

static void
obuck_update_params(struct obuck *obuck)
{
  u64 max_size = obuck->p.max_size - 4 - OBUCK_ALIGN;
  if (obuck->p.shake_security)
    max_size -= obuck->p.shake_buflen;
  ASSERT(max_size < obuck->p.max_size);
  max_size = MIN(max_size, (u64)OBUCK_OID_FIRST_SPECIAL << OBUCK_SHIFT);
  max_size = MIN(max_size, ((u64)1 << ((sizeof(ucw_off_t) * 8) - 1)) - 1);
  obuck->max_size = max_size;
}

void
obuck_init(struct obuck *obuck, char *name, int writeable)
{
  ucw_off_t size;

  ASSERT(name && *name);
  obuck->p = obuck_params;
  obuck->name = name;
  obuck->fd = ucw_open(name, (writeable ? O_RDWR | O_CREAT : O_RDONLY), 0666);
  if (obuck->fd < 0)
    die("Unable to open bucket file %s: %m", obuck->name);
  obuck_lock_read(obuck);
  size = ucw_seek(obuck->fd, 0, SEEK_END);
  if (size)
    {
      /* If the bucket pool is not empty, check consistency of its end */
      u32 check;
      if (ucw_pread(obuck->fd, &check, 4, size-4) != 4 ||
	  check != OBUCK_TRAILER)
	obuck_broken(obuck, "Missing trailer of last object", size - 4);
    }
  obuck_update_params(obuck);
  obuck_unlock(obuck);
}

void
obuck_cleanup(struct obuck *obuck)
{
  close(obuck->fd);
  if (obuck->write_fb)
    log(L_ERROR, "Bug: Forgot to close bucket write stream");
}

void
obuck_sync(struct obuck *obuck)
{
  if (obuck->write_fb)
    bflush(obuck->write_fb);
  fsync(obuck->fd);
}

static void
obuck_get(struct obuck *obuck, oid_t oid, struct obuck_context *ctx)
{
  ctx->pos = obuck_get_pos(oid);
  if (ucw_pread(obuck->fd, &ctx->hdr, sizeof(ctx->hdr), ctx->pos) != sizeof(ctx->hdr))
    obuck_broken(obuck, "Short header read", ctx->pos);
  if (ctx->hdr.magic != OBUCK_MAGIC)
    obuck_broken(obuck, "Missing magic number", ctx->pos);
  if (ctx->hdr.oid == OBUCK_OID_DELETED)
    obuck_broken(obuck, "Access to deleted bucket", ctx->pos);
  if (ctx->hdr.oid != oid)
    obuck_broken(obuck, "Invalid backlink", ctx->pos);
}

void
obuck_find_by_oid(struct obuck *obuck, struct obuck_context *ctx, uns flags)
{
  oid_t oid = ctx->hdr.oid;

  ASSERT(oid < OBUCK_OID_FIRST_SPECIAL);
  if (!(flags & OBUCK_NO_LOCK))
    obuck_lock_read(obuck);
  obuck_get(obuck, oid, ctx);
  if (!(flags & OBUCK_NO_LOCK))
    obuck_unlock(obuck);
}

int
obuck_find_first(struct obuck *obuck, struct obuck_context *ctx, uns flags)
{
  ctx->pos = 0;
  ctx->hdr.magic = 0;
  return obuck_find_next(obuck, ctx, flags);
}

int
obuck_find_next(struct obuck *obuck, struct obuck_context *ctx, uns flags)
{
  int c;

  for(;;)
    {
      if (ctx->hdr.magic)
	ctx->pos += obuck_bucket_size(ctx->hdr.length);
      if (!(flags & OBUCK_NO_LOCK))
        obuck_lock_read(obuck);
      c = ucw_pread(obuck->fd, &ctx->hdr, sizeof(ctx->hdr), ctx->pos);
      if (!(flags & OBUCK_NO_LOCK))
        obuck_unlock(obuck);
      if (!c)
	return 0;
      if (c != sizeof(ctx->hdr))
	obuck_broken(obuck, "Short header read", ctx->pos);
      if (ctx->hdr.magic != OBUCK_MAGIC)
	obuck_broken(obuck, "Missing magic number", ctx->pos);
      if (ctx->hdr.oid != OBUCK_OID_DELETED || (flags & OBUCK_FULL))
	return 1;
    }
}

struct fastbuf *
obuck_fetch(struct obuck *obuck, struct obuck_context *ctx)
{
  struct fastbuf *b;
  uns official_buflen = ALIGN_TO(MIN(ctx->hdr.length, obuck->p.io_buflen), OBUCK_ALIGN);
  uns real_buflen = official_buflen + OBUCK_ALIGN;

  b = xmalloc(sizeof(struct fb_bucket) + real_buflen);
  b->buffer = b->bptr = b->bstop = FB_BUCKET(b)->buffer;
  b->bufend = b->buffer + official_buflen;
  b->name = "bucket-read";
  b->pos = 0;
  b->refill = obuck_fb_refill;
  b->spout = NULL;
  b->seek = obuck_fb_seek;
  b->close = obuck_fb_close;
  b->config = NULL;
  b->can_overwrite_buffer = 2;
  FB_BUCKET(b)->start_pos = ctx->pos;
  FB_BUCKET(b)->bucket_size = ctx->hdr.length;
  FB_BUCKET(b)->obuck = obuck;
  return b;
}

struct obuck_fetch_oid_fb {
  struct fastbuf fb;
  struct obuck_header buf;
};

static void
obuck_fetch_oid_fb_close(struct fastbuf *b)
{
  xfree(b);
}

struct fastbuf *
obuck_fetch_oid(struct obuck *obuck, struct obuck_context *ctx, uns flags)
{
  oid_t oid = ctx->hdr.oid;
  ASSERT(oid < OBUCK_OID_FIRST_SPECIAL);
  ucw_off_t pos = ctx->pos = obuck_get_pos(oid);
  uns prefetch = MAX(sizeof(struct obuck_header), obuck->p.prefetch_size);
  struct obuck_fetch_oid_fb *b = xmalloc(sizeof(*b) - sizeof(struct obuck_header) + prefetch);
  bzero(b, sizeof(*b));
  if (!(flags & OBUCK_NO_LOCK))
    obuck_lock_read(obuck);
  ucw_off_t size = ucw_pread(obuck->fd, (byte *)&b->buf, prefetch, pos);
  if (size < (ucw_off_t)sizeof(b->buf))
    obuck_broken(obuck, "Short header read", pos);
  if (b->buf.magic != OBUCK_MAGIC)
    obuck_broken(obuck, "Missing magic number", pos);
  if (b->buf.oid == OBUCK_OID_DELETED)
    obuck_broken(obuck, "Access to deleted bucket", pos);
  if (b->buf.oid != oid)
    obuck_broken(obuck, "Invalid backlink", pos);
  ctx->hdr = b->buf;
  uns size2 = b->buf.length + 4 + ((uns)(OBUCK_ALIGN - sizeof(struct obuck_header) - b->buf.length - 4) & (OBUCK_ALIGN - 1));
  b = xrealloc(b, sizeof(*b) + size2);
  ucw_off_t rest = size2 + sizeof(struct obuck_header) - size;
  if (rest > 0 && ucw_pread(obuck->fd, ((byte *)&b->buf) + size, rest, pos + size) != rest)
    obuck_broken(obuck, "Short read", pos);
  if (!(flags & OBUCK_NO_LOCK))
    obuck_unlock(obuck);
  if (GET_U32((byte *)&b->buf + sizeof(struct obuck_header) + size2 - 4) != OBUCK_TRAILER)
    obuck_broken(obuck, "Missing trailer", pos);
  fbbuf_init_read(&b->fb, (byte *)(&b->buf + 1), b->buf.length, 1);
  b->fb.name = "bucket-fetch";
  b->fb.close = obuck_fetch_oid_fb_close;
  return &b->fb;
}

oid_t
obuck_predict_last_oid(struct obuck *obuck)
{
  ucw_stat_t buf;
  ucw_fstat(obuck->fd, &buf);
  return (oid_t)(buf.st_size >> OBUCK_SHIFT);
}

struct fastbuf *
obuck_create(struct obuck *obuck)
{
  ASSERT(!obuck->write_fb);

  obuck_lock_append(obuck);
  ucw_off_t start = ucw_seek(obuck->fd, 0, SEEK_END);
  if (start & (OBUCK_ALIGN - 1))
    obuck_broken(obuck, "Misaligned file", start);
  if (start >= obuck->max_size)
    obuck_broken(obuck, "Maximum bucket file size exceeded", start);
  obuck->create_hdr.magic = OBUCK_INCOMPLETE_MAGIC;
  obuck->create_hdr.oid = start >> OBUCK_SHIFT;
  obuck->create_hdr.length = 0;
  obuck->create_hdr.type = ~0U;

  struct fastbuf *b = xmalloc(sizeof(struct fb_bucket) + obuck->p.io_buflen);
  obuck->write_fb = b;
  b->buffer = FB_BUCKET(b)->buffer;
  b->bptr = b->bstop = b->buffer;
  b->bufend = b->buffer + obuck->p.io_buflen;
  b->pos = -(int)sizeof(obuck->create_hdr);
  b->name = "bucket-write";
  b->refill = NULL;
  b->spout = obuck_fb_spout;
  b->seek = NULL;
  b->close = NULL;
  b->config = NULL;
  b->can_overwrite_buffer = 0;
  FB_BUCKET(b)->start_pos = start;
  FB_BUCKET(b)->bucket_size = 0;
  FB_BUCKET(b)->obuck = obuck;
  bwrite(b, &obuck->create_hdr, sizeof(obuck->create_hdr));

  return b;
}

void
obuck_create_end(struct obuck *obuck, struct fastbuf *b, uns type, struct obuck_header *hdrp)
{
  ASSERT(b == obuck->write_fb);
  obuck->write_fb = NULL;

  obuck->create_hdr.magic = OBUCK_MAGIC;
  obuck->create_hdr.length = btell(b);
  obuck->create_hdr.type = type;
  int pad = (OBUCK_ALIGN - sizeof(obuck->create_hdr) - obuck->create_hdr.length - 4) & (OBUCK_ALIGN - 1);
  while (pad--)
    bputc(b, 0);
  bputl(b, OBUCK_TRAILER);
  bflush(b);
  ASSERT(!((FB_BUCKET(b)->start_pos + sizeof(obuck->create_hdr) + b->pos) & (OBUCK_ALIGN - 1)));
  if (ucw_pwrite(obuck->fd, &obuck->create_hdr, sizeof(obuck->create_hdr), FB_BUCKET(b)->start_pos) != sizeof(obuck->create_hdr))
    die("Bucket header update failed: %m");
  obuck_unlock_append(obuck);
  memcpy(hdrp, &obuck->create_hdr, sizeof(obuck->create_hdr));
  xfree(b);
}

void
obuck_delete(struct obuck *obuck, oid_t oid)
{
  obuck_lock_write(obuck);
  struct obuck_context ctx;
  obuck_get(obuck, oid, &ctx);
  ctx.hdr.oid = OBUCK_OID_DELETED;
  ucw_pwrite(obuck->fd, &ctx.hdr, sizeof(ctx.hdr), ctx.pos);
  obuck_unlock(obuck);
}

/*** Fast reading of the whole pool ***/

static int
obuck_slurp_refill(struct fastbuf *f)
{
  struct obuck *obuck = SKIP_BACK(struct obuck, limiter, f);
  if (!obuck->slurp_remains)
    return 0;
  uns l = bdirect_read_prepare(obuck->rpf, &f->buffer);
  if (!l)
    obuck_broken(obuck, "Incomplete object", obuck->slurp_start);
  l = MIN(l, obuck->slurp_remains);
  /* XXX: This probably should be bdirect_read_commit_modified() in some cases,
   *      but it doesn't hurt since we aren't going to seek.
   */
  bdirect_read_commit(obuck->rpf, f->buffer + l);
  obuck->slurp_remains -= l;
  f->bptr = f->buffer;
  f->bufend = f->bstop = f->buffer + l;
  f->pos += l;
  return 1;
}

void
obuck_slurp_end(struct obuck *obuck)
{
  if (obuck->rpf)
    {
      bclose(obuck->rpf);
      obuck->rpf = NULL;
      obuck_unlock_scan(obuck);
    }
}

struct fastbuf *
obuck_slurp_pool(struct obuck *obuck, struct obuck_header *hdrp, oid_t next_oid)
{
  do
    {
      if (!obuck->rpf)
	{
	  obuck_lock_read(obuck);
	  /* Note: obuck->rpf has its own file handle, and therefore its own file position */
	  obuck->rpf = bopen(obuck->name, O_RDONLY, obuck->p.slurp_buflen);
	  obuck->slurp_end = bfilesize(obuck->rpf);
	  obuck_relock_read_to_scan(obuck);
	}
      else
	{
	  bsetpos(obuck->rpf, obuck->slurp_current - 4);
	  if (bgetl(obuck->rpf) != OBUCK_TRAILER)
	    obuck_broken(obuck, "Missing trailer", obuck->slurp_start);
	}
      if (next_oid == OBUCK_OID_ANY)
	obuck->slurp_start = btell(obuck->rpf);
      else
	{
	  obuck->slurp_start = obuck_get_pos(next_oid);
	  bsetpos(obuck->rpf, obuck->slurp_start);
	}
      if (obuck->slurp_start >= obuck->slurp_end)
	{
	  obuck_slurp_end(obuck);
	  return NULL;
	}
      if (bread(obuck->rpf, hdrp, sizeof(struct obuck_header)) != sizeof(struct obuck_header))
	obuck_broken(obuck, "Short header read", obuck->slurp_start);
      if (hdrp->magic != OBUCK_MAGIC)
	obuck_broken(obuck, "Missing magic number", obuck->slurp_start);
      obuck->slurp_current = obuck->slurp_start + obuck_bucket_size(hdrp->length);
    }
  while (hdrp->oid == OBUCK_OID_DELETED);
  if (obuck_get_pos(hdrp->oid) != obuck->slurp_start)
    obuck_broken(obuck, "Invalid backlink", obuck->slurp_start);
  obuck->slurp_remains = hdrp->length;

  struct fastbuf *lim = &obuck->limiter;
  lim->bptr = lim->bstop = lim->buffer = lim->bufend = NULL;
  lim->name = "Bucket";
  lim->pos = 0;
  lim->refill = obuck_slurp_refill;
  lim->can_overwrite_buffer = obuck->rpf->can_overwrite_buffer;
  return lim;
}

/*** Shakedown ***/

static inline void
shake_write(struct obuck *obuck, void *addr, int len, ucw_off_t pos)
{
  int l = ucw_pwrite(obuck->fd, addr, len, pos);
  if (l != len)
    {
      if (l < 0)
	die("obuck_shakedown write error: %m");
      else
	die("obuck_shakedown write error: disk full");
    }
}

static inline void
shake_sync(struct obuck *obuck)
{
  if (obuck->p.shake_security > 1)
    ucw_fdatasync(obuck->fd);
}

static void
shake_write_backup(struct obuck *obuck, ucw_off_t bpos, byte *norm_buf, int norm_size, byte *fragment, int frag_size, ucw_off_t frag_pos, int more_size)
{
  struct obuck_header *bhdr;
  int boff = 0;
  int l;
  oid_t old_oid;

  /* First of all, the "normal" part -- everything that will be written in this pass */
  DBG("Backing up first round of changes at position %llx + %x", (long long) bpos, norm_size);
  while (boff < norm_size)
    {
      /* This needn't be optimized for speed. */
      bhdr = (struct obuck_header *) (norm_buf + boff);
      ASSERT(bhdr->magic == OBUCK_MAGIC);
      l = obuck_bucket_size(bhdr->length);
      old_oid = bhdr->oid;
      bhdr->oid = bpos >> OBUCK_SHIFT;
      shake_write(obuck, bhdr, l, bpos);
      bhdr->oid = old_oid;
      boff += l;
      bpos += l;
    }

  /* If we have an incomplete bucket at the end of the buffer, we must copy it as well. */
  if (more_size)
    {
      DBG("Backing up fragment of size %x and %x more", frag_size, more_size);

      /* First the part we already have in the buffer */
      bhdr = (struct obuck_header *) fragment;
      ASSERT(bhdr->magic == OBUCK_MAGIC);
      old_oid = bhdr->oid;
      bhdr->oid = bpos >> OBUCK_SHIFT;
      shake_write(obuck, bhdr, frag_size, bpos);
      bhdr->oid = old_oid;
      bpos += frag_size;

      /* And then the rest, using a small 64K buffer */
      byte *auxbuf = alloca(65536);
      l = 0;
      while (l < more_size)
	{
	  int j = MIN(more_size-l, 65536);
	  if (ucw_pread(obuck->fd, auxbuf, j, frag_pos + frag_size + l) != j)
	    die("obuck_shakedown read error: %m");
	  shake_write(obuck, auxbuf, j, bpos);
	  bpos += j;
	  l += j;
	}
    }
}

static void
shake_erase(struct obuck *obuck, ucw_off_t start, ucw_off_t end)
{
  if (start > end)
    die("shake_erase called with negative length, that's a bug");
  ASSERT(!(start & (OBUCK_ALIGN-1)) && !(end & (OBUCK_ALIGN-1)));
  while (start < end)
    {
      struct obuck_context ctx;
      u32 check = OBUCK_TRAILER;
      ctx.hdr.magic = OBUCK_MAGIC;
      ctx.hdr.oid = OBUCK_OID_DELETED;
      uns len = MIN(0x40000000, end-start);
      ctx.hdr.length = len - sizeof(ctx.hdr) - 4;
      DBG("Erasing %08x bytes at %llx", len, (long long) start);
      shake_write(obuck, &ctx.hdr, sizeof(ctx.hdr), start);
      start += len;
      shake_write(obuck, &check, 4, start-4);
    }
}

void
obuck_shakedown(struct obuck *obuck, int (*kibitz)(struct obuck_header *old, oid_t new, byte *buck))
{
  byte *buf;						/* Shakedown buffer and its size */
  int buflen = ALIGN_TO(obuck->p.shake_buflen, OBUCK_ALIGN);
  char *err;						/* Error message we will print */
  ucw_off_t rstart, wstart;				/* Original and new position of buffer start */
  ucw_off_t r_bucket_start, w_bucket_start;		/* Original and new position of the current bucket */
  int roff, woff;					/* Orig/new position of the current bucket relative to buffer start */
  int rsize;						/* Number of original bytes in the buffer */
  int l;						/* Raw size of the current bucket */
  int changed = 0;					/* "Something has been altered" flag */
  int wrote_anything = 0;				/* We already did a write to the bucket file */
  struct obuck_header *rhdr, *whdr;			/* Original and new address of header of the current bucket */
  ucw_off_t r_file_size;					/* Original size of the bucket file */
  int more;						/* How much does the last bucket overlap the buffer */

  buf = xmalloc(buflen);
  rstart = wstart = 0;
  roff = woff = rsize = 0;

  /* We need to be the only accessor, all the object ID's are becoming invalid */
  obuck_lock_write(obuck);
  r_file_size = ucw_seek(obuck->fd, 0, SEEK_END);
  ASSERT(!(r_file_size & (OBUCK_ALIGN - 1)));
  if (r_file_size >= (0x100000000 << OBUCK_SHIFT) - buflen)
    die("Bucket file is too large for safe shakedown. Shaking down with Bucket.ShakeSecurity=0 will still work.");

  DBG("Starting shakedown. Buffer size is %d, original length %llx", buflen, (long long) r_file_size);

  for(;;)
    {
      r_bucket_start = rstart + roff;
      w_bucket_start = wstart + woff;
      rhdr = (struct obuck_header *)(buf + roff);
      whdr = (struct obuck_header *)(buf + woff);
      if (roff == rsize)
	{
	  more = 0;
	  goto next;
	}
      if (rhdr->magic != OBUCK_MAGIC ||
	  rhdr->oid != OBUCK_OID_DELETED && rhdr->oid != (oid_t)(r_bucket_start >> OBUCK_SHIFT))
	{
	  err = "header mismatch";
	  goto broken;
	}
      l = obuck_bucket_size(rhdr->length);
      if (l > buflen)
	{
	  if (rhdr->oid != OBUCK_OID_DELETED)
	    {
	      err = "bucket longer than ShakeBufSize";
	      goto broken;
	    }
	  /* Empty buckets are allowed to be large, but we need to handle them extra */
	  DBG("Tricking around an extra-large empty bucket at %llx + %x", (long long)r_bucket_start, l);
	  rsize = roff + l;
	}
      else
	{
	  if (rsize - roff < l)
	    {
	      more = l - (rsize - roff);
	      goto next;
	    }
	  if (GET_U32((byte *)rhdr + l - 4) != OBUCK_TRAILER)
	    {
	      err = "missing trailer";
	      goto broken;
	    }
	}
      if (rhdr->oid != OBUCK_OID_DELETED)
	{
	  int status = kibitz(rhdr, w_bucket_start >> OBUCK_SHIFT, (byte *)(rhdr+1));
	  if (status)
	    {
	      int lnew = l;
	      if (status > 1)
		{
		  /* Changed! Reconstruct the trailer. */
		  lnew = obuck_bucket_size(rhdr->length);
		  ASSERT(lnew <= l);
		  PUT_U32((byte *)rhdr + lnew - 4, OBUCK_TRAILER);
		  changed = 1;
		}
	      whdr = (struct obuck_header *)(buf+woff);
	      if (rhdr != whdr)
		memmove(whdr, rhdr, lnew);
	      whdr->oid = w_bucket_start >> OBUCK_SHIFT;
	      woff += lnew;
	    }
	  else
	    changed = 1;
	}
      else
	{
	  kibitz(rhdr, OBUCK_OID_DELETED, NULL);
	  changed = 1;
	}
      roff += l;
      continue;

    next:
      if (changed)
	{
	  /* Write the new contents of the bucket file */
	  if (!wrote_anything)
	    {
	      if (obuck->p.shake_security)
		{
		  /* But first write a backup at the end of the file to ensure nothing can be lost. */
		  shake_write_backup(obuck, r_file_size, buf, woff, buf+roff, rsize-roff, rstart+roff, more);
		  shake_sync(obuck);
		}
	      wrote_anything = 1;
	    }
	  if (woff)
	    {
	      DBG("Write %llx %x", (long long)wstart, woff);
	      shake_write(obuck, buf, woff, wstart);
	      shake_sync(obuck);
	    }
	}
      else
	ASSERT(wstart == rstart);

      /* In any case, update the write position */
      wstart += woff;
      woff = 0;

      /* Skip what's been read and if there is any fragment at the end of the buffer, move it to the start */
      rstart += roff;
      if (more)
	{
	  memmove(buf, buf+roff, rsize-roff);
	  rsize = rsize-roff;
	}
      else
	rsize = 0;

      /* And refill the buffer */
      r_bucket_start = rstart+rsize;	/* Also needed for error messages */
      l = ucw_pread(obuck->fd, buf+rsize, MIN(buflen-rsize, r_file_size - r_bucket_start), r_bucket_start);
      DBG("Read  %llx %x (%x inherited)", (long long)r_bucket_start, l, rsize);
      if (l < 0)
	die("obuck_shakedown read error: %m");
      if (!l)
	{
	  if (!more)
	    break;
	  err = "unexpected EOF";
	  goto broken;
	}
      if (l & (OBUCK_ALIGN-1))
	{
	  err = "garbage at the end of file";
	  goto broken;
	}
      rsize += l;
      roff = 0;
    }

  DBG("Finished at position %llx", (long long) wstart);
  ucw_ftruncate(obuck->fd, wstart);
  shake_sync(obuck);

  obuck_unlock(obuck);
  xfree(buf);
  return;

 broken:
  log(L_ERROR, "Error during object pool shakedown: %s (pos=%lld, id=%x), gathering debris",
      err, (long long) r_bucket_start, (uns)(r_bucket_start >> OBUCK_SHIFT));
  /*
   * We can attempt to clean up the bucket file by erasing everything between the last
   * byte written and the next byte to be read. If the secure mode is switched on, we can
   * guarantee that no data are lost, only some might be duplicated.
   */
  shake_erase(obuck, wstart, rstart);
  die("Fatal error during object pool shakedown");
}

/*** Testing ***/

#ifdef TEST

#define COUNT 5000
#define MAXLEN 10000
#define KILLPERC 13
#define LEN(i) ((259309*(i))%MAXLEN)

static int test_kibitz(struct obuck_header *h, oid_t new, byte *buck)
{
  return 1;
}

int main(int argc, char **argv)
{
  int ids[COUNT];
  unsigned int i, j, cnt;
  struct obuck_header h;
  struct fastbuf *b;

  log_init(NULL);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  unlink(obuck_name);
  obuck_init(1);
  for(j=0; j<COUNT; j++)
    {
      b = obuck_create();
      for(i=0; i<LEN(j); i++)
        bputc(b, (i+j) % 256);
      obuck_create_end(b, BUCKET_TYPE_PLAIN, &h);
      printf("Writing %08x %d\n", h.oid, h.length);
      ids[j] = h.oid;
    }
  for(j=0; j<COUNT; j++)
    if (j % 100 < KILLPERC)
      {
	printf("Deleting %08x\n", ids[j]);
	obuck_delete(ids[j]);
      }
  cnt = 0;
  for(j=0; j<COUNT; j++)
    if (j % 100 >= KILLPERC)
      {
	cnt++;
	h.oid = ids[j];
	obuck_find_by_oid(&h);
	b = obuck_fetch();
	printf("Reading %08x %d\n", h.oid, h.length);
	if (h.length != LEN(j))
	  die("Invalid length");
	for(i=0; i<h.length; i++)
	  if ((unsigned) bgetc(b) != (i+j) % 256)
	    die("Contents mismatch");
	if (bgetc(b) >= 0)
	  die("EOF mismatch");
	bclose(b);
      }
  obuck_shakedown(test_kibitz);
  if (obuck_find_first(&h, 0))
    do
      {
	printf("<<< %08x\t%d\n", h.oid, h.length);
	cnt--;
      }
    while (obuck_find_next(&h, 0));
  if (cnt)
    die("Walk mismatch");
  obuck_cleanup();
  return 0;
}

#endif
