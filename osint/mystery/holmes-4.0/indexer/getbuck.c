/*
 *	Sherlock Indexer -- Getting Buckets from Indexer Sources
 *
 *	(c) 2003--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2004--2007 Robert Spalek <robert@ucw.cz>
 *	(c) 2008 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/conf.h"
#include "ucw/bbuf.h"
#include "ucw/simple-lists.h"
#include "ucw/string.h"
#include "sherlock/bucket.h"
#include "sherlock/object.h"
#include "sherlock/objread.h"
#include "sherlock/lizard-fb.h"
#include "indexer/indexer.h"

#include <fcntl.h>
#include <stdlib.h>
#include <pthread.h>

uns gb_max_count = ~0U;
static uns gb_index;
static uns gb_count;
static uns gb_progress;
static uns gb_progress_max;
static struct fastbuf *gb_fb;
static uns gb_open_count;
static uns gb_threaded;
static struct get_buck *gb_master;
static struct obuck obuck;
static int (*gb_get)(struct get_buck *gb, uns oid);

struct gb_ops {
  int (*get)(struct get_buck *gb, uns oid);
  void (*init)(void);
  void (*cleanup)(void);
};

struct gb_source {
  char *src, *name, *aux;
  const struct gb_ops *ops;
};

static struct gb_source *gb_sources, *gb_source;
static uns gb_num_sources, gb_cur_source;

static void
gb_report_skip(struct get_buck *gb, uns index)
{
  log(L_ERROR, "Bucket %x of type %x skipped: %m", gb->oid, gb->type);
  if (index != ~0U)
    die("Fatal error: Bucket is unreadable, although it was readable before a while");
}

/*** Backward-Compatible Bucket Source ***/

static int
gb_old_get(struct get_buck *gb, uns index)
{
  struct obuck_header bh;
  struct fastbuf *f;

  for(;;)
    {
      f = obuck_slurp_pool(&obuck, &bh, ~0U);
      if (!f)
        return 0;
      gb_progress = bh.oid;
      gb->oid = bh.oid;
      gb->type = bh.type;
      gb->o = obj_read_bucket(gb->buck_buf, gb->pool, gb->type, bh.length, f, NULL, !gb_threaded);
      gb_index++;
      if (index != ~0U && gb_index < index)
	continue;
      if (likely(gb->o != NULL))
	return 1;
      gb_report_skip(gb, index);
    }
}

static void
gb_old_cleanup(void)
{
  obuck_cleanup(&obuck);
}

static void
gb_old_init(void)
{
  obuck_init(&obuck, gb_source->name, 0);
  gb_progress_max = obuck_predict_last_oid(&obuck);
}

static const struct gb_ops gb_old_ops = {
  .init = gb_old_init,
  .cleanup = gb_old_cleanup,
  .get = gb_old_get
};

/*** Text Source ***/

static int
gb_text_get_line(void)
{
  byte *buf = bgets_stk(gb_fb);
  if (!buf)
    return 0;
  if (!buf[0])
    {
      gb_index++;
      gb_progress++;
    }
  return 1;
}

static int
gb_text_get(struct get_buck *gb, uns index)
{
  if (index != ~0U)
    while (gb_index + 1 != index)
      if (!gb_text_get_line())
        {
	  return 0;
	}
  gb->type = BUCKET_TYPE_PLAIN;
  gb->oid = gb_progress;
  gb->o = obj_new(gb->pool);
  if (!obj_read(gb_fb, gb->o))
    return 0;
  gb_progress++;
  gb_index++;
  return 1;
}

static void
gb_text_cleanup(void)
{
  bclose(gb_fb);
}

static void
gb_text_init(void)
{
  gb_fb = bopen_file(gb_source->name, O_RDONLY, &indexer_stream_params);
  gb_progress_max = ~0U;
}

static const struct gb_ops gb_text_ops = {
  .init = gb_text_init,
  .cleanup = gb_text_cleanup,
  .get = gb_text_get
};

/*** Raw bucket source ***/

static uns
bskip_align(struct fastbuf *fb)
{
  uns align = (1 << CARD_POS_SHIFT) - 1;
  ucw_off_t pos = btell(fb);
  uns skip = pos & align;
  if (skip)
    bskip(fb, align+1-skip);
  return (pos + align) >> CARD_POS_SHIFT;
}

static int
gb_raw_get(struct get_buck *gb, uns index)
{
  uns card_id = bskip_align(gb_fb);
  if (index != ~0U)
    while (gb_index + 1 != index)
      {
	uns len = bgetl(gb_fb);
	if (len == ~0U)
	  return 0;
	bskip(gb_fb, len);
	gb_progress++;
	gb_index++;
	card_id = bskip_align(gb_fb);
      }
  uns len = bgetl(gb_fb);
  gb->type = bgetc(gb_fb) + BUCKET_TYPE_PLAIN;
  gb->oid = card_id;
  uns sh = LIZARD_COMPRESS_HEADER - 1;
  gb->o = obj_read_bucket(gb->buck_buf, gb->pool, gb->type, len-sh, gb_fb, NULL, !gb_threaded);
  if (!gb->o)
    return 0;
  gb_progress++;
  gb_index++;
  return 1;
}

static void
gb_raw_cleanup(void)
{
  bclose(gb_fb);
}

static void
gb_raw_init(void)
{
  gb_fb = bopen_file(gb_source->name, O_RDONLY, &indexer_stream_params);
  gb_progress_max = ~0U;
}

static const struct gb_ops gb_raw_ops = {
  .init = gb_raw_init,
  .cleanup = gb_raw_cleanup,
  .get = gb_raw_get
};

#ifdef CONFIG_SHEPHERD_PROTOCOL

/*** V3.0 Local Bucket Source ***/

#include "gather/shepherd/export.h"

static struct fastbuf *bucket_index_f;		/* Reading of bucket index */

static int
gb_v30_get(struct get_buck *gb, uns index)
{
  for(;;)
    {
      /* Find the index entry */
      struct export_entry e;
      if (!bread(bucket_index_f, &e, sizeof(e)))
	return 0;
      gb_progress++;
      gb_index++;
      if (index != ~0U && gb_index != index)
	continue;
      gb->oid = e.oid;

      /* Find the bucket */
      struct obuck_header bh;
      struct fastbuf *f;
      f = obuck_slurp_pool(&obuck, &bh, e.oid);
      if (unlikely(!f))
	die("Inconsistent bucket file index");
      gb->type = bh.type;

      /* Fetch the bucket */
      gb->o = obj_read_bucket(gb->buck_buf, gb->pool, gb->type, bh.length, f, NULL, !gb_threaded);
      if (likely(gb->o != NULL))
	{
	  /* Add some more header fields */
	  obj_set_attr_num(gb->o, 'V', e.last_checked_time);
	  obj_add_attr_format(gb->o, 'W', "g%d", e.weight);
	  return 1;
	}

      gb_report_skip(gb, index);
    }
}

static void
gb_v30_cleanup(void)
{
  obuck_cleanup(&obuck);
  bclose(bucket_index_f);
}

static void
gb_v30_init(void)
{
  obuck_init(&obuck, gb_source->name, 0);
  bucket_index_f = bopen(gb_source->aux, O_RDONLY, 65536);
  gb_progress_max = (uns) bfilesize(bucket_index_f) / sizeof(struct export_entry);
}

static const struct gb_ops gb_v30_ops = {
  .init = gb_v30_init,
  .cleanup = gb_v30_cleanup,
  .get = gb_v30_get
};

/*** V3.0 Remote Bucket Source ***/

#include "gather/shepherd/protocol.h"

static int gb_fd;
static int gb_open;
static u32 gb_id;
static struct fastbuf fd_f;
static bb_t gb_buf;

static int
gb_fd_refill(struct fastbuf *f)
{
  if (!gb_open)
    return 0;
  struct shepp_packet_hdr rp;
  int l = careful_read(gb_fd, &rp, sizeof(rp));
  if (!l)
    die("Unexpected EOF from remote server");
  if (l < 0)
    die("Error reading from remote server: %m");
  DBG("Received %08x +%d", rp.type, rp.data_len);
  if (rp.leader != SHEPP_LEADER ||
      rp.id != gb_id ||
      (rp.type != SHEPP_REPLY_DATA_BLOCK || !rp.data_len) &&
      (rp.type != SHEPP_REPLY_DATA_END || rp.data_len))
    die("Protocol violation by remote server (reply %08x)", rp.type);
  if (rp.type == SHEPP_REPLY_DATA_END)
    {
      gb_open = 0;
      return 0;
    }
  bb_grow(&gb_buf, rp.data_len);
  f->buffer = gb_buf.ptr;
  f->bufend = gb_buf.ptr + gb_buf.len;
  l = careful_read(gb_fd, f->buffer, rp.data_len);
  if (!l)
    die("Unexpected EOF from remote server");
  if (l < 0)
    die("Error reading from remote server: %m");
  f->bptr = f->buffer;
  f->bstop = f->buffer + rp.data_len;
  return 1;
}

static int
gb_fd_get(struct get_buck *gb, uns index)
{
  for(;;)
    {
      struct shepp_bucket_header sh;
      if (unlikely(!breadb(&fd_f, &sh, sizeof(sh))))
	return 0;
      gb_progress++;
      gb_index++;

      DBG("### %08x %08x %08x (%08x)", sh.length, sh.type, sh.oid, index);
      gb->type = sh.type;
      gb->oid = sh.oid;
      if (index == ~0U || gb_index == index)
	{
	  gb->o = obj_read_bucket(gb->buck_buf, gb->pool, gb->type, sh.length, &fd_f, NULL, !gb_threaded);
	  if (likely(gb->o != NULL))
	    return 1;
	  gb_report_skip(gb, index);
	  die("Inconsistent input, unable to continue");	/* We don't know how to skip the rest of the bucket */
	}
      else
	bskip(&fd_f, sh.length);
    }
}

static void
gb_fd_cleanup(void)
{
  if (gb_open)
    gb_fd_get(gb_master, ~1U);
  bb_done(&gb_buf);
}

static void
gb_fd_init(void)
{
  gb_fd = atol(gb_source->name);
  gb_progress_max = atol(gb_source->aux);

  fd_f.name = "bucket-input";
  fd_f.refill = gb_fd_refill;
  /*
   *  The buffer is fully overwritable, but in case we use multiple threads,
   *  we disable zero-copy reading as otherwise the resulting object could
   *  point directly to the buffer, which could be stomped upon by another
   *  thread later. Moreover, most buckets are either short or compressed,
   *  so the extra copy usually takes place anyway.
   */
  fd_f.can_overwrite_buffer = 2;
  bb_init(&gb_buf);

  struct shepp_packet_hdr req;
  req.leader = SHEPP_LEADER;
  req.type = SHEPP_REQ_SEND_BUCKETS;
  req.id = ++gb_id;
  req.data_len = 0;
  if (careful_write(gb_fd, &req, sizeof(req)) <= 0)
    die("Error sending fetch request: %m");
  gb_open = 1;
}

static const struct gb_ops gb_fd_ops = {
  .init = gb_fd_init,
  .cleanup = gb_fd_cleanup,
  .get = gb_fd_get
};

#endif

/*** Multiplexer ***/

static pthread_mutex_t gb_mutex;

static void
gb_init_source(struct gb_source *gbs, char *source)
{
  char *w[4];
  int e = str_sepsplit(cf_strdup(source), ':', w, ARRAY_SIZE(w));
  if (e <= 0)
    die("Indexer.Source: Invalid syntax");
  gbs->src = source;
  gbs->name = w[1];
  gbs->aux = w[2];

  const struct gb_ops *ops;
  if (!strcmp(w[0], "bucket") && e == 2)
    ops = &gb_old_ops;
  else if (!strcmp(w[0], "text") && e == 2)
    ops = &gb_text_ops;
  else if (!strcmp(w[0], "raw") && e == 2)
    ops = &gb_raw_ops;
#ifdef CONFIG_SHEPHERD_PROTOCOL
  else if (!strcmp(w[0], "indexed") && e == 3)
    ops = &gb_v30_ops;
  else if (!strcmp(w[0], "remote"))
    die("Indexer.Source: Remote sources must be run through iconnect first");
  else if (!strcmp(w[0], "fd") && e == 3)
    ops = &gb_fd_ops;
#endif
  else
    die("Indexer.Source: Unknown source type");
  gbs->ops = ops;
}

static void
gb_init_sources(void)
{
  gb_cur_source = 0;
  gb_num_sources = clist_size(&indexer_sources);
  if (!gb_num_sources)
    die("Indexer.Source: No source defined");
  gb_sources = cf_malloc(gb_num_sources * sizeof(*gb_sources));
  uns i = 0;
  CLIST_FOR_EACH(struct simp_node *, s, indexer_sources)
    gb_init_source(gb_sources + i++, s->s);
}

static void
gb_connect(void)
{
  gb_progress = 0;
  gb_source = gb_sources + gb_cur_source;
  ITRACE("Reading %s", gb_source->src);
  gb_source->ops->init();
  gb_get = gb_source->ops->get;
}

static void
gb_disconnect(void)
{
  gb_sources[gb_cur_source].ops->cleanup();
}

static uns
gb_next_source(void)
{
  if (gb_cur_source >= gb_num_sources - 1)
    return 0;
  gb_disconnect();
  gb_cur_source++;
  gb_connect();
  return 1;
}

void
get_buck_init(struct get_buck *gb)
{
  if (!gb_open_count++)
    {
      gb_init_sources();
      gb_connect();
      gb_master = gb;
      gb_index = ~0U;
      gb_count = 0;
    }
  if (gb_open_count > 1)
    gb_threaded = 1;

  gb->buck_buf = buck2obj_alloc();
  gb->progress_max = gb_progress_max;
  gb->num_sources = gb_num_sources;
  pthread_mutex_init(&gb_mutex, NULL);
}

int
get_buck_next(struct get_buck *gb, uns index)
{
  if (gb_threaded)
    pthread_mutex_lock(&gb_mutex);
  int ok = 0;
  if (gb_count < gb_max_count)
    {
      while (!(ok = gb_get(gb, index)) && gb_next_source());
      if (ok)
        {
          gb->progress_current = gb_progress;
          gb->progress_count = ++gb_count;
          gb->cur_source = gb_cur_source;
          gb->index = gb_index;
	}
    }
  if (gb_threaded)
    pthread_mutex_unlock(&gb_mutex);
  return ok;
}

void
get_buck_cleanup(struct get_buck *gb)
{
  ASSERT(gb_open_count);
  if (!--gb_open_count)
    gb_disconnect();
  buck2obj_free(gb->buck_buf);
}

#ifdef TEST

#include "ucw/getopt.h"
#include "ucw/mempool.h"

int main(int argc, char **argv)
{
  log_init(argv[0]);
  if ((cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0) || optind != argc)
    return 1;

  struct mempool *mp = mp_new(16384);
  struct get_buck gb;
  get_buck_init(&gb);
  gb.pool = mp;
  struct fastbuf *out = bfdopen_shared(1, 65536);
  for (; get_buck_next(&gb, ~0U); mp_flush(mp))
    {
      bprintf(out, "# %08x (#%d; %d of %d), type %08x\n", gb.oid, gb.progress_count, gb.progress_current, gb.progress_max, gb.type);
      obj_write(out, gb.o, BUCKET_TYPE_PLAIN);
      bputc(out, '\n');
    }
  bclose(out);
  get_buck_cleanup(&gb);
  return 0;
}

#endif
