/*
 *	Sherlock Indexer -- Initial Object Scanning
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2002--2006 Robert Spalek <robert@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/md5.h"
#include "ucw/mempool.h"
#include "ucw/url.h"
#include "ucw/bitarray.h"
#include "ucw/base224.h"
#include "ucw/lfs.h"
#include "ucw/unicode.h"
#include "ucw/hashfunc.h"
#include "ucw/semaphore.h"
#include "ucw/threads.h"
#include "sherlock/object.h"
#include "sherlock/attrset.h"
#include "sherlock/tagged-text.h"
#include "sherlock/math.h"
#include "charset/unicat.h"
#include "indexer/indexer.h"
#include "indexer/matcher.h"
#include "indexer/params.h"
#include "indexer/graph.h"
#include "filter/filter.h"
#include "analyser/analyser.h"
#include "lang/lang.h"
#include "lang/detect.h"

#ifdef CONFIG_IMAGES_DUP
#include "indexer/images.h"
#include "images/object.h"
#include "images/duplicates.h"
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>

/* Global structures */

static struct index_params parameters;

/* Configuration */

static uns scan_batch = 10;
static uns filter_links;

static struct cf_section scanner_config = {
  CF_ITEMS {
    CF_UNS("BatchSize", &scan_batch),
    CF_UNS("FilterLinks", &filter_links),
    CF_END
  }
};

/*
 *  Per-thread contexts
 *
 *  The scanner runs in multiple threads which process buckets in parallel.
 *  Each one works over its own context and it uses atomic fastbufs to write
 *  most of the output files without any locking. The only tricky part is
 *  allocation of object ID's and writing of notes and attributes, handled
 *  by the alloc_...() functions below.
 *
 *  Initialization and cleanup is performed in several stages:
 *
 *	- init	      Global initialization before contexts are created
 *	- start(c)    At the beginning of each thread, serialized by a lock
 *		      and guaranteed to finish before any of the threads starts
 *		      real operation. (This is better than initializing everything
 *		      before the threads are forked off, because on NUMA systems
 *		      we allocate resources from the node where the thread runs.)
 *		      Also, context 0 is always initialized before other contexts.
 *	- stop(c)     At the end of each thread, not serialized
 *	- finish(c)   After all threads finish
 *	- cleanup     Global cleanup
 */

struct scan_context {
  /* Context ID and the associated thread */
  uns context_id;
  pthread_t thread;

  /* Currently processed object */
  struct mempool *pool;
  struct get_buck get_buck;
  struct odes *obj;
  byte *url;
  struct card_attr *attr;
  struct card_note *note;
  uns id;

  /* Counters */
  uns count_in;
  uns count_ok;
  uns count_skel;
  uns count_err;
  uns count_bots;

  /* Matcher context */
  struct matcher_context *matcher_context;

  /* Analyser context and its temporary fastbufs */
  struct fastbuf *fb_text, *fb_metas, *fb_thumbnail;
  struct an_context an_context;

  /* Atomic fastbufs for output files */
  struct fastbuf *fingerprints;
  struct fastbuf *labels_by_id;
  struct fastbuf *checksums;
  struct fastbuf *links;
  struct fastbuf *signatures;
  struct fastbuf *reftexts;
  struct fastbuf *image_thumbnails;

  /* Filter context */
  struct filter_args *filter_args;
  int bonus, card_bonus, area;
  struct url url_s;
  uns queue_key;
  byte *language;
  byte *title;
  byte *site_name;
  int site_level;
  int image_size;
  int image_aspect_ratio;
  int image_colors;
  int audio_length;
  int audio_bitrate;
  int audio_srate;
  int audio_channels;
  uns want_xform;
  uns noindex;

  /* Transformation of links */
  struct filter_args *xform_args;
  byte *link;
  struct url link_s;
  byte *url_xform;
  byte link_buf[MAX_URL_SIZE];
};

static struct scan_context *scan_contexts;

#define TDBG(c, m, a...) DBG("Thread %d: " m, c->context_id, ##a)

/*
 *  Allocation of card ID's and handling of attributes and notes
 *
 *  This is not easy, because we need to guard ID allocation by locks
 *  and write the corresponding attributes, notes and URL's in the correct
 *  order. Furthermore, there is a lot of things to do between getting
 *  the ID and writing the attributes, so we must avoid holding a lock
 *  during all that time.
 *
 *  We use a little tricky, but simple solution: We keep an array of attributes
 *  and notes buffered for writing and every thread gets the allocated ID accompanied
 *  by pointers to the corresponding structures where it can fill in the contents.
 *  When the buffers fill up, we announce flushing and we wait for all threads
 *  to enter the allocation function or finish their operation. In both cases,
 *  it's guaranteed that they have ceased modifying the buffers, so we can flush
 *  them to disk and let all threads continue again.
 */

/* Buffers for normal cards */
static int fd_attrs, fd_notes;
static struct card_attr *attr_buf;
static struct card_note *note_buf;
static uns out_buf_cnt;
static uns id_out;

/* Buffers for skeletons */
static int fd_skel;
static struct card_note *skel_buf;
static uns skel_buf_cnt;
static uns id_skel = FIRST_ID_SKEL;

static struct fastbuf *urls;
static struct fastbuf *url_index;
static struct fastbuf *skel_urls;

static pthread_mutex_t alloc_mutex;		/* Protects access to the global state */
static sem_t *alloc_flush_sem;			/* Used for waiting for all threads on flush */
static pthread_cond_t sync_cond;		/* Used for waiting for end of flush with alloc_mutex held */
static uns alloc_flushing;			/* We are currently flushing */
static uns alloc_running_threads;		/* Number of threads which still run */
static uns alloc_batch_count;

static int
alloc_open_file(char *name)
{
  name = index_name(name);
  int fd = ucw_open(name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd < 0)
    die("Cannot open %s: %m", name);
  return fd;
}

static void
alloc_init(void)
{
  if (!scan_batch)
    {
      scan_batch = indexer_fb_size / MAX(sizeof(struct card_attr), sizeof(struct card_note));
      log(L_DEBUG, "Automatically calibrating batch size to %d", scan_batch);
      ASSERT(scan_batch);
    }

  fd_attrs = alloc_open_file(fn_attributes);
  attr_buf = big_alloc(scan_batch * sizeof(struct card_attr));
  fd_notes = alloc_open_file(fn_notes);
  note_buf = big_alloc(scan_batch * sizeof(struct card_note));

  if (index_name_defined(fn_notes_skel))
    {
      fd_skel = alloc_open_file(fn_notes_skel);
      skel_buf = big_alloc(scan_batch * sizeof(struct card_note));
    }

  urls = index_maybe_bopen(fn_urls, O_CREAT | O_TRUNC | O_WRONLY, 1);
  url_index = index_maybe_bopen(fn_url_index, O_CREAT | O_TRUNC | O_WRONLY, 1);
  skel_urls = index_maybe_bopen(fn_skel_urls, O_CREAT | O_TRUNC | O_WRONLY, 1);

  pthread_mutex_init(&alloc_mutex, NULL);
  pthread_cond_init(&sync_cond, NULL);
  alloc_flush_sem = sem_alloc();
  alloc_running_threads = indexer_threads;
}

static void
alloc_write(int fd, void *buf, int size)
{
  int ret = write(fd, buf, size);
  if (ret < 0)
    die("Error writing attributes and notes: %m");
  if (ret != size)
    die("Unexpected short write: %d of %d", ret, size);
}

static void
alloc_flush(uns is_skeleton)
{
  if (is_skeleton)
    {
      if (skel_buf_cnt)
	{
	  alloc_write(fd_skel, skel_buf, skel_buf_cnt * sizeof(struct card_note));
	  skel_buf_cnt = 0;
	}
    }
  else
    {
      if (out_buf_cnt)
	{
	  alloc_write(fd_attrs, attr_buf, out_buf_cnt * sizeof(struct card_attr));
	  alloc_write(fd_notes, note_buf, out_buf_cnt * sizeof(struct card_note));
	  out_buf_cnt = 0;
	}
    }
  alloc_batch_count++;
}

static inline int
alloc_avail(uns is_skeleton)
{
  return (is_skeleton ? skel_buf_cnt : out_buf_cnt) < scan_batch;
}

static void
do_alloc_id(struct scan_context *c, uns is_skeleton)
{
  if (is_skeleton)
    {
      c->id = id_skel++;
      c->attr = NULL;
      c->note = &skel_buf[skel_buf_cnt++];
      if (skel_urls)
        bputsn(skel_urls, c->url);
    }
  else
    {
      c->id = id_out++;
      c->attr = &attr_buf[out_buf_cnt];
      c->note = &note_buf[out_buf_cnt++];
      bputo(url_index, btell(urls));
      bputsn(urls, c->url);
      bzero(c->attr, sizeof(*c->attr));
    }
  bzero(c->note, sizeof(*c->note));
  TDBG(c, "Allocated %s id 0x%x", is_skeleton ? "skel" : "card", c->id);
}

static void
alloc_id(struct scan_context *c, uns is_skeleton)
{
  if (indexer_threads == 1)
    {
      if (!alloc_avail(is_skeleton))
	alloc_flush(is_skeleton);
      do_alloc_id(c, is_skeleton);
      return;
    }

  pthread_mutex_lock(&alloc_mutex);
 restart:
  if (unlikely(alloc_flushing))
    {
      TDBG(c, "Sync");
      sem_post(alloc_flush_sem);
      pthread_cond_wait(&sync_cond, &alloc_mutex);
      TDBG(c, "Resumed");
      goto restart;
    }
  if (unlikely(!alloc_avail(is_skeleton)))
    {
      TDBG(c, "Need flush");
      alloc_flushing = 1;
      uns cnt = alloc_running_threads - 1;
      pthread_mutex_unlock(&alloc_mutex);
      TDBG(c, "Waiting for %d threads", cnt);
      while (cnt--)
	sem_wait(alloc_flush_sem);
      pthread_mutex_lock(&alloc_mutex);

      alloc_flush(is_skeleton);

      TDBG(c, "Flushed");
      alloc_flushing = 0;
      pthread_cond_broadcast(&sync_cond);
    }
  do_alloc_id(c, is_skeleton);
  pthread_mutex_unlock(&alloc_mutex);
}

static void
alloc_stop(struct scan_context *c UNUSED)
{
  pthread_mutex_lock(&alloc_mutex);
  alloc_running_threads--;
  if (alloc_flushing)
    {
      TDBG(c, "Final sync");
      sem_post(alloc_flush_sem);
    }
  pthread_mutex_unlock(&alloc_mutex);
}

static void
alloc_cleanup(void)
{
  alloc_flush(0);
  alloc_flush(1);
  sem_free(alloc_flush_sem);
  log(L_DEBUG, "%d batches", alloc_batch_count);

  close(fd_attrs);
  close(fd_notes);
  if (skel_buf)
    close(fd_skel);
  bclose(urls);
  bclose(url_index);
  bclose(skel_urls);

  struct fastbuf *merges = index_maybe_bopen(fn_merges, O_CREAT | O_TRUNC | O_WRONLY, 0);
  if (merges)
    {
      for (uns i=0; i<id_out; i++)
	bputl(merges, ~0U);
      bclose(merges);
    }
}

/* Reading of input */

static void
read_init(void)
{
  gb_max_count = max_num_objects;
}

static void
read_start(struct scan_context *c)
{
  c->get_buck.pool = c->pool;
  get_buck_init(&c->get_buck);
}

static void
read_finish(struct scan_context *c)
{
  get_buck_cleanup(&c->get_buck);
}

static uns
read_next(struct scan_context *c)
{
  mp_flush(c->pool);
  struct get_buck *gb = &c->get_buck;
  if (!get_buck_next(gb, ~0U))
    return 0;

  PROGRESS_LOCKED(gb->progress_count, alloc_mutex,
		  "scanner: %d objects -> %d cards, %d skels (src %d, %d%%)",
		  gb->progress_count, id_out, id_skel - FIRST_ID_SKEL, gb->cur_source,
		  (int)((float)gb->progress_current/gb->progress_max*100));
  c->count_in++;
  c->obj = gb->o;
  TDBG(c, "Read obj %d", gb->oid);
  return 1;
}

/* Filtering */

static struct filter_binding scan_bindings[] = {
  /* URL and its parts */
  { "url",		OFFSETOF(struct scan_context, url) },
  { "protocol",		OFFSETOF(struct scan_context, url_s.protocol) },
  { "host",		OFFSETOF(struct scan_context, url_s.host) },
  { "port",		OFFSETOF(struct scan_context, url_s.port) },
  { "path",		OFFSETOF(struct scan_context, url_s.rest) },
  { "username",		OFFSETOF(struct scan_context, url_s.user) },
  { "password",		OFFSETOF(struct scan_context, url_s.pass) },
  /* Gatherer attributes */
  { "queue_key",	OFFSETOF(struct scan_context, queue_key) },
  /* Attributes */
  { "bonus",		OFFSETOF(struct scan_context, bonus) },
  { "card_bonus",	OFFSETOF(struct scan_context, card_bonus) },
  { "site",		OFFSETOF(struct scan_context, site_name) },
  { "site_level",	OFFSETOF(struct scan_context, site_level) },
#ifdef CONFIG_LANG
  { "language",		OFFSETOF(struct scan_context, language) },
#endif
  { "title",		OFFSETOF(struct scan_context, title) },
  { "image_size",	OFFSETOF(struct scan_context, image_size) },
  { "image_aspect_ratio",	OFFSETOF(struct scan_context, image_aspect_ratio) },
  { "image_colors",	OFFSETOF(struct scan_context, image_colors) },
  { "audio_length",	OFFSETOF(struct scan_context, audio_length) },
  { "audio_bitrate",	OFFSETOF(struct scan_context, audio_bitrate) },
  { "audio_srate",	OFFSETOF(struct scan_context, audio_srate) },
  { "audio_channels",	OFFSETOF(struct scan_context, audio_channels) },
#ifdef CONFIG_AREAS
  /* Area ID */
  { "area",		OFFSETOF(struct scan_context, area) },
#endif
  { "want_xform",	OFFSETOF(struct scan_context, want_xform) },
  { "noindex",		OFFSETOF(struct scan_context, noindex) },
  { NULL,		0 }
};

static struct filter_binding xform_bindings[] = {
  { "url",		OFFSETOF(struct scan_context, link) },
  { "protocol",		OFFSETOF(struct scan_context, link_s.protocol) },
  { "host",		OFFSETOF(struct scan_context, link_s.host) },
  { "port",		OFFSETOF(struct scan_context, link_s.port) },
  { "path",		OFFSETOF(struct scan_context, link_s.rest) },
  { "username",		OFFSETOF(struct scan_context, link_s.user) },
  { "password",		OFFSETOF(struct scan_context, link_s.pass) },
  { "url_xform",	OFFSETOF(struct scan_context, url_xform) },
  { "src_url",		OFFSETOF(struct scan_context, url) },
  { NULL,		0 }
};

static struct filter *scan_filter_program, *xform_filter_program;

static void
scan_filter_init(void)
{
  if (!indexer_filter_name || !indexer_filter_name[0])
    return;
  scan_filter_program = filter_load(indexer_filter_name, filter_builtin_vars, scan_bindings, NULL);
  if (filter_links)
    xform_filter_program = filter_load(indexer_filter_name, filter_builtin_vars, xform_bindings, NULL);
}

static void
scan_filter_start(struct scan_context *c)
{
  if (scan_filter_program)
    c->filter_args = filter_intr_new(filter_clone(scan_filter_program));
  if (xform_filter_program)
    c->xform_args = filter_intr_new(filter_clone(xform_filter_program));
}

static byte *
find_title(struct odes *obj UNUSED, uns raw UNUSED)
{
#ifdef MT_TITLE
  for (struct oattr *t = obj_find_attr(obj, 'M'); t; t=t->same)
    {
      byte *v = t->val;
      if (*v >= '0' && *v <= '9')
	v++;
      if (*v == 0x90 + MT_TITLE)
	if (raw)
	  return t->val;
	else
	  return v+1;
    }
#endif
  return NULL;
}

static void
get_image_attrs(struct scan_context *c)
{
  struct oattr *attrs = obj_find_attr(c->obj, 'G');
  if (attrs)
    {
      uns width, height, ncolors;
      byte colorspace[16];
      sscanf(attrs->val, "%d%d%s%d", &width, &height, colorspace, &ncolors);

      /* should be safe because of gatherer's limits */
      c->image_size = sqrtf(width * height);
      c->image_aspect_ratio = (width > height) ? (width << 10) / height : (height << 10) / width;

      if (!strcasecmp(colorspace, "GRAY"))
	c->image_colors = 0;
      else
	c->image_colors = ncolors;
    }
  else
    c->image_size = c->image_aspect_ratio = c->image_colors = F_UNDEF_INT;
}

static void
get_audio_attrs(struct scan_context *c)
{
  struct oattr *f = obj_find_attr(c->obj, 'f' + OBJ_ATTR_SON);
  if (f)
    {
      byte *ftype = obj_find_aval(f->son, 'f');
      ASSERT(ftype);
      if (ftype[0] == 'a')
        {
	  c->audio_length = obj_find_anum(f->son, 'l', F_UNDEF_INT);
	  c->audio_bitrate = obj_find_anum(f->son, 'b', F_UNDEF_INT);
	  c->audio_srate = obj_find_anum(f->son, 'r', F_UNDEF_INT);
	  c->audio_channels = obj_find_anum(f->son, 'c', 1);
	  return;
	}
    }
  c->audio_length = c->audio_bitrate = c->audio_srate = c->audio_channels = F_UNDEF_INT;
}

static int
scan_filter(struct scan_context *c)
{
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];

  c->site_name = NULL;
  c->site_level = 0;
  c->bonus = 0;
  c->card_bonus = 0;
  if (url_canon_split(c->url, buf1, buf2, &c->url_s))
    die("scan_filter: error parsing URL");
  c->queue_key = obj_find_x32(c->obj, 'k', F_UNDEF_INT);
  if (!obj_find_aval(c->obj, 'X') && !obj_find_aval(c->obj, 'M'))
    c->language = NULL;
  else
    c->language = an_lang_decide_language(c->obj);
  c->title = NULL;
  get_image_attrs(c);
  get_audio_attrs(c);
  c->area = 0;
  c->want_xform = 0;
  c->noindex = 0;

  struct filter_args *a = c->filter_args;
  if (!a)
    return 1;

  c->title = find_title(c->obj, 0);
  a->attr = c->obj;
  a->raw = c;
  a->pool = c->pool;
  return filter_intr_run(a);
}

static int
xform_filter(struct scan_context *c)
{
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
  struct filter_args *a = c->xform_args;
  a->attr = NULL;
  a->raw = c;
  a->pool = c->pool;
  if (url_canon_split(c->link = c->url_xform, buf1, buf2, &c->link_s) || !filter_intr_run(c->xform_args))
    return 0;
  if (c->url_xform != c->link)
    {
      if (!c->url_xform || url_auto_canonicalize(c->url_xform, c->link_buf))
	return 0;
      c->url_xform = c->link_buf;
    }
  return 1;
}

/* Analysers */

static void
analyse_init(void)
{
  analyser_init_hook(AN_HOOK_SCANNER);
}

static void
analyse_cleanup(void)
{
  analyser_log_stats(&scan_contexts->an_context);
  analyser_cleanup(&scan_contexts->an_context);
}

static void
scan_analyse_start(struct scan_context *c)
{
  analyser_init(&c->an_context, AN_HOOK_SCANNER, AN_NEED_TEXT | AN_NEED_METAS | AN_NEED_ALL_URLS | AN_NEED_THUMBNAIL, &scan_contexts->an_context);
  if (c->an_context.need_mask & AN_NEED_TEXT)
    c->fb_text = fbgrow_create(4096);
  if (c->an_context.need_mask & AN_NEED_METAS)
    c->fb_metas = fbgrow_create(4096);
  if (c->an_context.need_mask & AN_NEED_THUMBNAIL)
    c->fb_thumbnail = fbgrow_create(4096);
}

static void
scan_analyse(struct scan_context *c)
{
  struct an_iface ai = {
    .obj = c->obj,
    .url_block = c->obj,
    .all_urls = (struct odes *[]) { c->obj, NULL },
    .pool = c->pool
  };

  uns need = analyser_need(&c->an_context, &ai);
  if (need)
    {
      if (need & AN_NEED_TEXT)
	{
	  ai.text = c->fb_text;
	  fbgrow_reset(ai.text);
	  for (struct oattr *a = obj_find_attr(c->obj, 'X'); a; a=a->same)
	    {
	      bwrite(ai.text, a->val, str_len(a->val));		// bputs(), but with str_len()
	      if (a->same)
		bputc(ai.text, ' ');
	    }
	  fbgrow_rewind(ai.text);
	}
      if (need & AN_NEED_METAS)
	{
	  ai.metas = c->fb_metas;
	  fbgrow_reset(ai.metas);
	  for (struct oattr *a = obj_find_attr(c->obj, 'M'); a; a=a->same)
	    bwrite(ai.metas, a->val, str_len(a->val));
	  fbgrow_rewind(ai.metas);
	}
      if (need & AN_NEED_THUMBNAIL)
        {
	  ai.thumbnail = c->fb_thumbnail;
	  fbgrow_reset(ai.thumbnail);
	  for (struct oattr *a = obj_find_attr(c->obj, 'N'); a; a=a->same)
	    {
	      byte len = strlen(a->val);
	      byte buf[len + 1];
	      uns buf_len = base224_decode(buf, a->val, len);
	      bwrite(ai.thumbnail, buf, buf_len);
	    }
	  fbgrow_rewind(ai.thumbnail);
	}
      analyser_run_needed(&c->an_context, &ai);
    }
}

static void
scan_analyse_finish(struct scan_context *c)
{
  bclose(c->fb_text);
  bclose(c->fb_metas);
  bclose(c->fb_thumbnail);
  if (c != scan_contexts)
    {
      analyser_merge_stats(&c->an_context);
      analyser_cleanup(&c->an_context);
    }
}

/* Parameters */

static void
prepare_parameters(void)
{
  parameters.ref_time = time(NULL);
  parameters.database_version = parameters.ref_time;
  parameters.num_slices = num_slices;
  parameters.srand = time(NULL);
  srand(parameters.srand);
}

/* Scanners */

static void
gen_fingerprint(struct scan_context *c)
{
  if (!c->fingerprints)
    return;

  struct card_print fp;

  url_fingerprint(c->url, &fp.fp);
  fp.cardid = c->id;
  bwrite(c->fingerprints, &fp, sizeof(fp));
#ifdef CONFIG_MERGING_HASHES
  if (c->attr)
    memcpy(c->attr->merging_hash, &fp.fp, SHERLOCK_MERGING_HASH_SIZE);
#endif
}

static int
frameset_to_redir_p(struct odes *o)
{
  /* We have to avoid pages which already are redirects, e.g., translated META refreshes. */
  return frameset_to_redir &&
    obj_find_attr(o, 'F') &&
    !obj_find_attr(o, 'Y');
}

static void
gen_labels_by_id(struct scan_context *c)
{
  struct fastbuf *b = c->labels_by_id;
  if (!b)
    return;

  struct oattr *a, *v;
  uns overrides = 0;

  bputl(b, c->id);
  bputc(b, LABEL_TYPE_URL | LABEL_FLAG_MERGED_ONLY);
  bput_attr_str(b, 'U', c->url);
  for (a=c->obj->attrs; v=a; a=a->next)
    {
      if (attr_set_match(&label_attr_set, a))
	bput_oattr(b, a);
      if (attr_set_match(&override_label_attr_set, a))
	overrides |= 1;
      if (attr_set_match(&override_body_attr_set, a))
	overrides |= 2;
    }
  bput_attr_separator(b);

  for (uns i=0; i<2; i++)
    if (overrides & (1 << i))
      {
	bputl(b, c->id);
	bputc(b, (i ? LABEL_TYPE_BODY : LABEL_TYPE_URL) | LABEL_FLAG_OVERRIDE);
	for (a=c->obj->attrs; v=a; a=a->next)
	  if (attr_set_match((i ? &override_body_attr_set : &override_label_attr_set), a))
	    bput_oattr(b, a);
	bput_attr_separator(b);
      }

  if (frameset_to_redir_p(c->obj))
    {
      bputl(b, c->id);
      bputc(b, LABEL_TYPE_URL);
      byte *title = find_title(c->obj, 1);
      if (title)
	bput_attr_str(b, 'M', title);
      bput_attr_str(b, '.', "frameset");
      bput_attr_separator(b);
    }

#ifdef CONFIG_SITES
  byte site[URL_KEY_BUF_SIZE];
  strcpy(site, c->site_name ?: (byte*)"");
  site_name_compute(c->url, site, c->site_level, c->note->site_hash);
  bputl(b, c->id);
  bputc(b, LABEL_TYPE_BODY);
  bput_attr_str(b, 't', site);
  bput_attr_separator(b);
#endif

  fbatomic_commit(b);
}

static void
gen_checksums(struct scan_context *c)
{
  if (c->noindex)
    {
      c->attr->flags |= CARD_FLAG_EMPTY;
      return;
    }

  struct csum csum = { .cardid = c->id };
  u16 buf[4096];
  uns cnt = 0;
  struct oattr *a;
  md5_context ct;
  uns accents = 0;
  uns chars = 0;
  uns lastcc = ' ';

  md5_init(&ct);
  for (byte *aname="XM"; *aname; aname++)
    for (a=obj_find_attr(c->obj, *aname); a; a=a->same)
      {
	byte *z = a->val;
	uns x, cc, u;
	do
	  {
	    GET_TAGGED_CHAR(z, x);
	    if (x >= 0x80000000 || !x)
	      {
		if (x >= 0x80010000)
		  continue;
		cc = ' ';
	      }
	    else
	      cc = x;
	    if (cc != lastcc || cc != ' ')
	      {
		lastcc = cc;
		u = Uunaccent(cc);
		if (cc != u)
		  accents++;
		if (Ualnum(cc))
		  chars++;
		buf[cnt++] = u;
		if (cnt >= ARRAY_SIZE(buf))
		  {
		    if (c->checksums)
		      md5_update(&ct, (byte *) buf, cnt * sizeof(buf[0]));
		    cnt = 0;
		  }
	      }
	  }
	while (x);
      }

  struct card_note *note = c->note;
  note->useful_size = chars;

  if (a = obj_find_attr(c->obj, 'N'))
    {
      note->flags |= CARD_NOTE_IMAGE;
      for (; a; a=a->same)
	{
	  byte *z = a->val;
	  uns len = str_len(z);
	  md5_update(&ct, z, len);
	  chars += len;
	}
    }

  for (a = obj_find_attr(c->obj, 'A'); a; a=a->same)
    {
      byte *z = a->val;
      uns len = str_len(z);
      md5_update(&ct, z, len + 1);
      /* chars += len; */
    }

  if (a = obj_find_attr(c->obj, 'f' + OBJ_ATTR_SON))
    {
      byte *t = obj_find_aval(a->son, 'f');
      ASSERT(t);
      if (t[0] == 'a')
        {
	  note->flags |= CARD_NOTE_AUDIO;
	  byte *h = obj_find_aval(a->son, 'h');
	  if (!h)
	    return;
	  md5_update(&ct, h, strlen(h));
	  u32 size = obj_find_anum(a->son, 's', 1);
	  md5_update(&ct, (void *)&size, 4);
	  chars += size;
	}
    }

  if (accents > chars/128)
    c->attr->flags |= CARD_FLAG_ACCENTED;
  if (!chars && reject_empty)
    c->attr->flags |= CARD_FLAG_EMPTY;
  if (chars <= min_summed_size)
    return;
  if (!c->checksums)
    return;

  md5_update(&ct, (byte *) buf, cnt * sizeof(buf[0]));

#ifdef CONFIG_AREAS
  /* Avoid mixing documents from different areas */
  md5_update(&ct, (byte *) &note->area, sizeof(note->area));
#endif

  memcpy(csum.md5, md5_final(&ct), MD5_SIZE);
  bwrite(c->checksums, &csum, sizeof(csum));
}

static void
gen_links(struct scan_context *c)
{
  if (!c->links)
    return;

  uns want_xform = c->want_xform && c->xform_args;
  for (struct oattr *a=c->obj->attrs; a; a=a->next)
    if (attr_set_match(&link_attr_set, a))
      for (struct oattr *v=a; v; v=v->same)
	{
	  c->note->flags |= CARD_NOTE_HAS_LINKS;
	  if (a->attr == 'Y')
	    c->note->flags |= CARD_NOTE_REDIRECT;

	  byte *p;
	  if (p = strchr(v->val, ' '))
	    {
	      byte *d = p+1;
	      while (*d && *d != ' ')
		d++;
	      if (*d && d[1] == '1')	/* nofollow links */
		continue;
	      *p = 0;
	    }
	  c->url_xform = v->val;
	  if (!want_xform || xform_filter(c))
	    {
	      struct link link;
	      url_fingerprint(c->url_xform, &link.fp);
	      link.src = (a->attr == 'Y') ? ETYPE_REDIRECT :
		  (a->attr == 'F') ? ETYPE_FRAME :
		  (a->attr == 'I') ? ETYPE_IMAGE :
		  ETYPE_NORMAL;
	      link.src |= c->id;
	      bwrite(c->links, &link, sizeof(link));
	    }
	  if (p)
	    *p = ' ';
	}
}

#if defined(CONFIG_LANG) && defined(CONFIG_FILETYPE)
static void
gen_lang(struct scan_context *c)
{
  if (!FILETYPE_IS_TEXT(CA_GET_FILE_TYPE(c->attr)))
    return;
  byte *lang = c->language;
  int id = lang ? lang_primary_language(lang) : -1;
  if (id >= 0)
    c->attr->type_flags |= id;
}
#else
static void gen_lang(struct scan_context *c UNUSED) { }
#endif

static inline byte
initial_weight(struct scan_context *c)
{
  int initial_weight;
  byte *s = obj_find_aval(c->obj, 'w');
  if (s)
    initial_weight = atoi(s);
  else
    {
#ifdef CONFIG_INDEXER_IMAGES_ONLY
      if ((s = obj_find_aval(c->obj, 'W')) && *s == 'g')
        initial_weight = atoi(s + 1);
      else
#endif
        initial_weight = default_weight;
    }
  initial_weight += c->bonus;
  return CLAMP(initial_weight, 0, 255);
}

static inline uns
xtoi(uns c)
{
  if (c >= '0' && c <= '9')
    return c - '0';
  else if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  else if (c >= 'A' && c <= 'F')
    return c - 'A' + 10;
  else
    {
      ASSERT(0);
      return 0;
    }
}

static void
gen_note_basics(struct scan_context *c)
{
  struct card_note *cn = c->note;

#ifdef CONFIG_AREAS
  cn->area = c->area;
#endif

  byte *footprint = obj_find_aval(c->obj, 'O');
  if (footprint)
    {
      u32 x[4];
      sscanf(footprint, "%8x%8x%8x%8x", x, x+1, x+2, x+3);
      memcpy(cn->footprint, x, 16);
    }
}

static void
gen_attrs(struct scan_context *c)
{
  struct card_attr *ca = c->attr;
  struct card_note *cn = c->note;

  ca->card = c->get_buck.index;
#ifdef CONFIG_INDEXER_STORE_OID
  cn->oid = c->get_buck.oid;
#endif
#ifdef CONFIG_SITES
  ca->site_id = 0;
#endif
#ifdef CONFIG_AREAS
  ca->area = c->area;
#endif
  ca->weight = initial_weight(c);
  cn->weight_scanner = ca->weight;
  cn->card_bonus = CLAMP(c->card_bonus, -32767, 32767);

  if (frameset_to_redir_p(c->obj))
    ca->flags |= CARD_FLAG_FRAMESET | CARD_FLAG_EMPTY;

#ifdef CONFIG_LASTMOD
  byte *lm;
  int age = -1;
  if ((lm = obj_find_aval(c->obj, 'L')) || (lm = obj_find_aval(c->obj, 'D')))
    age = convert_age(atol(lm), parameters.ref_time);
  ca->age = MAX(0, age);
#endif

  custom_create_attrs(c->obj, c->attr);
  gen_lang(c);			/* Needs type_flags set by custom_create_attrs */
}

static void
gen_signatures(struct scan_context *c)
{
  if (!c->signatures)
    return;

  struct oattr *oa;
  u32 sig[1+matcher_signatures];
  uns words;

  oa = obj_find_attr(c->obj, 'X');
  if (!oa)
    return;
  words = matcher_compute_minima(sig+1, c->matcher_context, oa);
  if (words < matcher_min_words)
    return;
  sig[0] = c->id;
  bwrite(c->signatures, sig, sizeof(sig));
}

#ifndef MT_EXT

static void gen_ref_texts(struct scan_context *c UNUSED) { }

#else

static int
parse_reference(byte *aval, byte *url)
{
  byte *x = aval;
  byte *y = url;
  while (*x && *x != ' ')
    *y++ = *x++;
  *y = 0;
  return *x++ ? atol(x) : -1;
}

static void
gen_ref_texts(struct scan_context *ctx)
{
#define MAX_REFS 1024
#define MAX_REF_DEPTH 3

  struct fastbuf *reftexts = ctx->reftexts;
  if (!reftexts || ctx->noindex >= 2)
    return;

  /* Map of references */
  byte ref_word_types[MAX_REFS];
  struct fingerprint fp[MAX_REFS];
  uns last_ref = 0;

  /* Currently open references */
  uns refstack[MAX_REF_DEPTH];
  int refsp = -1;
  byte rtext[MAX_REF_DEPTH][ref_max_length + 5];
  uns over[MAX_REF_DEPTH], nalpha[MAX_REF_DEPTH];
  byte *rthis[MAX_REF_DEPTH], *rlimit[MAX_REF_DEPTH];

  byte *b, *c, *l;
  struct oattr *aa, *a;
  byte *wstart, *wend;
  uns nalphas;

  /* Find all references we're interested in */
  for (aa=ctx->obj->attrs; aa; aa=aa->next)
    if (attr_set_match(&ref_link_attr_set, aa))
      for (a=aa; a; a=a->same)
	{
	  byte url[MAX_URL_SIZE];
	  int rid = parse_reference(a->val, url);
	  if (rid < 0 || rid >= MAX_REFS)
	    continue;
	  //DBG("Ref <%s> id %d", url, rid);
	  while (last_ref <= (uns) rid)
	    ref_word_types[last_ref++] = 0;
	  ref_word_types[rid] = MT_EXT;
	  url_fingerprint(url, &fp[rid]);
	}

  /* And scan the text for bracketed parts */
  for (a=obj_find_attr(ctx->obj, 'X'); a; a=a->same)
    {
      byte *z = a->val;
      uns x;
      wstart = wend = z;
      nalphas = 0;
      do
	{
	  GET_TAGGED_CHAR(z, x);
	  if (x < 0x80000000 && x != ' ')
	    {
	      if (Ualnum(x))
		nalphas++;
	      wend = z;
	      continue;
	    }
	  if (refsp >= 0 && wstart < wend && !over[refsp])
	    {
	      uns len = wend - wstart;
	      if (rthis[refsp] + len > rlimit[refsp])
		over[refsp] = 1;
	      else
		{
		  memcpy(rthis[refsp], wstart, len);
		  rthis[refsp] += len;
		  nalpha[refsp] += nalphas;
		}
	    }
	  wstart = wend = z;
	  nalphas = 0;
	  if (x < 0x80010000)		/* Word break */
	    {
	      if (refsp >= 0 && rthis[refsp][-1] != ' ')
		*rthis[refsp]++ = ' ';
	      /* We know it will fit in the buffer */
	    }
	  else if (x < 0x80020000)	/* Open */
	    {
	      refsp++;
	      ASSERT(refsp < MAX_REF_DEPTH);
	      refstack[refsp] = x & 0xffff;
	      rtext[refsp][0] = 0;
	      rthis[refsp] = rtext[refsp] + 1;
	      rlimit[refsp] = rthis[refsp] + ref_max_length;
	      over[refsp] = 0;
	      nalpha[refsp] = 0;
	    }
	  else				/* Close */
	    {
	      uns rid;
	      ASSERT(refsp >= 0);
	      rid = refstack[refsp];
	      *rthis[refsp] = 0;
	      if (rid < last_ref && ref_word_types[rid])
		{
		  b = rtext[refsp] + 1;
		  if (*b == ' ')
		    b++;
		  c = rthis[refsp];
		  if (c > b && c[-1] == ' ')
		    c--;
		  if (c > b && nalpha[refsp] >= ref_min_length)
		    {
		      bputl(reftexts, ctx->id);
		      bwrite(reftexts, &fp[rid], sizeof(struct fingerprint));
		      bputw(reftexts, c-b+1);
		      bputc(reftexts, 0x90 + ref_word_types[rid]);
		      bwrite(reftexts, b, c-b);
		    }
		}
	      if (refsp > 0 && !over[refsp-1])
		{
		  byte *cut;
		  b = rtext[refsp] + 1;
		  c = rthis[refsp-1];
		  l = rlimit[refsp-1];
		  if (b[0] == ' ')
		    {
		      if (c[-1] != ' ')
			*c++ = ' ';
		      b++;
		    }
		  cut = c;
		  while (*b && c <= l)
		    {
		      if (*b == ' ')
			cut = c;
		      *c++ = *b++;
		    }
		  if (c > l)
		    {
		      c = cut;
		      over[refsp-1] = 1;
		    }
		  over[refsp-1] |= over[refsp];
		  nalpha[refsp-1] += nalpha[refsp];
		  rthis[refsp-1] = c;
		}
	      refsp--;
	    }
	}
      while (x);
    }
  if (refsp >= 0)
    log(L_ERROR, "Unbalanced reference brackets for card %08x", ctx->id);
  fbatomic_commit(reftexts);
}

#endif

#ifdef CONFIG_IMAGES_DUP

static void
images_start(struct scan_context *c)
{
  c->image_thumbnails = index_maybe_atomic_open(fn_image_thumbnails, scan_contexts[0].image_thumbnails, -(indexer_fb_size/8));
}

static void
images_finish(struct scan_context *c)
{
  bclose(c->image_thumbnails);
}

static void
gen_images(struct scan_context *c)
{
  struct fastbuf *fb = c->image_thumbnails;
  if (!fb || (c->attr->flags & CARD_FLAG_EMPTY))
    return;
  struct image_obj_info ioi;
  if (!get_image_obj_info(&ioi, c->obj))
    return;
  struct image_signature sig;
  if (!get_image_obj_signature(&sig, c->obj))
    {
      log(L_WARN, "Image `%s' has no signature", obj_find_aval(c->obj, 'U'));
      return;
    }
  if (!get_image_obj_thumb(&ioi, c->obj, c->pool))
    {
      log(L_WARN, "Image `%s' has no thumbnail", obj_find_aval(c->obj, 'U'));
      return;
    }
  struct image_thumb hdr;
  hdr.id = c->id;
  hdr.cols = ioi.cols;
  hdr.rows = ioi.rows;
  hdr.thumb_cols = ioi.thumb_cols;
  hdr.thumb_rows = ioi.thumb_rows;
  hdr.thumb_size = ioi.thumb_size;
  hdr.thumb_format = ioi.thumb_format;
  hdr.vector = sig.vec;
  bwrite(fb, &hdr, sizeof(hdr));
  bwrite(fb, ioi.thumb_data, ioi.thumb_size);
  fbatomic_commit(fb);
}

#else
static inline void images_start(struct scan_context *c UNUSED) {}
static inline void images_finish(struct scan_context *c UNUSED) {}
static inline void gen_images(struct scan_context *c UNUSED) {}
#endif

/* Contexts */

static void
scan_start(struct scan_context *c)
{
  struct scan_context *mainc = &scan_contexts[0];

  TDBG(c, "Start");
  c->pool = mp_new(16384);

  c->fingerprints = index_maybe_atomic_open(fn_fingerprints, mainc->fingerprints, sizeof(struct card_print));
  c->labels_by_id = index_maybe_atomic_open(fn_labels_by_id, mainc->labels_by_id, -4096);
  c->checksums = index_maybe_atomic_open(fn_checksums, mainc->checksums, sizeof(struct csum));
  c->links = index_maybe_atomic_open(fn_links, mainc->links, sizeof(struct link));
  c->reftexts = index_maybe_atomic_open(fn_ref_texts, mainc->reftexts, -(indexer_fb_size/8));

  if (matcher_signatures && index_name_defined(fn_signatures))
    {
      c->signatures = index_atomic_open(fn_signatures, mainc->signatures, (1+matcher_signatures)*4);
      c->matcher_context = matcher_new(mainc->matcher_context);
    }

  scan_filter_start(c);
  scan_analyse_start(c);
  read_start(c);
  images_start(c);
}

static void
scan_stop(struct scan_context *c)
{
  TDBG(c, "Stop");
  alloc_stop(c);
}

static void
scan_finish(struct scan_context *c)
{
  struct scan_context *mainc = &scan_contexts[0];

  log(L_DEBUG, "Thread %d: %d in, %d ok, %d skel, %d err, %d bots",
      c->context_id, c->count_in, c->count_ok, c->count_skel, c->count_err, c->count_bots);
  if (c->context_id)
    {
      mainc->count_in += c->count_in;
      mainc->count_ok += c->count_ok;
      mainc->count_skel += c->count_skel;
      mainc->count_err += c->count_err;
      mainc->count_bots += c->count_bots;
    }

  bclose(c->signatures);
  bclose(c->reftexts);
  bclose(c->links);
  bclose(c->checksums);
  bclose(c->labels_by_id);
  bclose(c->fingerprints);
  images_finish(c);

  read_finish(c);
  scan_analyse_finish(c);
  mp_delete(c->pool);
}

static void
scan_loop(struct scan_context *c)
{
  TDBG(c, "Loop");
  while (read_next(c))
    {
      struct odes *o = c->obj;
      c->url = obj_find_aval(o, 'U');
      if (!c->url)
	die("Object %x has no URL, probably broken bucket file", c->get_buck.oid);
      byte *ct = obj_find_aval(o, 'T');
      if (ct && !strcmp(ct, "x-sherlock/robots"))	/* Skip robots.txt buckets */
	{
	  c->count_bots++;
	  continue;
	}
      c->attr = NULL;					/* For sure */
      c->note = NULL;

      scan_analyse(c);
      if (scan_filter(c))
	{
	  int ok = 1;
	  if (c->get_buck.type > BUCKET_TYPE_PLAIN)
	    {
	      byte *stat = obj_find_aval(o, '!');
	      if (stat && !strncmp(stat, "2304", 4))	/* We accept documents forbidden by robots.txt */
		;
	      else if (!stat || !obj_find_attr(o, 'D'))	/* Not yet gathered */
		{
		  c->count_skel++;
		  ok = 0;
		}
	      else if (stat[0] != '0')			/* Gathered with error */
		{
		  c->count_err++;
		  ok = 0;
		}
	    }
	  if (!ok)
	    {
	      if (skel_buf)
		{
		  alloc_id(c, 1);
		  gen_fingerprint(c);
		  gen_note_basics(c);
		  c->note->weight_scanner = initial_weight(c);
		}
	    }
	  else
	    {
	      c->count_ok++;
	      alloc_id(c, 0);
	      gen_note_basics(c);
	      gen_fingerprint(c);
	      gen_labels_by_id(c);
	      gen_checksums(c);
	      gen_links(c);
	      gen_attrs(c);
	      gen_signatures(c);
	      gen_ref_texts(c);
	      gen_images(c);
	    }
	}
    }
}

/*
 * Startup locking
 */

static sem_t *ready_sem;			/* Tells master that threads are ready to run */
static sem_t *go_sem;				/* The master replies that we can run */

static void *
scan_thread(void *cc)
{
  struct scan_context *c = cc;

  scan_start(c);
  sem_post(ready_sem);
  sem_wait(go_sem);
  scan_loop(c);
  scan_stop(c);
  return NULL;
}

static char *short_opts = CF_SHORT_OPTS;
static char *help = "\
Usage: scanner [<options>]\n\
\n\
Options:\n"
CF_USAGE
;

static void NONRET
usage(void)
{
  fputs(help, stderr);
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  setproctitle_init(argc, argv);
  cf_declare_section("Scanner", &scanner_config, 0);

  int opt;
  while ((opt = cf_getopt(argc, argv, short_opts, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
	default:
	  usage();
      }
  if (optind < argc)
    usage();

  DBG("Init");
  matcher_init();
  prepare_parameters();
  put_attr_set_type(BUCKET_TYPE_V33);
  scan_filter_init();
  url_key_init();
  alloc_init();
  read_init();
  analyse_init();

  scan_contexts = xmalloc_zero(indexer_threads * sizeof(struct scan_context));
  scan_start(&scan_contexts[0]);
  if (indexer_threads > 1)
    {
      pthread_attr_t attr;
      if (pthread_attr_init(&attr) < 0 ||
	  pthread_attr_setstacksize(&attr, indexer_thread_stack_size ? : ucwlib_thread_stack_size) < 0)
	ASSERT(0);
      ready_sem = sem_alloc();
      go_sem = sem_alloc();
      for (uns i=1; i<indexer_threads; i++)
	{
	  struct scan_context *c = &scan_contexts[i];
	  c->context_id = i;
	  if (pthread_create(&c->thread, &attr, scan_thread, c) < 0)
	    die("Unable to create thread: %m");
	  sem_wait(ready_sem);
	}
    }

  log(L_INFO, "Scanning objects");
  for (uns i=1; i<indexer_threads; i++)
    sem_post(go_sem);

  scan_loop(&scan_contexts[0]);
  scan_stop(&scan_contexts[0]);

  for (uns i=1; i<indexer_threads; i++)
    if (pthread_join(scan_contexts[i].thread, NULL) < 0)
      die("Cannot join thread: %m");

  for (uns i=0; i<indexer_threads; i++)
    scan_finish(&scan_contexts[i]);

  struct scan_context *mainc = &scan_contexts[0];
  parameters.objects_in = mainc->count_ok + mainc->count_err + mainc->count_bots;
  params_save(&parameters);

  DBG("Cleanup");
  analyse_cleanup();
  log(L_INFO, "Scanned %d objects (%d ok, %d err, %d robots, %d skeletons)",
      mainc->count_in, mainc->count_ok, mainc->count_err, mainc->count_bots, mainc->count_skel);
  log(L_INFO, "Created %d cards and %d skeleton notes", id_out, id_skel - FIRST_ID_SKEL);

  if (indexer_threads > 1)
    {
      sem_free(ready_sem);
      sem_free(go_sem);
    }
  alloc_cleanup();
  return 0;
}
