/*
 *	Sherlock Shepherd Daemon -- Database Cleanup
 *
 *	(c) 2004--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2004 Robert Spalek <robert@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "sherlock/object.h"
#include "filter/filter.h"
#include "gather/gather.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <stdlib.h>

struct known_bucket {
  u32 oid;
  byte flags;
} PACKED;

enum known_bucket_flags {
  KFB_THICK = 1,
  KFB_ROBOTS = 2
};

static uns progress_cnt;
static oid_t progress_last_oid;
static struct url_db *url_db;

static struct fastbuf *
find_known_buckets(struct fastbuf *idx)
{
  struct fastbuf *ab = temp_state_file();
  struct url_state s;
  struct known_bucket b;

  while (breadb(idx, &s, sizeof(s)))
    switch (ustate_type(&s))
      {
      case UTYPE_SLEEPING:
      case UTYPE_NEW:
      case UTYPE_OK:
      case UTYPE_ERROR:
	b.oid = s.oid;
	b.flags = 0;
	if (ustate_type(&s) == UTYPE_OK || ustate_type(&s) == UTYPE_ERROR)
	  b.flags |= KFB_THICK;
	if (s.flags & USF_ROBOTS)
	  b.flags |= KFB_ROBOTS;
	bwrite(ab, &b, sizeof(b));
	break;
      case UTYPE_SKEY:
      case UTYPE_ZOMBIE:
	break;
      default:
	ASSERT(0);
      }

  bclose(idx);
  brewind(ab);
  return ab;
}

#define SORT_PREFIX(x) buck_##x
#define SORT_KEY_REGULAR struct known_bucket
#define SORT_UNIQUE
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_INT(x) (x).oid
#include "ucw/sorter/sorter.h"

struct cfilter_data {
  byte *url;				/* Set by the caller */
  struct url url_s;			/* Broken-down URL */
  uns section;				/* Section number */
  area_t area;				/* Area number */
  byte *content_type;			/* Guessed content-type */
  byte *content_encoding;		/* Guessed content-encoding */
  int stable_time;			/* Time between last significant change and last check */
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
};

static struct filter_binding cfilter_bindings[] = {
  /* URL and its parts */
  { "url",		OFFSETOF(struct cfilter_data, url) },
  { "protocol",		OFFSETOF(struct cfilter_data, url_s.protocol) },
  { "host",		OFFSETOF(struct cfilter_data, url_s.host) },
  { "port",		OFFSETOF(struct cfilter_data, url_s.port) },
  { "path",		OFFSETOF(struct cfilter_data, url_s.rest) },
  { "username",		OFFSETOF(struct cfilter_data, url_s.user) },
  { "password",		OFFSETOF(struct cfilter_data, url_s.pass) },
  /* Gatherer attributes */
  { "content_type",	OFFSETOF(struct cfilter_data, content_type) },
  { "content_encoding",	OFFSETOF(struct cfilter_data, content_encoding) },
  /* Section */
  { "section",		OFFSETOF(struct cfilter_data, section) },
#ifdef CONFIG_AREAS
  /* Area */
  { "area",		OFFSETOF(struct cfilter_data, area) },
#endif
  /* Stable time */
  { "stable_time",	OFFSETOF(struct cfilter_data, stable_time) },
  { NULL,		0 }
};

static struct filter *cfilter;
static struct filter_args *cfilter_args;
static struct mempool *cfilter_pool;

static void
init_filter(void)
{
  cfilter = filter_load(shepherd_filter_name, filter_builtin_vars, cfilter_bindings, NULL);
  cfilter_args = filter_intr_new(cfilter);
  cfilter_pool = mp_new(4096);
}

static int
filter_url(struct cfilter_data *d)
{
  uns err;

  mp_flush(cfilter_pool);
  if (err = url_canon_split(d->url, d->buf1, d->buf2, &d->url_s))
    return 0;
  d->section = 0;
  d->area = AREA_NONE;
  d->content_type = d->content_encoding = NULL;
  d->stable_time = 0;

  if (cfilter)
    {
      struct filter_args *a = cfilter_args;
      a->pool = cfilter_pool;
      a->raw = d;
      a->attr = NULL;
      guess_content_by_name(d->url_s.rest, &d->content_type, &d->content_encoding);
      if (!filter_intr_run(a))
	return 0;
    }
  return 1;
}

static struct fastbuf *buckets, *jour;
static struct known_bucket next_known_bucket;
static int keep_buckets, no_filters;
static uns stat_already_deleted, stat_unref, stat_invalid, stat_ghostified, stat_thick;
static uns stat_thin, stat_lost, stat_filtered_out, stat_chg_section;
static u64 size_thick, size_thin;
static struct mempool *shake_pool;
static struct buck2obj_buf *buck_buf;

static byte *
scan_header(struct obuck_header *bh, byte *data, struct url_state *s, byte *urlbuf)
{
  struct fastbuf b;
  uns body;
  mp_flush(shake_pool);
  fbbuf_init_read(&b, data, bh->length, 1);
  struct odes *o = obj_read_bucket(buck_buf, shake_pool, bh->type, bh->length, &b, &body, 1);
  if (unlikely(!o))
    die("Bucket %08x: malformed", s->oid);

  byte *U, *O;
  U = obj_find_aval(o, 'U');
  O = obj_find_aval(o, 'O');
  if (unlikely(!U || !O))
    die("Bucket %08x is malformed, required attributes missing or invalid", s->oid);
  if (unlikely(strlen(U) >= MAX_URL_SIZE))
    die("Bucket %08x: URL too long", s->oid);
  strcpy(urlbuf, U);
  if (unlikely(strlen(O) != 32
	       || sscanf(O, "%8x%8x%8x%8x", &s->fp.site.x[0], &s->fp.site.x[1], &s->fp.rest.x[0], &s->fp.rest.x[1]) != 4))
    die("Bucket %08x: Invalid format of object footprint", s->oid);

  return data + body;
}

static int
shake_kibitz(struct obuck_header *bh, oid_t new_id, byte *data)
{
  int rc = 1;

  if (!(progress_cnt++ % 10000))
    setproctitle("shep-cleanup: %d buckets (%d%%)", progress_cnt,
		 progress_last_oid ? (int)((float)bh->oid/(float)progress_last_oid*100) : 0);

  if (new_id == OBUCK_OID_DELETED)
    {
      stat_already_deleted++;
      return 0;
    }
  if (bh->type < BUCKET_TYPE_V30 || bh->type > BUCKET_TYPE_V33_LIZARD)
    {
      log(L_ERROR, "Bucket %08x has an unknown type %08x, deleting it", bh->oid, bh->type);
      stat_invalid++;
      return 0;
    }

  while (next_known_bucket.oid < bh->oid)
    {
      if (!breadb(buckets, &next_known_bucket, sizeof(next_known_bucket)))
	next_known_bucket.oid = ~0U;
    }
  if (next_known_bucket.oid > bh->oid)
    {
      DBG("B %08x unreferenced", bh->oid);
      stat_unref++;
      return keep_buckets;
    }

  struct url_state s;
  byte url[MAX_URL_SIZE];
  bzero(&s, sizeof(s));
  s.oid = new_id;
  byte *body = scan_header(bh, data, &s, url);
  DBG("B %08x <%s> %08x%08x:%08x%08x", s.oid, url, FP_QUAD(s.fp));

  if (!no_filters)
    {
      struct cfilter_data cfd;
      cfd.url = url;
      if (!filter_url(&cfd) && !(next_known_bucket.flags & KFB_ROBOTS))
	{
	  DBG("\tFiltered out");
	  stat_filtered_out++;
	  if (!keep_buckets)
	    {
	      rc = 0;
	      s.type = UTYPE_ERROR;
	      goto done;
	    }
	}
      s.section = cfd.section;
      s.area = cfd.area;
      s.stable_time = CLAMP(cfd.stable_time, 0, 255);
    }

  if (body && !(next_known_bucket.flags & KFB_THICK))
    {
      DBG("\tGhostifying");
      stat_ghostified++;
      if (!keep_buckets)
	{
	  bh->length = body - data;
	  rc = 2;
	}
    }

  uns bsize = obuck_bucket_size(bh->length);
  if (next_known_bucket.flags & KFB_THICK)
    {
      size_thick += bsize;
      stat_thick++;
      s.type = UTYPE_OK;
    }
  else
    {
      size_thin += bsize;
      stat_thin++;
      s.type = UTYPE_NEW;
    }

 done:
  bwrite(jour, &s, sizeof(s));
  if (url_db && rc)
    url_db_write(url_db, new_id, url);
  return rc;
}

#define SORT_PREFIX(x) idx_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_UNIQUE
#define SORT_BY_FP
#include "gather/shepherd/index-sort.h"

static void
merge_updates(struct fastbuf *new, struct fastbuf *old, struct fastbuf *jour)
{
  struct url_state os, js;
  int have_js;

  have_js = breadb(jour, &js, sizeof(js));
  while (breadb(old, &os, sizeof(os)))
    {
      if (ustate_type(&os) != UTYPE_SKEY && ustate_type(&os) != UTYPE_ZOMBIE)
	{
	  int cmp;
	  for(;;)
	    {
	      if (!have_js)
		{
		  cmp = 1;
		  break;
		}
	      cmp = fp_cmp(&js.fp, &os.fp);
	      if (cmp >= 0)
		break;
	      have_js = breadb(jour, &js, sizeof(js));
	    }
	  if (cmp > 0)
	    {
	      log(L_ERROR, "Object %08x%08x:%08x%08x lost", FP_QUAD(os.fp));
	      stat_lost++;
	      continue;
	    }
	  else if (ustate_type(&js) == UTYPE_ERROR)
	    {
	      /* Filtered out */
	      continue;
	    }
	  else
	    {
	      os.oid = js.oid;
	      if (!no_filters)
		{
		  if (os.section != js.section)
		    stat_chg_section++;
		  os.section = js.section;
#ifdef CONFIG_AREAS
		  if (os.area != js.area)
		    stat_chg_section++;
		  os.area = js.area;
#endif
		  if (js.stable_time)
		    os.stable_time = js.stable_time;
		}
	    }
	}
      bwrite(new, &os, sizeof(os));
    }
  bclose(old);
  bclose(jour);
}

static struct option longopts[] = {
  CF_LONG_OPTS
  { "keep",		0, 0, 'k' },
  { "no-filters",	0, 0, 'f' },
  { NULL,		0, 0, 0 }
};

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-cleanup [<options>] <state>\n\
\n\
Options:\n\
" CF_USAGE "\
--keep\t\t\tKeep buckets as they are (don't delete unused ones nor convert thin ones)\n\
--no-filters\t\tAvoid filtering\n\
");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  setproctitle_init(argc, argv);

  int opt;
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS, longopts, NULL)) > 0)
    switch (opt)
      {
      case 'k':
	keep_buckets = 1;
	break;
      case 'f':
	no_filters = 1;
	break;
      default:
	usage();
      }
  if (optind != argc-1)
    usage();

  byte *state = cf_strdup(argv[optind]);
  setproctitle("shep-cleanup: preprocessing");

  log(L_INFO, "Loading site list");
  site_hash_init(NULL);
  site_hash_load(state, SITE_HASH_NO_URLS);

  log(L_INFO, "Looking for referenced buckets");
  buckets = find_known_buckets(read_state_file(state, "index"));
  buckets = buck_sort(buckets, NULL, (u32)~0U);

  if (url_db = url_db_open(O_CREAT | O_TRUNC | O_WRONLY, 1))
    log(L_INFO, "Creating new URL database");

  log(L_INFO, "Shaking down bucket file");
  jour = temp_state_file();
  if (!no_filters)
    init_filter();
  if (!breadb(buckets, &next_known_bucket, sizeof(next_known_bucket)))
    next_known_bucket.oid = ~0U;
  bucket_open(1);
  shake_pool = mp_new(1<<14);
  buck_buf = buck2obj_alloc();
  progress_last_oid = obuck_predict_last_oid(&bucket_file);
  obuck_shakedown(&bucket_file, shake_kibitz);
  log(L_INFO, "Bucket file size: %d MB", (uns)(obuck_get_pos(obuck_predict_last_oid(&bucket_file)) / 1048576));
  buck2obj_free(buck_buf);
  mp_delete(shake_pool);
  bucket_close();
  bclose(buckets);
  setproctitle("shep-cleanup: postprocessing");

  if (url_db)
    url_db_close(url_db);

  log(L_INFO, "Sorting index");
  struct fastbuf *idx = read_state_file(state, "index");
  idx = idx_sort(idx, NULL);

  log(L_INFO, "Sorting index updates");
  brewind(jour);
  jour = idx_sort(jour, NULL);

  log(L_INFO, "Merging updates");
  struct fastbuf *newidx = temp_state_file();
  merge_updates(newidx, idx, jour);
  put_state_file(state, "index", newidx, STATE_FLAGS_ALL);
  state_flags_set(state, STATE_FLAG_SORTED);

  log(L_INFO, "Buckets: %d thick, %d thin, %d lost", stat_thick, stat_thin, stat_lost);
  log(L_INFO, "Average sizes thick, thin: %d, %d (predicted %d, %d)",
      stat_thick ? (uns)(size_thick / stat_thick) : 0,
      stat_thin ? (uns)(size_thin / stat_thin) : 0,
      avg_bucket_size,
      avg_url_size);
  log(L_INFO, "Actions: %d already deleted, %d unreferenced, %d invalid",
      stat_already_deleted, stat_unref, stat_invalid);
  log(L_INFO, "Actions: %d filtered out, %d changed section or area, %d turned to ghosts",
      stat_filtered_out, stat_chg_section, stat_ghostified);

  site_hash_save(state);

  return 0;
}
