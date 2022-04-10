/*
 *	Sherlock Shepherd Daemon -- Database Recovery
 *
 *	(c) 2004--2005 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"
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
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

static ucw_time_t now;

static enum recovery_mode {
  REC_FULL,
  REC_RESYNC,
  REC_ROLLBACK
} mode;

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

#define SORT_PREFIX(x) idx_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_UNIQUE
#define SORT_HASH_FP
static inline int
idx_compare(struct url_state *x, struct url_state *y)
{
  int r = fp_cmp(&x->fp, &y->fp);
  if (r)
    return r;
  uns x_type = ustate_type(x), y_type = ustate_type(y);
  if (x_type != UTYPE_NEW && y_type == UTYPE_NEW)
    return -1;
  if (x_type == UTYPE_NEW && y_type != UTYPE_NEW)
    return 1;
  REV_COMPARE(x->last_seen, y->last_seen);
  REV_COMPARE(x->oid, y->oid);
  return 0;
}
#include "gather/shepherd/index-sort.h"

static struct fastbuf *
scan_buckets(void)
{
  struct fastbuf *out = temp_state_file();
  struct fastbuf *buck;
  struct obuck_header bh;
  struct mempool *mp = mp_new(65536);
  uns progress_cnt=0, new_cnt=0, ok_cnt=0, filter_out_cnt=0, new_site_cnt=0;;
  struct odes *o;
  struct url_state s;
  struct buck2obj_buf *buck_buf = buck2obj_alloc();
  struct url_db *url_db;

  bucket_open(0);
  oid_t progress_last_oid = obuck_predict_last_oid(&bucket_file);
  if (url_db = url_db_open(O_CREAT | O_TRUNC | O_WRONLY, 1))
    log(L_INFO, "Creating new URL database");
  while (buck = obuck_slurp_pool(&bucket_file, &bh, OBUCK_OID_ANY))
    {
      if (!(progress_cnt++ % 10000))
	setproctitle("shep-recover: %d buckets (%d%%) -> %d records", progress_cnt,
		     progress_last_oid ? (int)((float)bh.oid/(float)progress_last_oid*100) : 0,
		     new_cnt + ok_cnt);
      mp_flush(mp);
      o = obj_read_bucket(buck_buf, mp, bh.type, bh.length, buck, NULL, 1);
      if (!o)
	{
	  log(L_ERROR, "Cannot parse bucket %08x with type %08x: %m", (uns)bh.oid, bh.type);
	  continue;
	}
      byte *url = obj_find_aval(o, 'U');
      byte *dwn = obj_find_aval(o, 'D');
      byte *fpr = obj_find_aval(o, 'O');
      byte *res = obj_find_aval(o, '!');

      if (!url)
	{
	  log(L_ERROR, "Bucket %08x has no URL", (uns)bh.oid);
	  continue;
	}
      if (strlen(fpr) != 32 || sscanf(fpr, "%8x%8x%8x%8x", &s.fp.site.x[0], &s.fp.site.x[1], &s.fp.rest.x[0], &s.fp.rest.x[1]) != 4)
	{
	  log(L_ERROR, "Bucket %08x has missing or invalid footprint", (uns)bh.oid);
	  continue;
	}
      DBG("B %08x <%s> %08x%08x:%08x%08x", (uns)bh.oid, url, FP_QUAD(s.fp));
      struct cfilter_data cfd;
      cfd.url = url;
      int is_robots = !memcmp(&s.fp.rest, &ROBOTS_TXT_FOOTPRINT, sizeof(struct rest_fp));
      if (!filter_url(&cfd) && !is_robots)
	{
	  DBG("\tFILTERED OUT");
	  filter_out_cnt++;
	  continue;
	}

      struct site *site = site_lookup(&s.fp.site);
      if (!site)
	{
	  if (mode == REC_RESYNC)
	    {
	      log(L_INFO, "No site known for URL %s", url);
	      continue;
	    }
	  site = site_create(&s.fp.site, cfd.url_s.protoid, cfd.url_s.host, cfd.url_s.port);
	  new_site_cnt++;
	}

      s.oid = bh.oid;
      if (dwn && res)
	{
	  s.type = (res[0] == '0') ? UTYPE_OK : UTYPE_ERROR;
	  s.last_seen = atol(dwn);
	  ok_cnt++;
	}
      else
	{
	  s.type = UTYPE_NEW;
	  s.last_seen = now;
	  new_cnt++;
	}
      s.retry_count = 0;
      s.weight = 0;
      if (is_robots)
	s.flags = USF_ROBOTS;
      else if (!memcmp(&s.fp.rest, &ROOT_FOOTPRINT, sizeof(struct rest_fp)))
	s.flags = USF_NEEDED_BY_EQ;
      else
	s.flags = 0;
      s.stable_time = CLAMP(cfd.stable_time, 0, 255);
      s.refresh_freq = 0;
      s.download_time = 0;
      s.section = CLAMP(cfd.section, 0, 255);
      s.area = cfd.area;
      bwrite(out, &s, sizeof(s));
      if (url_db)
        url_db_write(url_db, bh.oid, url);
    }
  if (url_db)
    url_db_close(url_db);
  bucket_close();
  buck2obj_free(buck_buf);
  mp_delete(mp);
  log(L_INFO, "Scanned %d buckets, found %d downloaded, %d new, %d filtered out",
      progress_cnt, ok_cnt, new_cnt, filter_out_cnt);
  if (mode != REC_RESYNC)
    log(L_INFO, "Created %d new sites", new_site_cnt);
  brewind(out);
  return out;
}

static void
merge_updates(struct fastbuf *idx, struct fastbuf *old, struct fastbuf *new)
{
  struct url_state os, ns;
  struct footprint last_ns;
  int have_os, have_ns;
  uns lost_cnt=0, new_cnt=0, upd_cnt=0, unref_cnt=0;

  have_os = breadb(old, &os, sizeof(os));
  have_ns = breadb(new, &ns, sizeof(ns));
  while (have_os || have_ns)
    {
      int cmp;
      if (!have_ns)
	cmp = -1;
      else if (!have_os)
	cmp = 1;
      else
	cmp = fp_cmp(&os.fp, &ns.fp);
      if (cmp < 0)
	{
	  if (ustate_type(&os) == UTYPE_SKEY || ustate_type(&os) == UTYPE_ZOMBIE)
	    bwrite(idx, &os, sizeof(os));
	  else
	    lost_cnt++;
	}
      else if (cmp > 0)
	{
	  if (mode != REC_RESYNC)
	    bwrite(idx, &ns, sizeof(ns));
	  new_cnt++;
	}
      else if (ustate_type(&os) == UTYPE_ZOMBIE)
        {
	  bwrite(idx, &os, sizeof(os));
	  unref_cnt++;
	  /* Leave unreferenced bucket until next cleanup */
	}
      else
	{
	  ustate_set_type(&os, ustate_type(&ns));
	  os.oid = ns.oid;
	  if (ns.stable_time)
	    os.stable_time = ns.stable_time;
	  bwrite(idx, &os, sizeof(os));
	  upd_cnt++;
	}
      if (cmp <= 0)
	have_os = breadb(old, &os, sizeof(os));
      if (cmp >= 0)
	{
	  last_ns = ns.fp;
	  do
	    have_ns = breadb(new, &ns, sizeof(ns));
	  while (have_ns && !fp_cmp(&last_ns, &ns.fp));
	}
    }
  bclose(old);
  bclose(new);
  log(L_INFO, "%d URL's updated, %d %s, %d lost, %d unreferenced", upd_cnt, new_cnt, (mode == REC_RESYNC) ? "unidentified" : "new", lost_cnt, unref_cnt);
}

static void
trunc_file(byte *state, byte *file, ucw_off_t pos)
{
  byte *n = state ? state_file_name(state, file) : file;
  int fd = ucw_open(n, O_RDWR);
  if (fd < 0)
    die("Cannot open %s: %m", n);
  ucw_off_t len = ucw_seek(fd, 0, SEEK_END);
  if (len < pos)
    die("Cannot truncate %s, because it is too short", n);
  ucw_ftruncate(fd, pos);
  close(fd);
}

static void
rollback(byte *state)
{
  struct checkpoint_entry ce;
  byte *cpn = state_file_name(state, "checkpoints");
  int fd;
  if ((fd = ucw_open(cpn, O_RDONLY)) < 0)
    {
      log(L_ERROR, "No checkpoint file found.");
      exit(1);
    }
  ucw_off_t len = ucw_seek(fd, 0, SEEK_END);
  ASSERT(len >= 0);
  if (len % sizeof(struct checkpoint_entry))
    {
      log(L_INFO, "Checkpoint file ends with an incomplete record, truncating");
      len -= (len % sizeof(ce));
    }
  if (!len)
    {
      log(L_ERROR, "No checkpoint found");
      exit(1);
    }
  ucw_seek(fd, len - sizeof(ce), SEEK_SET);
  if (read(fd, &ce, sizeof(ce)) != sizeof(ce))
    die("Error reading checkpoint: %m");
  close(fd);

  time_t tt = ce.time;
  struct tm *tm = localtime(&tt);
  log(L_INFO, "Rolling back to %04d-%02d-%02d %02d:%02d:%02d",
      tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec);

  trunc_file(state, "journal", ce.journal_pos);
  ASSERT(!(ce.contrib_pos & (CONTRIB_ALIGN-1)));
  trunc_file(state, "contrib", ce.contrib_pos);
  if (ce.urls_pos)
    trunc_file(NULL, url_database_file, ce.urls_pos);
  /* For now, we do not truncate the bucket file, buckettool will always fix it anyway */
  log(L_INFO, "Done");
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-cleanup [<options>] <state> <failure-type>\n\
\n\
Options:\n\
" CF_USAGE "\
\n\
Failure types:\n\
reap\t\t\tInterrupted reaping phase (rollback to last checkpoint)\n\
cleanup\t\t\tInterrupted cleanup phase (re-synchronize state with buckets)\n\
full\t\t\tTry to rebuild everything from the bucket file\n\
");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  setproctitle_init(argc, argv);

  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc - 2)
    usage();

  byte *state = cf_strdup(argv[optind]);
  byte *failed = argv[optind+1];
  if (!strcmp(failed, "cleanup"))
    mode = REC_RESYNC;
  else if (!strcmp(failed, "full"))
    mode = REC_FULL;
  else if (!strcmp(failed, "reap"))
    mode = REC_ROLLBACK;
  else
    usage();

  if (mode == REC_ROLLBACK)
    {
      rollback(state);
      return 0;
    }

  setproctitle("shep-recover: preprocessing");
  init_filter();
  now = time(NULL);

  log(L_INFO, "Loading site list");
  site_hash_init(NULL);
  site_hash_load(state, SITE_HASH_NO_URLS);

  log(L_INFO, "Scanning buckets");
  struct fastbuf *new_idx = scan_buckets();

  log(L_INFO, "Sorting found documents");
  new_idx = idx_sort(new_idx, NULL);

  log(L_INFO, "Sorting original index");
  struct fastbuf *old_idx = read_state_file(state, "index");
  old_idx = idx_sort(old_idx, NULL);

  state_params_new(state);

  log(L_INFO, "Recovering index");
  setproctitle("shep-recover: postprocessing");
  struct fastbuf *idx = temp_state_file();
  merge_updates(idx, old_idx, new_idx);
  put_state_file(state, "index", idx, 0);
  state_flags_set(state, STATE_FLAG_SORTED);

  site_hash_save(state);

  struct fastbuf *x = temp_state_file();
  bputs(x, "closed\n");
  put_state_file(state, "control", x, 0);

  return 0;
}
