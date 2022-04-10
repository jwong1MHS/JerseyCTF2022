/*
 *	Sherlock Shepherd Reaper -- Jobs and Buckets
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2004--2005 Robert Spalek <robert@ucw.cz>
 *	(c) 2006--2009 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/conf.h"
#include "sherlock/object.h"
#include "ucw/lfs.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/workqueue.h"
#include "ucw/threads.h"
#include "gather/gather.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/reap.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

uns num_active_jobs;

static struct job *
job_new(void)
{
  struct qnode *node;
  struct qsite *site = get_site(&node);
  if (!site)
    return NULL;

  struct plan_entry *pe = plan_get_next(site);
  DBG("JOBS: allocating new job for oid=%08x pri=%u", pe->oid, pe->priority);
  struct mempool *mp = mp_new(4096);
  struct job *j = mp_alloc_zero(mp, sizeof(*j));
  j->pool = mp;
  j->site = site;
  j->plane = pe;
  j->work.priority = pe->priority;
  num_active_jobs++;
  return j;
}

static void
job_thread_go(struct worker_thread *thread UNUSED, struct work *work)
{
  struct job *j = SKIP_BACK(struct job, work, work);

  DBG("JOBS: thread %d is prefetching job for oid=%08x pri=%u", thread ? thread->id : 0, j->plane->oid, j->plane->priority);

  /* Prefetch the original bucket */
  struct obuck_context ctx;
  ctx.hdr.oid = j->plane->oid;
  j->prefetched_bucket = obuck_fetch_oid(&bucket_file, &ctx, OBUCK_NO_LOCK);
  j->prefetched_hdr = ctx.hdr;

  /* Prefetch robots.txt */
  if (!(j->plane->flags & (PEF_SYNTH_ROBOTS | PEF_ROBOTS)))
    prefetch_robots(j);
}

static struct work_queue prefetch_queue;
static struct worker_pool prefetch_pool;

static void
prefetch_queue_init(void)
{
  DBG("JOBS: initializing prefetch queue");
  if (!prefetch_threads)
    return;
  prefetch_pool.num_threads = prefetch_threads;
  worker_pool_init(&prefetch_pool);
  work_queue_init(&prefetch_pool, &prefetch_queue);
}

static void
prefetch_queue_cleanup(void)
{
  DBG("JOBS: cleaning prefetch queue");
  if (!prefetch_threads)
    return;
  struct work *work;
  while (work = work_wait(&prefetch_queue))
    job_return(SKIP_BACK(struct job, work, work));
  work_queue_cleanup(&prefetch_queue);
  worker_pool_cleanup(&prefetch_pool);
}

static void
prefetch_queue_fill(void)
{
  while (prefetch_queue.nr_running < prefetch_limit)
    {
      struct job *job = job_new();
      if (!job)
	break;
      job->work.go = job_thread_go;
      work_submit(&prefetch_queue, &job->work);
    }
}

static struct job *
job_get(void)
{
  struct job *job;
  struct work *work;
  if (!prefetch_threads)
    {
      if (!(job = job_new()))
	goto no_job;
      job_thread_go(NULL, &job->work);
    }
  else
    {
      prefetch_queue_fill();
      if (!(work = work_wait(&prefetch_queue)))
	goto no_job;
      job = SKIP_BACK(struct job, work, work);
    }
  DBG("JOBS: new job for oid=%08x pri=%u", job->plane->oid, job->plane->priority);
  return job;
no_job:
  DBG("JOBS: not job ready");
  return NULL;
}

struct job *
job_alloc(void)
{
  if (shut_down)
    return NULL;
  struct job *j;
  while (j = job_get())
    {
      if (!job_prepare_request(j))
        break;
      DBG("JOBS: autoreply");
      j->request_id = 0;
      job_process_reply(j);
    }
  return j;
}

uns
jobs_prefetched_count(void)
{
  return prefetch_queue.nr_running;
}

static struct fastbuf *journal;
static int url_log_name_size;
static byte *last_url_log_name;

uns downloaded_count, refreshed_count, anticipated_count;

static struct buck2obj_buf *buck_buf;
static struct mempool *buck_pool;
static struct mempool *robots_pool;

static struct url_db *url_db;

void
jobs_init(void)
{
  DBG("JOBS: Init");
  journal = create_state_file(current_state, "journal");
  bucket_open(1);
  url_db = url_db_open(O_RDWR | O_APPEND | O_CREAT, 1);
  if (url_db)
    log(L_INFO, "Opened URL database");
  obuck_lock_scan(&bucket_file);
  if (url_log_file)
    {
      url_log_name_size = strlen(url_log_file) + 64; /* for '%' escapes */
      last_url_log_name = xmalloc(url_log_name_size);
    }
  buck_pool = mp_new(1 << 14);
  buck_buf = buck2obj_alloc();
  robots_pool = mp_new(1 << 18);
  prefetch_queue_init();
}

void
jobs_cleanup(void)
{
  DBG("JOBS: Cleanup");
  prefetch_queue_cleanup();
  mp_delete(robots_pool);
  buck2obj_free(buck_buf);
  mp_delete(buck_pool);
  obuck_unlock_scan(&bucket_file);
  log(L_INFO, "Bucket file size: %d MB", (uns)(obuck_get_pos(obuck_predict_last_oid(&bucket_file)) / 1048576));
  if (url_db)
    url_db_close(url_db);
  bucket_close();
  bclose(journal);
}

ucw_off_t
jobs_checkpoint_journal(void)
{
  DBG("JOBS: Checkpoint journal");
  bfilesync(journal);
  return btell(journal);
}

ucw_off_t
jobs_checkpoint_buckets(void)
{
  DBG("JOBS: Checkpoint buckets");
  obuck_sync(&bucket_file);
  return obuck_get_pos(obuck_predict_last_oid(&bucket_file));
}

ucw_off_t
jobs_checkpoint_urls(void)
{
  if (!url_db)
    return 0;
  DBG("JOBS: Checkpointing urls");
  url_db_sync(url_db);
  return url_db_get_size(url_db);
}

static int url_log_fd = -1;

/*
 * Unfortunately, ucw/log.c isn't built for switching between different
 * log files or even different log formats, so we have to forge our own
 * logging routines.
 */

static void
job_url_log_switch(struct tm *tm)
{
  byte name[url_log_name_size];
  int l = strftime(name, url_log_name_size, url_log_file, tm);
  if (l < 0 || l >= url_log_name_size)
    die("Error formatting URL log file name: %m");
  if (strcmp(last_url_log_name, name))
    {
      strcpy(last_url_log_name, name);
      if (url_log_fd >= 0)
	close(url_log_fd);
      url_log_fd = ucw_open(name, O_WRONLY | O_CREAT | O_APPEND, 0666);
      if (url_log_fd < 0)
	die("Unable to open URL log file %s: %m", name);
    }
}

static void
job_url_log(const char *fmt, ...)
{
  va_list args;
  time_t t = time(NULL); 			/* We want this to be reliable, so don't use `now' */
  struct tm *tm = localtime(&t);
  int fd = 2;

  va_start(args, fmt);
  if (url_log_file)
    {
      job_url_log_switch(tm);
      fd = url_log_fd;
    }

  byte buf[2048], *b;
  int l = strftime(buf, sizeof(buf), "I %Y-%m-%d %H:%M:%S ", tm);
  if (l < 0)
    die("Error formatting URL log timestamp: %m");
  b = buf + l;
  l = vsnprintf(b, buf + sizeof(buf) - b - 2, fmt, args);
  b += l;
  if (l < 0)
    die("Internal error: URL log message too long");
  *b++ = '\n';
  l = write(fd, buf, b - buf);
  if (l != (int)(b - buf))
    die("Error writing to URL log: %m");

  va_end(args);
}

static void
job_dispose(struct job *j)
{
  num_active_jobs--;
  put_site(j->site);
  bclose(j->req_data_fb);
  mp_delete(j->pool);
}

void
job_return(struct job *j)
{
  DBG("JOBS: Returning job for oid %08x", j->plane->oid);
  if (j->prefetched_bucket)
    bclose(j->prefetched_bucket);
  if (j->prefetched_robots)
    bclose(j->prefetched_robots);
  plan_unget_next(j->site);
  job_dispose(j);
}

uns
job_prepare_request(struct job *j)
{
  struct reap_request_hdr *r = &j->req_hdr;
  uns autoreply = 0;
  uns robots_p = j->plane->flags & (PEF_ROBOTS | PEF_SYNTH_ROBOTS);
  struct cfilter_data cfd;
  byte *m;

  /* Load bucket header */
  uns body;
  j->orig_hdr = obj_read_bucket(buck_buf, j->pool, j->prefetched_hdr.type, j->prefetched_hdr.length, j->prefetched_bucket, &body, 1);
  if (!j->orig_hdr)
    die("Inconsistent bucket %08x: %m", (uns) j->plane->oid);
  j->url = obj_find_aval(j->orig_hdr, 'U');
  if (!j->url)
    die("Inconsistent bucket %08x: URL missing", (uns) j->plane->oid);

  /* Load robots.txt */
  if (!robots_p)
    fetch_robots(j, buck_buf, buck_pool, robots_pool);

  /* Initialize request fields */
  if ((j->site->qnode->qkey & SKEY_TYPE_MASK) == SKEY_UNRESOLVED)
    r->type = REAP_REQ_RESOLVE;
  else if (j->plane->flags & PEF_REFRESH)
    r->type = REAP_REQ_REFRESH;
  else
    r->type = REAP_REQ_GATHER;
  r->complexity = j->complexity = 1;

  if (j->plane->flags & PEF_SYNTH_ROBOTS)		/* Force robots.txt */
    {
      /* The object belongs to a random record for the same site, so we need to synthesize a robots.txt record first */
      struct url ur;
      byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], buf3[MAX_URL_SIZE], buf4[MAX_URL_SIZE];
      int err;
      if (err = url_canon_split(j->url, buf1, buf2, &ur))
	die("Invalid bucket URL %s: %s", j->url, url_error(err));
      ur.rest = "/robots.txt";
      if ((err = url_pack(&ur, buf3)) || (err = url_enescape(buf3, buf4)))
	die("Unable to generate robots.txt URL");
      j->url = obj_set_attr(j->orig_hdr, 'U', buf4)->val;
    }

  cfd.url = j->url;
  cfd.src_url = NULL;
  cfd.user_mark = 0;
  cfd.stable_time = 0;
  cfd.want_xform = 0;

  if (r->type != REAP_REQ_RESOLVE && (m = verify_contrib(&cfd, 0)) && (!robots_p || cfd.filter_robots))
    {
      byte buf[8 + strlen(m)];
      sprintf(buf, "%d %s", cfd.error_code, m);
      job_fake_reply(j, buf);
      autoreply = 1;
    }
  else if (j->plane->flags & PEF_SYNTH_ROBOTS)
    {
      struct fastbuf *f = fbmem_create(2048);
      put_attr_set_type(BUCKET_TYPE_V33);
      bput_attr_str(f, 'U', j->url);
      r->data_length = btell(f);
      r->data_format = BUCKET_TYPE_V33;
      j->req_data_fb = fbmem_clone_read(f);
      bclose(f);
    }
  else if (robots_p || check_robots(j))
    {
      bsetpos(j->prefetched_bucket, 0);
      r->data_length = j->prefetched_hdr.length;
      r->data_format = j->prefetched_hdr.type;
      r->user_mark = cfd.user_mark;
      j->req_data_fb = j->prefetched_bucket;
      j->prefetched_bucket = NULL;
    }
  else
    {
      job_fake_reply(j, "2304 Forbidden by robots.txt");
      autoreply = 1;
    }
  j->want_xform = cfd.want_xform;

  bclose(j->prefetched_bucket);
  j->prefetched_bucket = NULL;

  if ((j->site->qnode->qkey & SKEY_TYPE_MASK) == SKEY_NONEXISTENT && !autoreply)
    {
      /* Host marked as non-existent, so we can refuse the URL without bothering the ReapD */
      job_fake_reply(j, "2306 Site doesn't exist");
      autoreply = 1;
    }

  /* Construct the skeleton of the journal entry */
  int uerr = url_footprint(&j->journal_entry.fp, j->url);
  ASSERT(!uerr);
  j->journal_entry.oid = OID_UNDEFINED;
  if (j->plane->flags & PEF_SYNTH_ROBOTS)
    j->journal_entry.flags |= USF_ROBOTS;
  j->journal_entry.stable_time = cfd.stable_time;

  j->site->qnode->last_was_autoreply = autoreply;
  return autoreply;
}

static void
job_write_bucket(struct job *j)
{
  struct obuck_header bh;
  struct reap_reply_hdr *r = j->reply_hdr;
  struct url_state *s = &j->journal_entry;

  struct fastbuf *bf = obuck_create(&bucket_file);
  put_attr_set_type(r->data_format);
  bput_attr_format(bf, 'O', "%08x%08x%08x%08x", FP_QUAD(s->fp));
  bwrite(bf, j->reply_data, r->data_length);
  obuck_create_end(&bucket_file, bf, r->data_format, &bh);
  if (url_db)
    url_db_write(url_db, bh.oid, j->url);
  s->oid = bh.oid;
}

static enum error_type
classify_error(uns err, uns resolving)
{
  if (err < 1000)
    return ERRT_NONE;
  if (resolving)			/* Any error during resolving is treated as temporary site error */
    return ERRT_TEMP_SITE;
  switch (err)
    {
    case 2103:
    case 2104:
    case 2105:
    case 2134:
      /* We treat these errors as temporary, regardless what libgather thinks */
    case 1103:
    case 1104:
    case 1105:
    case 1134:
      return ERRT_TEMP_SITE;
    case 1107:
      return ERRT_TEMP_CONNECTION;
    case 1138:
      return ERRT_TEMP_PROXY;
    default:
      return (err >= 2000) ? ERRT_PERM : ERRT_TEMP_REQUEST;
    }
}

static uns
job_do_process_reply(struct job *j)
{
  struct qsite *site = j->site;
  struct qnode *qnode = site->qnode;
  struct odes *o;
  struct reap_reply_hdr *r = j->reply_hdr;
  struct url_state *s = &j->journal_entry;
  byte *c, *m;
  int err;
  int refreshing = j->plane->flags & PEF_REFRESH;
  int resolving = ((qnode->qkey & SKEY_TYPE_MASK) == SKEY_UNRESOLVED);

  struct fastbuf f;
  fbbuf_init_read(&f, j->reply_ctrl, r->control_length,
	(j->reply_ctrl == j->reply_data) ? 1 : 2);  /* Avoid destructing data part if shared (in fake replies) */
  o = j->ctrl = obj_read_bucket(global_buck_buf, j->pool, r->control_format, r->control_length, &f, NULL, 1);
  ASSERT(o);
#ifdef LOCAL_DEBUG
  obj_dump(o);
#endif

  if (m = obj_find_aval(o, '!'))
    {
      err = atol(m);
      while (*m && *m != ' ')
	m++;
      while (*m == ' ')
	m++;
    }
  else
    ASSERT(0);
  c = obj_find_aval(j->ctrl, 'U');
  ASSERT(c && !strcmp(j->url, c));

  /* Update the journal entry */
  qnode->last_access = main_now_seconds;
  s->last_seen = main_now_seconds;
  s->retry_count = j->plane->retry_count;

  /* Record gathering speed */
  uns gspeed = obj_find_anum(o, 'h', 0);
  uns gspeed_decisec = (gspeed + 77) / 100;	/* Biased rounding */
  if (s->retry_count)
    s->download_time = 0xf0 + MIN(s->retry_count, 15);
  else
    s->download_time = MIN(gspeed_decisec, 0xef);

  /* Some temporary errors can be promoted to permanent ones if the retry counter is too large */
  enum error_type err_type = classify_error(err, resolving);
  if (err_type == ERRT_TEMP_REQUEST || err_type == ERRT_TEMP_CONNECTION)
    {
      if (s->retry_count++ > req_err_retry)
	err_type = ERRT_PERM;
    }
  if (err_type)
    err = ((err_type == ERRT_PERM) ? 2000 : 1000) + err%1000; /* Recalculate error code */

  /* Log it */
  byte log_retry[16];
  if (s->retry_count)
    sprintf(log_retry, " r=%d", s->retry_count);
  else
    log_retry[0] = 0;
  job_url_log("%s: %04d %s [%s:%x%s] t=%d q=%04x:%08x:%d%s",
	      j->url, err, m, (j->reply_src ? : "internal"),
	      (j->reply_src ? j->request_id : 0),
	      (refreshing ? "*" : ""),
	      gspeed, QK_TRIPLE(qnode->qkey), log_retry);

  /* If it's a temporary error, log it to the journal and defer the URL to the next reap cycle */
  int old_conn_err_count = qnode->conn_err_count;
  qnode->conn_err_count = 0;
  if (err_type && err_type != ERRT_PERM)
    {
      if (err_type == ERRT_TEMP_PROXY)		/* Proxy error: put the URL back and try again */
	{
	  plan_unget_next(site);
	  return 0;
	}
      s->type = UTYPE_TEMP_ERROR;
      s->section = err_type;
      if (err_type == ERRT_TEMP_SITE)		/* Site error: defer the whole site */
	plan_reset_site(site);
      else if (err_type == ERRT_TEMP_CONNECTION)
	qnode->conn_err_count = old_conn_err_count + 1;
      return 1;
    }
  s->retry_count = 0;

  /* Process skey changes */
  uns new_skey = qkey_to_skey(qnode->qkey);
  if (c = obj_find_aval(o, 'k'))
    {
      if (sscanf(c, "%x", &new_skey) != 1 ||
	  (new_skey & SKEY_TYPE_MASK) == SKEY_UNRESOLVED ||
	  (new_skey & SKEY_TYPE_MASK) == SKEY_NONEXISTENT)
	{
	  new_skey = SKEY_NONIP | (s->fp.site.x[0] & 0xff);
	  DBG("Received bogus skey, using %08x instead", new_skey);
	}
    }
  if (qkey_to_skey(qnode->qkey) != new_skey)
    {
      DBG("SKEY changed to %08x", new_skey);
      site->new_skey = new_skey;
      if (site->skey_change_cnt < 1)
	{
	  /*
	   * Unless the skey changes too frequently, log the new one in the journal
	   * We currently limit the changes to one per reap cycle, otherwise shep-cork can
	   * ridicule us for emitting two records within one second.
	   */
	  site->skey_change_cnt++;
	  struct url_state t;
	  bzero(&t, sizeof(t));
	  t.fp.site = s->fp.site;
	  t.fp.rest = SKEY_FOOTPRINT;
	  t.last_seen = main_now_seconds;
	  t.oid = new_skey;
	  t.flags = USF_ROBOTS;
	  t.type = UTYPE_SKEY;
	  bwrite(journal, &t, sizeof(t));
	  DBG("Logged SKEY change");
	}
    }

  /* If we were just resolving, return the requested URL back to the plan */
  if (resolving)
    {
      ASSERT(err_type == ERRT_NONE);
      ASSERT(site->new_skey);
      plan_unget_next(site);
      return 0;
    }

  /* Update global counters */
  if (j->plane->flags & PEF_ANTICIPATED)
    anticipated_count++;
  else if (refreshing)
    refreshed_count++;
  else
    downloaded_count++;

  /* Update the stability time */
  if (!s->stable_time)
    {
      byte *aJ = obj_find_aval(j->ctrl, 'J');
      byte *aD = obj_find_aval(j->ctrl, 'D');
      if (aJ && aD)
        {
          int age = (atol(aD) - atol(aJ)) / stable_time_unit;
          s->stable_time = CLAMP(age, 0, 255);
        }
    }

  /* Check refreshing */
  if (err == 3 || err == 4)
    {
      ASSERT(refreshing);
      s->type = UTYPE_OK;
      return 1;
    }

  /* Raise the undead */
  if (err_type == ERRT_PERM && !(j->plane->flags & PEF_SACRISIMMUS) && !USF_IS_SACRISIMMUS(s->flags))
    {
      ASSERT((oid_t)err != OID_UNDEFINED);
      for (uns i = 0; i < DARY_LEN(zombie_gerr_ranges); i++)
        {
	  struct unsrange *r = zombie_gerr_ranges + i;
          if ((uns)err >= r->min && (uns)err <= r->max)
            {
              s->type = UTYPE_ZOMBIE;
              s->oid = err;
              return 1;
            }
        }
    }

  /* Write the result to a bucket and take a note in the journal */
  job_write_bucket(j);
  s->type = (err >= 2000) ? UTYPE_ERROR : UTYPE_OK;

  /* Process references if it's a normal page or a redirect */
  if (err == 0 || err == 1)
    write_contribs(j);
  return 1;
}

void
job_process_reply(struct job *j)
{
  if (job_do_process_reply(j))
    {
      struct url_state *s = &j->journal_entry;
      if (s->oid == OID_UNDEFINED)
	{
	  /* If no bucket got created, copy the original OID.
	   * If it didn't exist (synthetic robots.txt request), force creation of bucket.
	   */
	  if (j->plane->flags & PEF_SYNTH_ROBOTS)
	    job_write_bucket(j);
	  else
	    s->oid = j->plane->oid;
	  ASSERT(s->oid != OID_UNDEFINED);
	}
      bwrite(journal, s, sizeof(*s));
    }
  job_dispose(j);
}

void
job_fake_reply(struct job *j, const char *msg)
{
  struct reap_reply_hdr *p = mp_alloc(j->pool, sizeof(struct reap_reply_hdr) + 4 + strlen(j->url) + 32 + strlen(msg) + 1);
  j->reply_hdr = p;
  byte *rb = (byte *)(p+1);
  j->reply_ctrl = j->reply_data = rb;
  put_attr_set_type(BUCKET_TYPE_V33);
  rb = put_attr_str(rb, 'U', j->url);
  *rb++ = 0;
  rb = put_attr_num(rb, 'd', (uns) main_now_seconds);
  rb = put_attr_str(rb, '!', (char *)msg);
  bzero(p, sizeof(*p));
  p->type = REAP_REPLY_GATHERED;
  p->id = j->req_hdr.id;
  p->control_length = p->data_length = rb - j->reply_data;
  p->control_format = BUCKET_TYPE_V33;
  p->data_format = BUCKET_TYPE_V33;
  j->reply_src = NULL;
}
