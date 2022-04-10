/*
 *	Sherlock Gatherer Daemon -- Main Program
 *
 *	(c) 1997--2004 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/clists.h"
#include "ucw/mempool.h"
#include "ucw/hashfunc.h"
#include "sherlock/object.h"
#include "gather/daemon/gatherd.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/poll.h>
#include <time.h>
#include <signal.h>
#include <errno.h>

ucw_time_t now;

struct thread {
  cnode n;
  pid_t pid;				/* Process running this thread */
  int pipe_fd;				/* Where is the thread pipe connected to */
  ucw_time_t start_time;			/* Timeout control */
  int timed_out;
  struct mempool *pool;			/* Mempool local to this thread */
  struct qhost *host;			/* Host this thread works for */
  struct qnode *qnode;			/* Queue node for the host */
  struct odes *obj;			/* Queued object attributes */
  struct urlrec ur;			/* URL database record for this object */
  int fetching_robot_file;		/* We're fetching robots.txt */
  oid_t refreshing;			/* OID we're refreshing */
  byte url[MAX_URL_SIZE];		/* Current URL */
  struct fastbuf *reply;		/* Here we read the data from the child */
};

#define TTRACE(x,y...) do { if (trace_threads) log(L_DEBUG, x,##y); } while (0)
#define XTTRACE(x,y...) do { if (trace_threads > 1) log(L_DEBUG, x,##y); } while (0)

/*** References ***/

static void
process_refs(struct odes *o)
{
  struct oattr *a;
  char *attrs = "FRYdI";

  while (*attrs)
    for (a=obj_find_attr(o, *attrs++); a; a=a->same)
      add_ref(a->val, 0);
}

/*** Dynamic filtering by robots.txt ***/

static struct buck2obj_buf *read_buf;
  /* Used at three places, but never at the same time:
   * 1. the parent process parses the replies of its children here
   * 2. the child process first parses the robots.txt file here
   * 3. and, then, it reads the original object info gthis->refreshing via this buffer
   */

static int
dynamic_filter(struct mempool *pool, byte *urlrest, oid_t oid)
{
  byte rest[MAX_URL_SIZE];
  struct obuck_context ctx;
  int accept = 1;

  if (oid == OID_UNDEFINED || oid >= OID_FIRST_ERROR)
    return 1;
  if (url_enescape(urlrest, rest))
    die("dynamic_filter: error escaping %s", urlrest);
  ctx.hdr.oid = oid;
  obuck_find_by_oid(&bucket_file, &ctx, 0);
  struct fastbuf *b = obuck_fetch(&bucket_file, &ctx);
  struct odes *obj = obj_read_bucket(read_buf, pool, ctx.hdr.type, ctx.hdr.length, b, NULL, 1);
  for (struct oattr *o = obj_find_attr(obj, 'r'); o; o=o->same)
    if (!strncmp(rest, o->val, str_len(o->val)))
      {
	accept = 0;
	break;
      }
  bclose(b);
  return accept;
}

/*** Gathering of a single document ***/

static struct thread *current_thread;
static uns sync_counter;

static int
prepare_document(struct thread *t)
{
  byte buf1[MAX_URL_SIZE];
  struct url u;
  struct qhost *h = t->host;
  struct qitem *qit;
  int e;

  t->fetching_robot_file = (h->robot_id == OID_UNDEFINED && h->protocol == URL_PROTO_HTTP);
  t->refreshing = OID_UNDEFINED;
  u.protocol = url_proto_names[h->protocol];
  u.user = NULL;
  u.pass = NULL;
  u.host = h->name;
  u.port = h->port;
  if (t->fetching_robot_file)
    u.rest = "/robots.txt";
  else if (qit = peek_item(h))
    u.rest = qit->text;
  else
    {
      finish_host(h, 0, 0);
      return 0;
    }
  if ((e = url_pack(&u, buf1)) ||
      (e = url_enescape(buf1, t->url)))
    die("prepare_document: error packing URL: %s", url_error(e));

  if (t->fetching_robot_file)
    {
      /* Need to make up a urldb record for retrying to work */
      t->ur.flags = 0;
      /* This is correct, we always handle robot files before other documents from the same host */
      t->ur.retries = h->rec_err_count;
    }
  else
    {
      if (!urldb_lookup(t->url, &t->ur))
	die("prepare_document: not in URL database: %s", t->url);
      if (!(t->ur.flags & URF_QUEUED))
	die("prepare_document: URL %s not marked as queued", t->url);
      if (t->ur.oid < OID_FIRST_ERROR)
	{
	  TTRACE("Refreshing object %08x", t->ur.oid);
	  t->refreshing = t->ur.oid;
	}
    }

  return 1;
}

static void
gather_send_result(struct thread *t)
{
  if (gthis->error_code <= 1)		/* Downloaded successfully or redirected */
    {
      u32 out_format = (compress_level > 0) ? BUCKET_TYPE_V33_LIZARD : (compress_level < 0) ? BUCKET_TYPE_V30 : BUCKET_TYPE_V33;
      struct fastbuf *b = obuck_create(&bucket_file);
      out_format = gobj_write(b, out_format, GWF_DUMP_BODY | (dump_full_objects ? GWF_DUMP_SOURCE : 0));
      struct obuck_header buck;
      obuck_create_end(&bucket_file, b, out_format, &buck);
      obj_set_attr_num(gthis->aa, 'O', buck.oid);
    }

  if (t->refreshing != OID_UNDEFINED &&
      (gthis->error_code <= 1 || gthis->error_code >= 2000))
    {
      /*
       *  CAVEAT: The remaining cases are handled in gather_finish().
       *  When changing this condition, check there as well!
       */
      TTRACE("Deleting old object %08x in subthread", t->refreshing);
      obuck_delete(&bucket_file, t->refreshing);
    }

  /* Send description to gatherd master */
  struct fastbuf *b = bfdopen(t->pipe_fd, 4096);
  gobj_write(b, BUCKET_TYPE_V33, 0);
  bclose(b);
}

static void
error_hook(void)
{
  gather_send_result(current_thread);
  exit(0);
}

static void
gather_document(struct thread *t)
{
  current_thread = t;

  /* Slurp the object and parse it */
  setproctitle("%.64s", t->url);
  gthis = gobj_new(NULL);
  gthis->error_hook = error_hook;
  gthis->url = gobj_parse_url(&gthis->url_s, t->url, "document", 0);
  gthis->robot_file_p = t->fetching_robot_file;
  if (!t->fetching_robot_file && !dynamic_filter(t->pool, gthis->url_s.rest, t->host->robot_id))
    gerror(2304, "Forbidden by robots.txt");
  if (t->refreshing != OID_UNDEFINED)
    {
      /* Read the old object we're trying to refresh */
      struct obuck_context ctx;
      ctx.hdr.oid = t->refreshing;
      obuck_find_by_oid(&bucket_file, &ctx, 0);
      struct fastbuf *b = obuck_fetch(&bucket_file, &ctx);
      gthis->refreshing = obj_read_bucket(read_buf, gthis->pool, ctx.hdr.type, ctx.hdr.length, b, NULL, 1);
      bclose(b);
      if (t->ur.http_last_mod)
	obj_set_attr_num(gthis->refreshing, 'L', t->ur.http_last_mod);
    }
  if (!t->host->qkey)
    {
      setproctitle("R %.64s", t->url);
      gather_create_key();
    }
  setproctitle("D %.64s", t->url);
  gather_download();
  /* Since now we run with configuration altered by the filters */
  setproctitle("P %.64s", t->url);
  gather_parse();
  gather_analyse();
  uns diff = gobj_check_update();
  if (!(diff & (GOBJ_CHG_TEXT_LARGE | GOBJ_CHG_REDIRECT | GOBJ_CHG_HTTP | GOBJ_CHG_BRAND_NEW)))
    gerror(4, "Not changed");

  /* Write the gathered object to a bucket and send description to gatherer master */
  setproctitle("S %.64s", t->url);
  gather_send_result(t);
}

static void
update_avg_change(struct thread *t)
{
  struct odes *o = t->obj;
  byte *Da, *pa;
  uns chg, old_chg;

  old_chg = t->ur.avg_change_time;
  chg = 0;
  Da = obj_find_aval(o, 'D');
  pa = obj_find_aval(o, 'p');
  if (Da && pa)
    {
      int age = (time_t)atol(Da) - (time_t)atol(pa);
      if (age > 0)
	{
	  if (!old_chg)
	    chg = age;
	  else
	    {
	      age = (age+255) / 256;
	      old_chg = (old_chg+255) / 256;
	      chg = doc_change_mix*old_chg + (256-doc_change_mix)*age;
	    }
	}
    }
  t->ur.avg_change_time = chg;
}

static oid_t
gather_finish(struct thread *t, int err)
{
  struct qhost *h = t->host;
  struct qnode *qn = t->qnode;
  struct odes *o = t->obj;
  byte *a, *md5, *err_msg;
  uns retry_after = 0;
  uns skip_refs = 0;
  byte *log_new = "";
  oid_t oid = OID_UNDEFINED;
  struct rfilter_data rfdata;
  int thread_err = 0;
  int touched_urldb = 0;
  u32 orig_qkey = h->qkey;
  u32 new_qkey = 0;

  if (!err)
    {
      if (err_msg = obj_find_aval(o, '!'))
	{
	  err = thread_err = atol(err_msg);
	  while (*err_msg && *err_msg != ' ')
	    err_msg++;
	  while (*err_msg == ' ')
	    err_msg++;
	}
      else
	{
	  log(L_ERROR, "Incomplete object found");
	  err = 1303;
	  err_msg = "Incomplete object";
	}
    }
  else
    err_msg = "Subprocess failed";

  /* We know the object is accepted, but we need to run the filters to alter configuration */
  rfdata.url = t->url;
  rfdata.qkey = orig_qkey;
  if (ref_filter(&rfdata) && !err)
    die("Inconsistent filters: Object %s accepted before downloading, but rejected afterwards!", t->url);

  /* If it isn't a temporary error, reset the error counters */
  if (err < 1000 || err >= 2000)
    {
      if (t->ur.retries)
	{
	  t->ur.retries = 0;
	  touched_urldb = 1;
	}
      h->rec_err_count = 0;
      qn->rec_err_count = 0;
    }

  /* Inspect error code and set oid, skip_refs and retry_after according to it */
  if (err >= 2000)			/* Fatal error */
    oid = OID_FIRST_ERROR + err;
  else if (err >= 1000)			/* Temporary error */
    {
      if (t->ur.retries >= max_rec_err)
	oid = OID_FIRST_ERROR + err;
      else
	{
	  t->ur.retries++;
	  touched_urldb = 1;
	  requeue_item(h);
	  h->rec_err_count++;
	  if (orig_qkey)	/* not for resolvers */
	    {
	      qn->rec_err_count++;
	      if (a = obj_find_aval(o, 'W'))
		retry_after = atol(a);
	      else if (qn->rec_err_count >= rec_err_limit || h->rec_err_count >= rec_err_limit)
		retry_after = rec_err_dly2;
	      else
		retry_after = rec_err_dly1;
	    }
	  else if (h->rec_err_count < 2)
	    retry_after = rec_err_dly1;
	}
    }
  else if (!err || err == 1)		/* Successful download or a redirect */
    {
      a = obj_find_aval(o, 'O');
      ASSERT(a);
      oid = atol(a);
    }
  else if (err == 2)			/* Resolving queue key */
    {
      a = obj_find_aval(o, 'k');
      ASSERT(a);
      new_qkey = strtoul(a, NULL, 16);
    }
  else if ((err == 3 || err == 4) && t->refreshing != OID_UNDEFINED)	/* Not modified */
    {
      t->ur.access = now;
      t->ur.flags &= ~(URF_QUEUED | URF_REGATHER);
      t->ur.oid = t->refreshing;
      if (err == 4)
	t->ur.http_last_mod = obj_find_anum(o, 'L', 0);
      touched_urldb = 1;
      dequeue_item(h);
      skip_refs = 1;
      log_new = "=";
    }
  else
    die("Unknown gatherer error code %d for %s", err, t->url);

  if (oid != OID_UNDEFINED)		/* We're done with this document */
    {
      if (!orig_qkey)
	{
	  /* The only possible cause is that the host doesn't exist */
	  new_qkey = 0x7f020000 + random_max(16);
	}
      if (t->fetching_robot_file)
	{
	  h->robot_id = oid;
	  h->robot_time = now;
	  touch_host(h);
	}
      else
	{
	  t->ur.access = now;
	  t->ur.flags &= ~(URF_QUEUED | URF_REGATHER);
	  t->ur.oid = oid;
	  t->ur.http_last_mod = obj_find_anum(o, 'L', 0);
	  update_avg_change(t);
	  touched_urldb = 1;
	  /*
	   * We'd like to delete the old document inside the thread, but it's hard
	   * to decide there whether it's going to be replaced by the new version
	   * or not due to somewhat complicated handling of temporary errors.
	   * Hence we do it only in clear cases and handle the rest here.
	   */
	  if (t->refreshing != OID_UNDEFINED &&
	      thread_err > 1 && thread_err < 2000)
	    {
	      TTRACE("Deleting old object %08x in main thread", t->refreshing);
	      obuck_delete(&bucket_file, t->refreshing);
	    }
	  log_new = "+";
	  if (md5 = obj_find_aval(o, 'C'))
	    {
	      struct md5rec mr;
	      if (md5db_lookup(md5, &mr) && mr.oid != t->refreshing)
		{
		  skip_refs = 1;
		  log_new = "!";
		}
	      else
		md5db_store(md5, oid);
	    }
	  dequeue_item(h);
	}
    }

  log(L_INFO, "%s: %04d %s [%d%s%s] k=%08x p=%u", t->url, err, err_msg, t->pid,
      (t->refreshing != OID_UNDEFINED) ? "*" : "",
      log_new, orig_qkey, h->qpriority);

  if (retry_after < min_server_delay && new_qkey >= NUM_RESOLVER_KEYS)
    retry_after = min_server_delay;
  if (touched_urldb && !t->fetching_robot_file)
    urldb_store(t->url, &t->ur);
  finish_host(h, retry_after, new_qkey);
  if (!skip_refs)
    process_refs(o);

  sync_counter++;
  if (sync_counter == auto_sync)
    {
      sync_counter = 0;
      queue_sync();
      db_sync();
      obuck_sync(&bucket_file);
    }
  return oid;
}

/*** Gatherer threads ***/

static clist busy_threads;
static uns thread_count;
static volatile sig_atomic_t shut_down;
static struct pollfd *poll_table;
static struct thread **poll_to_thread;
static int poll_count;

static void				/* Just sets the "shut_down" variable */
sig_int(int x UNUSED)
{
  log(L_INFO | L_SIGHANDLER, "Gracious shutdown requested");
  shut_down = 1;
}

static void
sig_term(int x UNUSED)
{
  log(L_INFO | L_SIGHANDLER, "Shutdown requested");
  shut_down = 2;
}

static void
init_threads(void)
{
  struct sigaction siga;

  clist_init(&busy_threads);
  poll_table = xmalloc(max_threads * sizeof(struct pollfd));
  poll_to_thread = xmalloc(max_threads * sizeof(struct thread *));
  bzero(&siga, sizeof(siga));
  siga.sa_handler = sig_int;
  siga.sa_flags = SA_RESTART;
  sigaction(SIGINT, &siga, NULL);
  siga.sa_handler = sig_term;
  sigaction(SIGTERM, &siga, NULL);
  siga.sa_handler = SIG_IGN;
  sigaction(SIGHUP, &siga, NULL);
}

static void
rebuild_poll_table(void)
{
  struct thread *t;

  poll_count = 0;
  CLIST_WALK(t, busy_threads)
    {
      struct pollfd *f = &poll_table[poll_count];
      f->fd = t->pipe_fd;
      f->events = POLLIN | POLLERR | POLLHUP;
      poll_to_thread[poll_count++] = t;
    }
  XTTRACE("rebuild_poll_table: %d entries, %d threads", poll_count, thread_count);
}

static void
run_thread(struct qhost *h, struct qnode *n)
{
  struct mempool *pool;
  struct thread *t;
  int fds[2];
  struct sigaction siga;

  TTRACE("Starting thread for %s://%s:%d/", url_proto_names[h->protocol], h->name, h->port);
  pool = mp_new(4096);
  t = mp_alloc(pool, sizeof(struct thread));
  t->pool = pool;
  t->host = h;
  t->qnode = n;

  if (!prepare_document(t))
    {
      mp_delete(pool);
      return;
    }
  TTRACE("Will process %s", t->url);

  if (pipe(fds) < 0)
    die("pipe: %m");
  t->pid = fork();
  if (!t->pid)
    {
      close(fds[0]);
      t->pipe_fd = fds[1];
      log_fork();
      bzero(&siga, sizeof(siga));
      siga.sa_handler = SIG_IGN;
      sigaction(SIGINT, &siga, NULL);
      siga.sa_handler = SIG_DFL;
      sigaction(SIGTERM, &siga, NULL);
      gather_document(t);
      exit(0);
    }
  TTRACE("... process %d", t->pid);
  clist_add_tail(&busy_threads, &t->n);
  t->pipe_fd = fds[0];
  t->start_time = now;
  t->timed_out = 0;
  t->obj = NULL;
  t->reply = fbmem_create(1<<16);
  close(fds[1]);
  poll_count = -1;
  thread_count++;
}

static void
thread_died(struct thread *t, int status)
{
  int c;
  int err = 0;
  oid_t oid;

  if (WIFEXITED(status))
    {
      if (c = WEXITSTATUS(status))
	{
	  log(L_ERROR, "Subprocess %d exited with rc=%d", t->pid, c);
	  err = 1300;
	}
    }
  else
    {
      if (WIFSIGNALED(status))
	log(L_ERROR, "Subprocess %d exited on signal %d", t->pid, WTERMSIG(status));
      else
	log(L_ERROR, "Subprocess %d died, cause unknown", t->pid);
      err = t->timed_out ? 2302: 1301;
    }

  oid = gather_finish(t, err);
  close(t->pipe_fd);
  clist_remove(&t->n);
  bclose(t->reply);
  mp_delete(t->pool);
  thread_count--;
  poll_count = -1;
  if (oid < OID_FIRST_ERROR)
    {
      uns obuck_size = obuck_get_pos(oid) >> 10;
      if (!shut_down && max_bucket_file_size && obuck_size >= max_bucket_file_size)
	{
	  log(L_INFO, "Bucket file size has reached %dKB, shutting down.", obuck_size);
	  shut_down = 1;
	}
    }
}

static void
handle_event(struct thread *t)
{
  byte *buf;
  uns avail = bdirect_write_prepare(t->reply, &buf);
  if (avail)
    {
      int c = read(t->pipe_fd, buf, avail);
      if (c < 0)
	die("Pipe read: %m");
      bdirect_write_commit(t->reply, buf+c);
      XTTRACE("Direct read %d bytes of input for thread %d", c, t->pid);
      if (c > 0)
	return;
    }
  else
    {
      byte tmpbuf[4096];
      int c = read(t->pipe_fd, tmpbuf, 4096);
      if (c < 0)
	die("Pipe read: %m");
      bwrite(t->reply, buf, c);
      XTTRACE("Buffered read %d bytes of input for thread %d", c, t->pid);
      if (c > 0)
	return;
    }

  XTTRACE("Pipe EOF on fd %d", t->pipe_fd);
  pid_t pid;
  int status;
  pid = waitpid(t->pid, &status, 0);
  if (pid < 0)
    die("waitpid: %m");
  TTRACE("Process %d exited with status %x", t->pid, status);

  struct fastbuf *fb = fbmem_clone_read(t->reply);
  bclose(t->reply);
  t->reply = fb;
  t->obj = obj_read_bucket(read_buf, t->pool, BUCKET_TYPE_V33, bfilesize(t->reply), t->reply, NULL, 1);
  if (unlikely(!t->obj))
    die("Cannot parse the reply of process %d", t->pid);

  thread_died(t, status);
}

static void
hard_shutdown(void)
{
  struct thread *t;
  int k = 0;

  /*
   * When killing subprocesses, lock the bucket file to avoid its corruption.
   * Unfortunately, this gives a small chance that such shutdown will result
   * in orphaned buckets, but the expirer will delete them, so no need to worry.
   */
  if (!allow_hard_shutdown)
    return;
  obuck_lock_write(&bucket_file);
  CLIST_WALK(t, busy_threads)
    {
      if (!k++)
	log(L_INFO, "Killing subprocesses");
      kill(t->pid, SIGTERM);
    }
  obuck_unlock(&bucket_file);
}

static void
loop(void)
{
  for(;;)
    {
      ucw_time_t wait_seconds = 1000000;
      int i, c, have_waiting;
      struct thread *t;

      now = time(NULL);

      setproctitle("gatherd: %d threads%s", thread_count, shut_down ? " [shutdown]" : "");
      if (!shut_down && max_host_count && host_count > max_host_count)
	{
	  log(L_INFO, "Number of known hosts has reached %d, shutting down.", host_count);
	  shut_down = 1;
	}
      have_waiting = host_time_step(&wait_seconds);
      if (thread_count < max_threads && !shut_down)
	{
	  struct qnode *n;
	  struct qhost *h = dequeue_host(&n);
	  if (h)
	    {
	      run_thread(h, n);
	      continue;
	    }
	}
      if (!thread_count)
	{
	  if (shut_down)
	    {
	      log(L_INFO, "Gatherer shut down.");
	      return;
	    }
	  if (!have_waiting)
	    {
	      log(L_INFO, "Everything gathered.");
	      return;
	    }
	}
      if ((t = clist_head(&busy_threads)) && t->start_time <= now - max_run_time)
	{
	  log(L_ERROR, "Killing subprocess %d (time exceeded)", t->pid);
	  kill(t->pid, SIGTERM);
	  t->start_time = now;
	  t->timed_out = 1;
	  continue;
	}
      if (shut_down > 1)
	{
	  hard_shutdown();
	  shut_down = 1;
	  continue;
	}
      if (!wait_seconds || wait_seconds > max_run_time)
	wait_seconds = max_run_time;
      if (poll_count < 0)
	rebuild_poll_table();
      c = poll(poll_table, poll_count, wait_seconds * 1000);
      if (c < 0)
	{
	  if (errno == EINTR || errno == EAGAIN)
	    continue;
	  die("poll: %m");
	}
      if (!c)
	continue;
      for (i=0; i<poll_count && c; i++)
	if (poll_table[i].revents)
	  {
	    handle_event(poll_to_thread[i]);
	    c--;
	  }
    }
}

static void NONRET
usage(void)
{
  fputs("Usage: gatherd [<options>]\nOptions:\n" CF_USAGE, stderr);
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(NULL);
  setproctitle_init(argc, argv);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0)
    usage();
  if (optind != argc)
    usage();

  log_file(log_name);
  now = time(NULL);
  log(L_INFO, "Sherlock Gatherer " SHER_VER " starting...");

  gatherer_init();
  gather_lock(0);
  queue_init(0);
  urldb_open();
  md5db_open();
  refs_init(0);
  bucket_open(1);

  read_buf = buck2obj_alloc();
  init_threads();
  gather_note_state("gathering");
  loop();
  buck2obj_free(read_buf);

  bucket_close();
  md5db_close();
  urldb_close();
  queue_cleanup();
  gather_unlock();
  return 0;
}
