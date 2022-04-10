/*
 *	Sherlock Shepherd Daemon -- Local gathering
 *
 *	(c) 2009 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/fastbuf.h"
#include "gather/gather.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/reap.h"

#include <string.h>
#include <stdlib.h>
#include <setjmp.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>

static uns max_children;
static uns max_load = 1000;
static uns compress_level;
static uns trace_level;
static uns child_timeout = 15;

static struct cf_section children_config = {
  CF_ITEMS {
    CF_UNS("MaxChildren", &max_children),
    CF_UNS("MaxLoad", &max_load),
    CF_UNS("Compress", &compress_level),
    CF_UNS("Trace", &trace_level),
    CF_UNS("ChildTimeOut", &child_timeout),
    CF_END
  }
};

static void CONSTRUCTOR
read_config(void)
{
  cf_declare_section("ShepChildren", &children_config, 0);
}

#define CHILD_TRACE(c, x, y...) DBG("CHILD %u: " x, c->proc.pid,##y)

struct child {
  cnode n;
  struct main_process proc;
  struct main_file in;
  struct job *job;
  struct fastbuf *out;
  struct reap_reply_hdr recv_hdr;
  byte *recv_buffer;
  uns recv_data_len;
};

static uns num_children;
static clist children;
static uns num_children_ok, num_children_failed, total_children_complexity, children_load;

static void
child_free(struct child *c)
{
  struct job *j = c->job;
  total_children_complexity += j->complexity;
  children_load -= j->complexity;
  job_process_reply(j);

  if (c->proc.data)
    {
      CHILD_TRACE(c, "Killing");
      if (kill(c->proc.pid, SIGTERM))
	die("kill: %m");
      process_del(&c->proc);
    }

  file_del(&c->in);
  close(c->in.fd);

  num_children--;
  clist_remove(&c->n);
  xfree(c);
}

static void
child_finish(struct child *c)
{
  ASSERT(!c->in.data && !c->proc.data);
  struct job *j = c->job;
  j->reply_hdr = &c->recv_hdr;
  j->reply_ctrl = c->recv_buffer;
  j->reply_data = c->recv_buffer + c->recv_hdr.control_length;
  num_children_ok++;
  child_free(c);
}

static void
child_error(struct child *c, const char *status, const char *err, ...)
{
  va_list args;
  va_start(args, err);
  CHILD_TRACE(c, "Child failed: %s", stk_vprintf(err, args));
  va_end(args);
  job_fake_reply(c->job, status ? : "1301 Gatherer bug");
  num_children_failed++;
  child_free(c);
}

static void
child_read_done(struct main_file *f)
{
  struct child *c = f->data;
  if (f->rpos < f->rlen)
    child_error(c, NULL, "Pipe closed");
  else if (!c->recv_buffer)
    {
      c->recv_data_len = c->recv_hdr.control_length + c->recv_hdr.data_length;
      ASSERT(c->recv_data_len);
      CHILD_TRACE(c, "Receiving %d bytes of data", c->recv_data_len);
      c->recv_buffer = xmalloc(c->recv_data_len);
      file_read(f, c->recv_buffer, c->recv_data_len);
    }
  else
    {
      CHILD_TRACE(c, "Receive complete");
      c->in.data = NULL;
      if (!c->proc.data)
	child_finish(c);
    }
}

static void
child_error_handler(struct main_file *f, int cause)
{
  struct child *c = f->data;
  switch (cause)
    {
      case MFERR_READ:
        child_error(c, NULL, "Read error: %m");
        break;
      case MFERR_TIMEOUT:
        child_error(c, "2302", "Timed out");
	break;
      default:
	ASSERT(0);
    }
}

static void
child_exit_handler(struct main_process *p)
{
  struct child *c = p->data;
  p->data = NULL;
  if (c->proc.status < 0)
    die("fork: %s", c->proc.status_msg);
  else if (c->proc.status)
    child_error(c, NULL, "Suprocess failed: %s", c->proc.status_msg);
  else
    {
      CHILD_TRACE(c, "Subprocess exited");
      if (!c->in.data)
	child_finish(c);
    }
}

static jmp_buf child_jmp;

static void child_error_hook(void)
{
  longjmp(child_jmp, 1);
}

static uns
child_gather(struct child *c, struct fastbuf *data_out, struct fastbuf *cntl_out)
{
  struct job *j = c->job;
  struct reap_request_hdr *h = &j->req_hdr;

  struct odes *orig;
  char *url;
  int refresh = 0;
  volatile int resolve = 0;

  if (h->data_format < BUCKET_TYPE_V30 || h->data_format > BUCKET_TYPE_V33_LIZARD) {
    log(L_ERROR, "Malformed gather request: Invalid format data %08x", h->data_format);
    exit(1);
  }

  gthis = gobj_new(NULL);
  gthis->filter_user_mark = h->user_mark;
  orig = obj_read_bucket(global_buck_buf, gthis->pool, h->data_format, h->data_length, j->req_data_fb, NULL, 1);
  if (!orig) {
    log(L_ERROR, "Malformed gather request: Failed to parse bucket");
    exit(1);
  }

  url = obj_find_aval(orig, 'U');
  if (!url) {
    log(L_ERROR, "Malformed gather request: Missing URL");
    exit(1);
  }
  if (h->type == REAP_REQ_RESOLVE)
    resolve = 1;
  else if (h->type == REAP_REQ_REFRESH && obj_find_aval(orig, 'D')) {
    refresh = 1;
    gthis->refreshing = orig;
  }
  if (trace_level >= 1)
    log(L_INFO, "%s %s", (resolve ? "Resolving" : refresh ? "Refreshing" : "Gathering"), url);

  gthis->error_hook = child_error_hook;
  if (!setjmp(child_jmp)) {
    gthis->url = gobj_parse_url(&gthis->url_s, url, "document", 0);
    gthis->robot_file_p = !strcmp(gthis->url_s.rest, "/robots.txt");
    if (resolve) {
      setproctitle("R %.64s", url);
      gather_create_key();
    }
    setproctitle("D %.64s", url);
    gather_download();
    /* Since now we run with configuration altered by the filters */
    setproctitle("P %.64s", url);
    gather_parse();
    gather_analyse();
    uns diff = gobj_check_update();
    if (!diff)
      gerror(4, "Not changed");
  }

  setproctitle("S %.64s", url);
  u32 out_format = (compress_level > 0) ? BUCKET_TYPE_V33_LIZARD : BUCKET_TYPE_V33;
  out_format = gobj_write(data_out, out_format, GWF_DUMP_BODY);
  gobj_write(cntl_out, BUCKET_TYPE_V33, 0);

  gobj_free(gthis);
  return out_format;
}

static void
child_run(struct child *c, int fd)
{
  struct fastbuf *data = fbmem_create(4096), *cntl = fbmem_create(4096);
  struct reap_reply_hdr hdr;

  bzero(&hdr, sizeof(hdr));
  hdr.data_format = child_gather(c, data, cntl);
  hdr.type = REAP_REPLY_GATHERED;
  hdr.id = c->job->req_hdr.id;
  hdr.data_length = btell(data);
  hdr.control_length = btell(cntl);
  hdr.control_format = BUCKET_TYPE_V33;

  CHILD_TRACE(c, "Replying result");
  struct fastbuf *out = bfdopen(fd, 4096);
  bwrite(out, &hdr, sizeof(hdr));
  bbcopy(fbmem_clone_read(cntl), out, hdr.control_length);
  bbcopy(fbmem_clone_read(data), out, hdr.data_length);
  bclose(out);
}

static void
child_new(struct job *j)
{
  DBG("Forking children");

  struct child *c = xmalloc_zero(sizeof(*c));
  clist_add_tail(&children, &c->n);
  num_children++;
  children_load += j->complexity;
  c->job = j;

  int fds[2];
  if (pipe(fds) < 0)
    die("pipe: %m");

  c->proc.data = c;
  c->proc.handler = child_exit_handler;
  if (!process_fork(&c->proc))
    {
      reset_signals();
      close(fds[0]);
      file_close_all();
      log_fork();
      c->proc.pid = getpid();
      DBG("START");
      child_run(c, fds[1]);
      DBG("EXIT");
      exit(0);
    }

  close(fds[1]);
  c->in.data = c;
  c->in.fd = fds[0];
  c->in.read_done = child_read_done;
  c->in.error_handler = child_error_handler;
  file_add(&c->in);
  file_read(&c->in, &c->recv_hdr, sizeof(c->recv_hdr));
  file_set_timeout(&c->in, main_now + child_timeout * 1000);

  CHILD_TRACE(c, "Waiting for reply");
}

static int
child_hook_handler(struct main_hook *h UNUSED)
{
  struct job *j;
  while (num_children < max_children && children_load < max_load && (j = job_alloc()))
    child_new(j);
  return HOOK_IDLE;
}

static struct main_hook child_hook = { .handler = child_hook_handler };

uns
child_init(void)
{
  clist_init(&children);
  if (max_children)
    {
      gatherer_init();
      hook_add(&child_hook);
    }
  return max_children;
}

void
child_cleanup(void)
{
  ASSERT(clist_empty(&children));
}

uns
child_stats(void)
{
  /* The format should match conn_stats() */
  log(L_INFO, "%10s: %5d jobs ok, %5d jobs failed, complexity sum %5d", "Local", num_children_ok, num_children_failed, total_children_complexity);
  uns total = num_children_ok;
  num_children_ok = num_children_failed = total_children_complexity = 0;
  return total;
}
