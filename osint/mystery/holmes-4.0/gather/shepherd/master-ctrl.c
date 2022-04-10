/*
 *	Sherlock Shepherd Daemon -- Control Connections
 *
 *	(c) 2004--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2004 Robert Spalek <robert@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/conf.h"
#include "ucw/mainloop.h"
#include "ucw/ipaccess.h"
#include "ucw/mempool.h"
#include "ucw/chartype.h"
#include "sherlock/object.h"
#include "ucw/unaligned.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/master.h"
#include "gather/shepherd/protocol.h"
#include "gather/shepherd/export.h"
#include "gather/shepherd/man.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/file.h>

static struct main_file listening_socket;

struct connection {
  struct main_file f;
  struct shepp_packet_hdr req_hdr;
  byte *req_data;
  struct shepp_packet_hdr reply_hdr;
  byte reply_data[16];			// Must follow reply_hdr
  struct state *locked_state;
  struct main_timer timeout;
  struct main_process process;
  int pid;
  byte *user_state;			// User-specified state name
  uns user_limit;			// User limit on number of buckets
  uns user_min_weight;			// User limit on object weight
  uns user_best_active;			// User wants to select best N active buckets
  uns user_best_inactive;		// User wants to select best N inactive buckets
  uns num_buckets;			// Number of buckets exported
  byte *export_name;
};

/*** Feeding of the indexer ***/

static struct connection *send_conn;

static void
prepare_exports(struct connection *c)
{
  struct state *st = c->locked_state;

  if (!c->user_limit) /* Do not create export with zero user limit */
    return;

  if (c->user_min_weight || c->user_best_active != ~0U || c->user_best_inactive != ~0U)
    {
      sprintf(st->fn, "tmp/export-%d", c->pid);
      byte o1[32], o2[32], o3[32], *o[4];
      uns oi = 0;
      if (c->user_min_weight)
	sprintf(o[oi++] = o1, "-w%d", c->user_min_weight);
      if (c->user_best_active != ~0U)
	sprintf(o[oi++] = o2, "-B%d", c->user_best_active);
      if (c->user_best_inactive != ~0U)
	sprintf(o[oi++] = o3, "-b%d", c->user_best_inactive);
      o[oi] = NULL;
      if (!run_command("bin/shep-export", st->name, st->fn, o[0], o[1], o[2], o[3]))
	die("Unable to build export list");
    }
  else
    {
      int lock_fd = open(state_fn(st, "exports.lock"), O_WRONLY | O_CREAT | O_TRUNC, 0666);
      if (lock_fd < 0)
	die("Cannot open %s: %m", st->fn);
      if (flock(lock_fd, LOCK_EX) < 0)
	die("Cannot flock %s: %m", st->fn);
      state_fn(st, "exports");
      if (access(st->fn, R_OK) < 0 &&
	  !run_command("bin/shep-export", st->name, st->fn, NULL))
	die("Unable to build export list");
      flock(lock_fd, LOCK_UN);
      close(lock_fd);
    }

  c->export_name = cf_strdup(st->fn);

  struct fastbuf *exp = bopen(st->fn, O_RDONLY, 1024);
  c->num_buckets = bfilesize(exp) / sizeof(struct export_entry);
  c->num_buckets = MIN(c->num_buckets, c->user_limit);
  bclose(exp);
}

static void
my_shepp_error(uns code, char *err)
{
  log(L_ERROR, "Communication error %d: %s", code, err);
  exit(0);
}

static uns
ctrl_send_single(struct connection *c, uns raw_mode)
{
  if (!c->user_limit)
    {
      log(L_INFO, "Connection %d: Cannot send buckets with zero user limit", c->f.fd);
      shepp_send_none(NULL, SHEPP_REPLY_UNKNOWN_REQ, &c->req_hdr);
      return 0;
    }

  log(L_INFO, "Sending %sbuckets", (raw_mode ? "raw " : ""));

  struct fastbuf *exp = bopen(c->export_name, O_RDONLY, 65536);
  struct fastbuf *buck;
  struct fastbuf *out = shepp_fb_open_write(&c->req_hdr);

  struct export_entry ee;
  struct obuck_header bh;
  int out_limit = (out->bufend - out->buffer)/4;
  struct shepp_bucket_header sh;
  byte synth_buf[256];
  uns count = c->num_buckets;
  struct buck2obj_buf *strip_b2o = NULL;
  struct mempool *strip_pool = NULL;

  while (breadb(exp, &ee, sizeof(ee)) && count--)
    {
      buck = obuck_slurp_pool(&bucket_file, &bh, ee.oid);
      if (!buck)
	die("Inconsistent bucket file or exports: exported oid %x missing", ee.oid);

      ucw_time_t last_checked = ee.last_checked_time & ~1U;
      uns strip = ee.last_checked_time & 1;

      byte *synth = synth_buf;
      if (!raw_mode)
        {
          put_attr_set_type(bh.type);
          synth = put_attr_num(synth, 'V', last_checked);
          synth = put_attr_format(synth, 'W', "g%d", ee.weight);
	}
      uns synth_len = synth - synth_buf;

      sh.oid = bh.oid;
      if (strip)
	{
	  /* Send only the bucket header */
	  if (!strip_b2o)
	    {
	      strip_b2o = buck2obj_alloc();
	      strip_pool = mp_new(4096);
	    }
	  mp_flush(strip_pool);
	  struct odes *obj = obj_new(strip_pool);
	  uns body_start;
	  if (buck2obj_parse(strip_b2o, bh.type, bh.length, buck, obj, &body_start, NULL, 1) < 0)
	    die("Inconsistent bucket file: unable to parse oid %x", ee.oid);
	  put_attr_set_type(BUCKET_TYPE_V33);
	  sh.length = size_object(obj);
	  sh.type = BUCKET_TYPE_V33;
	  bwrite(out, &sh, sizeof(sh));
	  bput_object(out, obj);
	}
      else
	{
	  /* Send the whole bucket */
	  sh.length = bh.length + synth_len;
	  sh.type = bh.type;
	  bwrite(out, &sh, sizeof(sh));
	  bwrite(out, synth_buf, synth_len);
	  bbcopy(buck, out, bh.length);
	}

      /* Try to avoid buckets separated into 2 pieces by flushing the fastbuf
       * if the available space is too small.  The constant 1/4 chosen here
       * shrinks the buffer effectively to 3/4 of its size, which is
       * acceptable.  */
      if (out->bufend - out->bptr < out_limit)
	bflush(out);
    }
  obuck_slurp_end(&bucket_file);

  if (strip_b2o)
    {
      buck2obj_free(strip_b2o);
      mp_delete(strip_pool);
    }

  bclose(out);
  bclose(exp);
  log(L_INFO, "Buckets sent");
  return 1;
}

static uns
ctrl_send_bucket(struct connection *c, void *payload)
{
  struct footprint fp;
  if (c->req_hdr.data_len != sizeof(fp))
    {
      log(L_INFO, "Connection %d: Malformed request for sending bucket", c->f.fd);
      shepp_send_none(NULL, SHEPP_REPLY_UNKNOWN_REQ, &c->req_hdr);
      return 0;
    }
  memcpy(&fp, payload, sizeof(fp));
  struct fastbuf *out = shepp_fb_open_write(&c->req_hdr);

  uns gt;
  struct sel_binary_src *src = sel_binary_open_file(state_fn(c->locked_state, "index"), sizeof(struct url_state), 0);
  struct url_state *s = sel_binary_find(src, &fp, &gt);
  while (1)
    {
      if (!s || fp_cmp(&fp, &s->fp))
        {
	  log(L_INFO, "Connection %d: Bucket %08x%08x:%08x%08x not found", c->f.fd, FP_QUAD(fp));
	  break;
	}
      if (s->type != UTYPE_SKEY && s->type != UTYPE_ZOMBIE)
        {
	  struct obuck_context ctx;
	  ctx.hdr.oid = s->oid;
	  struct fastbuf *in = obuck_fetch_oid(&bucket_file, &ctx, 0);
	  struct shepp_bucket_header sh;
	  sh.length = ctx.hdr.length;
	  sh.type = ctx.hdr.type;
	  sh.oid = ctx.hdr.oid;
	  bwrite(out, &sh, sizeof(sh));
	  bbcopy(in, out, ctx.hdr.length);
	  ASSERT(bgetc(in) == -1);
	  bclose(in);
	  log(L_INFO, "Connection %d: Bucket %x sent", c->f.fd, ctx.hdr.oid);
	  break;
	}
      s = sel_binary_find_next(src);
    }

  sel_binary_close(src);
  bclose(out);
  return 1;
}

static uns
ctrl_send_urls(struct connection *c, void *payload)
{
  if (c->req_hdr.data_len != 8)
    {
      log(L_INFO, "Connection %d: Malformed request for sending URL database", c->f.fd);
      shepp_send_none(NULL, SHEPP_REPLY_UNKNOWN_REQ, &c->req_hdr);
      return 0;
    }
  if (!url_database_file || !*url_database_file)
    {
      log(L_INFO, "Connection %d: Requested URL database does not exist", c->f.fd);
      shepp_send_none(NULL, SHEPP_REPLY_UNKNOWN_REQ, &c->req_hdr);
      return 0;
    }
  u64 offset = get_u64(payload);
  log(L_INFO, "Sending URL database%s", !offset ? "" : ~offset ? " incrementally" : " header");
  struct fastbuf *in = bopen(url_database_file, O_RDONLY, 65536);
  struct fastbuf *out = shepp_fb_open_write(&c->req_hdr);
  if (~offset)
    {
      bsetpos(in, offset);
      bbcopy_slow(in, out, ~0U); /* Safe because new records are always written atomically */
    }
  else
    bbcopy_slow(in, out, sizeof(struct url_db_hdr));
  bclose(out);
  bclose(in);
  log(L_INFO, "URL database sent");
  return 1;
}

static void
ctrl_send_raw(struct connection *c, byte *file)
{
  setproctitle("shepherd: Sending raw %s (%s)", file, c->locked_state->name);
  log(L_INFO, "Sending raw %s", file);
  struct fastbuf *idx = read_state_file(c->locked_state->name, file);
  struct fastbuf *out = shepp_fb_open_write(&c->req_hdr);
  bbcopy_slow(idx, out, ~0U);
  bclose(out);
  bclose(idx);
  log(L_INFO, "Raw %s sent", file);
}

static void
ctrl_recv_feedback(struct connection *c)
{
  log(L_INFO, "Receiving feedback");

  shepp_send_none(NULL, SHEPP_REPLY_OK, &c->req_hdr);

  struct fastbuf *f = bopen_tmp(65536);
  struct fastbuf *in = shepp_fb_open_read(NULL);
  bbcopy_slow(in, f, ~0U);
  bclose(in);
  bfix_tmp_file(f, fn_feedback);
  log(L_INFO, "Feedback received");
}

static void
ctrl_send_mode(struct connection *c)
{
  mail_fork();
  DBG("Entered feeding process (fd=%d)", c->f.fd);
  setproctitle("shepherd: Exporting %s", c->locked_state->name);
  shepp_fd = c->f.fd;
  send_conn = c;
  fcntl(c->f.fd, F_SETFL, 0);	/* We are in a subprocess, so we can block */

  /* Reset signal handlers */
  signal(SIGTERM, SIG_DFL);

  /* Close all unnecessary files */
  file_close_all();

  /* Prepare export file and count entries */
  if (c->user_limit)
    prepare_exports(c);

  /* Reply to the send mode request */
  struct odes *attrs = shepp_new_attrs();
  obj_set_attr_num(attrs, 'N', c->num_buckets);
  obj_add_attr(attrs, 'S', c->locked_state->name);
  shepp_send_attrs(NULL, SHEPP_REPLY_SEND_MODE, &c->req_hdr, attrs);
  log(L_INFO, "Ready to send");

  /* Serve individual send requests */
  for(;;)
    {
      setproctitle("shepherd: Waiting (%s)", c->locked_state->name);
      if (!shepp_recv_hdr(&c->req_hdr, NULL))
	break;
      void *req_payload = shepp_recv_data(&c->req_hdr);
      switch (c->req_hdr.type)
	{
	case SHEPP_REQ_SEND_BUCKETS:
	  setproctitle("shepherd: Sending (%s)", c->locked_state->name);
	  if (!ctrl_send_single(c, 0))
	    return;
	  break;
	case SHEPP_REQ_SEND_FEEDBACK:
	  setproctitle("shepherd: Receiving feedback (%s)", c->locked_state->name);
	  ctrl_recv_feedback(c);
	  break;
	case SHEPP_REQ_SEND_RAW_BUCKETS:
	  setproctitle("shepherd: Sending raw (%s)", c->locked_state->name);
	  if (!ctrl_send_single(c, 1))
	    return;
	  break;
	case SHEPP_REQ_SEND_URLS:
          setproctitle("shepherd: Sending URL database");
	  if (!ctrl_send_urls(c, req_payload))
	    return;
	  break;
	case SHEPP_REQ_SEND_BUCKET:
	  setproctitle("shepherd: Sending bucket");
	  if (!ctrl_send_bucket(c, req_payload))
	    return;
	  break;
	case SHEPP_REQ_SEND_RAW_INDEX:
	  ctrl_send_raw(c, "index");
	  break;
	case SHEPP_REQ_SEND_RAW_SITES:
	  ctrl_send_raw(c, "sites");
	  break;
	case SHEPP_REQ_SEND_RAW_PARAMS:
	  ctrl_send_raw(c, "params");
	  break;
	default:
	  shepp_send_none(NULL, SHEPP_REPLY_UNKNOWN_REQ, &c->req_hdr);
	  die("Protocol violation: packet type %08x received in send mode", c->req_hdr.type);
	}
    }
  log(L_INFO, "EOF received, closing connection");
}

/*** Control connections ***/

static struct connection *borrow_waiting_conn;
uns borrow_requested;

static void
ctrl_cleanup(struct connection *c)
{
  if (c == borrow_waiting_conn)
    {
      borrow_waiting_conn = NULL;
      borrow_requested = 0;
    }
  if (c->locked_state)
    {
      struct state *st = c->locked_state;
      log(L_INFO, "Unlocking state %s", st->name);
      ASSERT(st->num_locks);
      st->num_locks--;
    }
  if (c->pid)
    {
      byte tmpname[64];
      sprintf(tmpname, "tmp/export-%d", c->pid);
      if (unlink(tmpname) < 0 && errno != ENOENT)
	log(L_ERROR, "Cannot unlink %s: %m", tmpname);
    }
  xfree(c->req_data);
  xfree(c);
}

static void
ctrl_error(struct main_file *fi, int cause)
{
  struct connection *c = fi->data;

  if (cause == -1)
    {
      if (log_connections)
	log(L_INFO, "Connection %d: closed", fi->fd);
    }
  else if (cause == -2)
    log(L_ERROR, "Connection %d: Protocol violation", fi->fd);
  else
    {
      static byte *c[] = { "Read error", "Write error", "Timeout" };
      log(L_ERROR, "Connection %d: %s", fi->fd, c[cause]);
    }
  close(fi->fd);
  file_del(fi);
  ctrl_cleanup(c);
}

static void
ctrl_send_finished(struct main_process *pr)
{
  struct connection *c = pr->data;
  if (pr->status_msg[0])
    log(L_ERROR, "Bucket sending subprocess %s", pr->status_msg);
  timer_del(&c->timeout);
  ctrl_cleanup(c);
}

static void
ctrl_send_timeout(struct main_timer *tm)
{
  struct connection *c = tm->data;
  log(L_INFO, "Bucket sending subprocess %d timed out, terminating it", c->process.pid);
  kill(c->process.pid, SIGTERM);
  timer_del(tm);
}

static void ctrl_got_header(struct main_file *fi);

static void
ctrl_sent_reply(struct main_file *fi)
{
  struct connection *c = fi->data;
  fi->read_done = ctrl_got_header;
  file_read(fi, &c->req_hdr, sizeof(c->req_hdr));
  file_set_timeout(fi, main_now + 1000*ctrl_timeout);
}

static int
ctrl_lock_state(struct connection *c)
{
  if (c->locked_state)
    return 1;
  struct state *st = clist_tail(&state_list);
  while (st->phase != M_CLOSED || (c->user_state && strcasecmp(st->name, c->user_state)))
    {
      st = clist_prev(&state_list, &st->n);
      if (!st)
	return 0;
    }
  st->num_locks++;
  st->export_count++;
  c->locked_state = st;
  return 1;
}

static void
ctrl_ship_reply(struct connection *c, u32 type)
{
  c->reply_hdr.type = type;
  ASSERT(c->reply_hdr.data_len <= sizeof(c->reply_data));
  file_write(&c->f, &c->reply_hdr, sizeof(c->reply_hdr) + c->reply_hdr.data_len);
}

static void
ctrl_switch_to_send_mode(struct connection *c)
{
  /* Fetch parameters */
  struct odes *attrs = shepp_new_attrs();
  shepp_decode_attrs(attrs, c->req_data, c->req_hdr.data_len);
  c->user_state = obj_find_aval(attrs, 'S');
  c->user_limit = obj_find_anum(attrs, 'M', ~0U);
  c->user_min_weight = obj_find_anum(attrs, 'W', 0);
  c->user_best_active = obj_find_anum(attrs, 'B', ~0U);
  c->user_best_inactive = obj_find_anum(attrs, 'b', ~0U);

  if (c->user_state)
    log(L_INFO, "Connection %d: Bucket sending requested from state %s", c->f.fd, c->user_state);
  else
    log(L_INFO, "Connection %d: Bucket sending requested", c->f.fd);

  if (scheduled_cleanup || opt_private || shutdown_requested)
    {
      log(L_INFO, "Connection %d: State lock deferred", c->f.fd);
      ctrl_ship_reply(c, SHEPP_REPLY_DEFER);
    }
  else if (!ctrl_lock_state(c))
    {
      log(L_INFO, "Connection %d: Requested state doesn't exist", c->f.fd);
      ctrl_ship_reply(c, SHEPP_REPLY_NO_SUCH_STATE);
    }
  else
    {
      /* Pass the connection to a new process and let the master forget about it */
      /* (except for a process catcher set up to unlock the state when the child is done) */
      file_del(&c->f);
      c->process.handler = ctrl_send_finished;
      c->process.data = c;
      if (!process_fork(&c->process))
	{
	  /* Child process */
	  c->pid = getpid();
	  ctrl_send_mode(c);
	  exit(0);
	}
      c->pid = c->process.pid;
      log(L_INFO, "Connection %d: Locked state %s (pid %d)", c->f.fd, c->locked_state->name, c->process.pid);
      close(c->f.fd);
      c->timeout.handler = ctrl_send_timeout;
      c->timeout.data = c;
      if (feeder_timeout)
	timer_add(&c->timeout, main_now + 1000*feeder_timeout);
    }
}

static void
ctrl_set_option(struct connection *c, uns *opt, byte *name)
{
  if (c->req_hdr.data_len != 4)
    {
      log(L_INFO, "Connection %d: Malformed request for setting option %s received", c->f.fd, name);
      ctrl_ship_reply(c, SHEPP_REPLY_UNKNOWN_REQ);
      return;
    }
  *opt = GET_U32(c->req_data);
  log(L_INFO, "Connection %d: Set option %s to %d", c->f.fd, name, *opt);
  ctrl_ship_reply(c, SHEPP_REPLY_OK);
  states_rethink();
}

void
ctrl_borrow_ack(void)
{
  struct connection *c = borrow_waiting_conn;
  if (c)
    ctrl_ship_reply(c, SHEPP_REPLY_OK);
}

static void
ctrl_return_state(struct connection *c)
{
  struct state *st = clist_tail(&state_list);
  ASSERT(st);
  if (st->phase == M_BORROWED)
    {
      char *verdict = states_do_scan_single(st);
      if (verdict)
	{
	  log(L_ERROR, "Connection %d: Attempted to return bad state %s: %s", c->f.fd, st->name, verdict);
	  ctrl_ship_reply(c, SHEPP_REPLY_RETURNING_BAD);
	  return;
	}
      if (c->req_hdr.type == SHEPP_REQ_RETURN_STATE)
	{
	  log(L_INFO, "Connection %d: Returning borrowed state %s (%s)", c->f.fd, st->name, phase_names[st->phase]);
	  if (st->phase == M_BORROWED)
	    st->phase = M_CLOSED;
	}
      else
	{
	  log(L_INFO, "Connection %d: Rolling back borrowed state %s", c->f.fd, st->name);
	  state_delete(st, 1);
	}
      ctrl_ship_reply(c, SHEPP_REPLY_OK);
      states_rethink();
    }
  else
    {
      log(L_INFO, "Connection %d: Attempting to return borrowed state, but there is none", c->f.fd);
      ctrl_ship_reply(c, SHEPP_REPLY_NO_BORROWED);
    }
}

static void
ctrl_unlock_states(struct connection *c)
{
  log(L_INFO, "Connection %d: Unlock requested%s", c->f.fd, (opt_locked ? "" : " (but already unlocked)"));
  if (opt_locked)
    {
      CLIST_FOR_EACH(struct state *, st, state_list)
	if (st->is_initial)
	  {
	    ASSERT(st->num_locks);
	    st->num_locks--;
	  }
      opt_locked = 0;
    }
  ctrl_ship_reply(c, SHEPP_REPLY_OK);
  states_rethink();
}

static void
ctrl_got_interim(struct main_file *fi)
{
  if (fi->rpos < fi->rlen)		/* EOF received */
    ctrl_error(fi, -1);
  else					/* Unwanted data received */
    ctrl_error(fi, -2);
}

static void
ctrl_got_packet(struct main_file *fi)
{
  struct connection *c = fi->data;
  if (fi->rpos < fi->rlen)		/* EOF received */
    {
      ctrl_error(fi, -1);
      return;
    }
  c->reply_hdr.id = c->req_hdr.id;
  c->reply_hdr.data_len = 0;
  switch (c->req_hdr.type)
    {
    case SHEPP_REQ_SEND_MODE:
      ctrl_switch_to_send_mode(c);
      break;
    case SHEPP_REQ_PING:
      ctrl_ship_reply(c, SHEPP_REPLY_PONG);
      break;
    case SHEPP_REQ_SET_CLEANUP:
      ctrl_set_option(c, &scheduled_cleanup, "cleanup");
      break;
    case SHEPP_REQ_SET_IDLE:
      ctrl_set_option(c, &opt_idle, "idle");
      break;
    case SHEPP_REQ_SET_PRIVATE:
      ctrl_set_option(c, &opt_private, "private");
      break;
    case SHEPP_REQ_SET_DELETE_OLD:
      ctrl_set_option(c, &delete_old_states, "delete-old");
      break;
    case SHEPP_REQ_LOCK_STATE:
      ctrl_lock_state(c);
      ASSERT(c->locked_state);
      log(L_INFO, "Connection %d: Locked state %s", c->f.fd, c->locked_state->name);
      ctrl_ship_reply(c, SHEPP_REPLY_OK);
      break;
    case SHEPP_REQ_BORROW_STATE:
    case SHEPP_REQ_BORROW_STATE_Q:
      if (borrow_requested)
	{
	  log(L_INFO, "Connection %d: State borrow requested, but already pending", c->f.fd);
	  ctrl_ship_reply(c, SHEPP_REPLY_IN_PROGRESS);
	}
      else
	{
	  log(L_INFO, "Conneciton %d: State borrow requested", c->f.fd);
	  borrow_requested = c->req_hdr.type - SHEPP_REQ_BORROW_STATE + 1;
	  borrow_waiting_conn = c;
	  states_rethink();
	  fi->read_done = ctrl_got_interim;
	  file_read(fi, c->reply_data, 1);
	  file_set_timeout(fi, 0);
	}
      break;
    case SHEPP_REQ_RETURN_STATE:
    case SHEPP_REQ_ROLLBACK_STATE:
      ctrl_return_state(c);
      break;
    case SHEPP_REQ_UNLOCK_STATES:
      ctrl_unlock_states(c);
      break;
    default:
      log(L_INFO, "Connection %d: Unknown request %08x received", c->f.fd, c->req_hdr.type);
      ctrl_ship_reply(c, SHEPP_REPLY_UNKNOWN_REQ);
    }
}

static void
ctrl_got_header(struct main_file *fi)
{
  struct connection *c = fi->data;
  if (fi->rpos < fi->rlen)		/* EOF received */
    {
      ctrl_error(fi, -1);
      return;
    }
  if (c->req_hdr.leader != SHEPP_LEADER)
    {
      ctrl_error(fi, -2);
      return;
    }
  xfree(c->req_data);
  c->req_data = NULL;
  if (c->req_hdr.data_len)
    {
      if (c->req_hdr.data_len > 0x10000)
	{
	  ctrl_error(fi, -2);
	  return;
	}
      c->req_data = xmalloc(c->req_hdr.data_len);
      fi->read_done = ctrl_got_packet;
      file_read(fi, c->req_data, c->req_hdr.data_len);
      file_set_timeout(fi, main_now + 1000*ctrl_timeout);
    }
  else
    ctrl_got_packet(fi);
}

static void
ctrl_unauthorized(int fd)
{
  /* Send an `unauthorized connect' reply if there is space in the buffer */
  if (fcntl(fd, F_SETFL, O_NONBLOCK) >= 0)
    {
      struct shepp_packet_hdr h;
      h.leader = SHEPP_LEADER;
      h.type = SHEPP_REPLY_NOT_AUTHORIZED;
      h.id = 0;
      h.data_len = 0;
      write(fd, &h, sizeof(h));
    }
}

static int
ctrl_new_conn(struct main_file *fi)
{
  struct sockaddr_in sa;
  int len = sizeof(sa);
  int fd = accept(fi->fd, (struct sockaddr *) &sa, &len);
  if (fd < 0)
    {
      if (errno != EAGAIN && errno != EWOULDBLOCK)
	log(L_ERROR, "accept: %m");
      return 0;
    }
  u32 ip = ntohl(sa.sin_addr.s_addr);
  if (!ipaccess_check(&ctrl_access_list, ip))
    {
      log(L_INFO, "Rejected connection from %s", inet_ntoa(sa.sin_addr));
      ctrl_unauthorized(fd);
      close(fd);
      return 1;
    }
  if (log_connections)
    log(L_INFO, "Accepted connection %d from %s", fd, inet_ntoa(sa.sin_addr));

  int one = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) < 0)
    log(L_ERROR, "Cannot request TCP keepalives: %m");

  struct connection *c = xmalloc_zero(sizeof(*c));
  struct main_file *f = &c->f;
  f->fd = fd;
  f->error_handler = ctrl_error;
  f->read_done = ctrl_got_header;
  f->write_done = ctrl_sent_reply;
  f->data = c;
  file_add(f);
  c->reply_hdr.leader = SHEPP_LEADER;
  c->reply_hdr.id = 0;
  static const byte welcome[] = { 4, '3', '3', '0', 'V' };
  memcpy(c->reply_data, welcome, sizeof(welcome));
  c->reply_hdr.data_len = sizeof(welcome);
  ctrl_ship_reply(c, SHEPP_REPLY_WELCOME);
  return 1;
}

void
ctrl_init(void)
{
  struct sockaddr_in sa;
  int one = 1;
  int sk;

  sk = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sk < 0)
    die("socket: %m");
  if (setsockopt(sk, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
    die("setsockopt(SO_REUSEADDR) failed: %m");

  sa.sin_family = AF_INET;
  sa.sin_port = htons(ctrl_port);
  sa.sin_addr.s_addr = htonl(INADDR_ANY);
  if (bind(sk, (struct sockaddr *) &sa, sizeof(sa)) < 0)
    die("bind: %m");

  if (listen(sk, ctrl_listen_queue) < 0)
    die("listen: %m");

  listening_socket.fd = sk;
  listening_socket.read_handler = ctrl_new_conn;
  file_add(&listening_socket);

  shepp_error_cb = my_shepp_error;
}

void
ctrl_reload(void)
{
  file_del(&listening_socket);
  close(listening_socket.fd);
  ctrl_init();
}
