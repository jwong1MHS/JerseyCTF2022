/*
 *	Sherlock Shepherd Daemon -- Managing States
 *
 *	(c) 2004--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "sherlock/bucket.h"
#include "ucw/mainloop.h"
#include "ucw/mempool.h"
#include "ucw/lfs.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/master.h"

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <alloca.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/mount.h>
#include <sys/statvfs.h>

clist state_list;
uns scheduled_cleanup;
byte *fn_feedback, *fn_feedback_run, *fn_feedback_old;

const byte *phase_names[] = {
  "incomplete", "closed", "prepare", "plan", "reap", "cork", "corked", "merge", "feedback", "equiv", "select",
  "record", "sort", "finish", "cleanup", "recover", "rollback", "borrowed"
};

static struct main_process process;
static struct main_timer bucket_watch_timer;
static uns sigterm_sent;
static uns low_on_space;
static uns low_on_bucket_reserve;
static uns cycles_since_cleanup;

static struct state *
state_new(byte *name)
{
  int nlen = strlen(name);
  struct state *st = xmalloc_zero(sizeof(*st) + nlen + 1 + nlen + 64);
  memcpy(st->name, name, nlen+1);
  st->fn = st->name + nlen + 1;
  return st;
}

static inline struct state *
state_current(void)
{
  struct state *st = clist_tail(&state_list);
  ASSERT(st);
  return st;
}

void
state_delete(struct state *st, int delete_files)
{
  if (delete_files)
    delete_state(st->name);
  clist_remove(&st->n);
  xfree(st);
}

byte *
state_fn(struct state *st, byte *file)
{
  sprintf(st->fn, "%s/%s", st->name, file);
  return st->fn;
}

static void
state_touch_file(struct state *st, byte *file)
{
  byte *fn = state_fn(st, file);
  int fd = open(fn, O_WRONLY | O_CREAT, 0666);
  if (fd < 0)
    die("Cannot create %s: %m", fn);
  close(fd);
}

char *
states_do_scan_single(struct state *st)
{
  byte buf[256];
  FILE *f;
  enum phase phase;

  f = fopen(state_fn(st, "control"), "r");
  if (!f)
    return "No control file";
  if (!fgets(buf, sizeof(buf), f))
    buf[0] = 0;
  fclose(f);
  byte *e = strchr(buf, '\n');
  if (!e || e == buf)
    return "Invalid control file";
  *e = 0;
  for (phase=M_CLOSED; phase<M_MAX; phase++)
    if (!strcmp(phase_names[phase], buf))
      break;
  if (phase >= M_MAX)
    return "!Unknown phase specified in control file";

  struct state_params par;
  e = state_params_try_read(state_fn(st, "params"), &par);
  if (e)
    return cf_printf("Error reading parameters: %s", e);
  if (par.format_version != PARAMS_VERSION_CURRENT)
    return cf_printf("!Unknown version %08x, need to run shep-upgrade", par.format_version);

  if (access(state_fn(st, "index"), R_OK))
    return "Missing index file";
  if (access(state_fn(st, "sites"), R_OK))
    return "Missing sites file";
#ifdef CONFIG_AREAS
  if (access(state_fn(st, "areas"), R_OK))
    return "Missing areas file";
#endif
  if (st->phase == M_REAP || st->phase == M_CORK || st->phase == M_CORKED)
    {
      if (access(state_fn(st, "contrib"), R_OK))
	return "!Missing contrib file";
      if (st->phase != M_CORKED && access(state_fn(st, "journal"), R_OK))
	return "!Missing journal file";
    }

  st->phase = phase;
  return NULL;
}

static void
states_scan_single(char *name)
{
  struct state *st = state_new(name);
  char *err;

  if (err = states_do_scan_single(st))
    {
      if (err[0] == '!')
	die("Fatal inconsistency found in state %s: %s", st->name, err+1);
      else
	{
	  log(L_ERROR, "State %s: %s", st->name, err);
	  st->phase = M_INCOMPLETE;
	}
    }
  cnode *before = state_list.head.next;
  while (before != &state_list.head && strcmp(((struct state *)before)->name, st->name) < 0)
    before = before->next;
  clist_insert_before(&st->n, before);
}

void
states_scan(void)
{
  DIR *d = opendir(state_dir);
  if (!d)
    die("Unable to scan %s: %m", state_dir);
  clist_init(&state_list);
  struct dirent *e;
  struct stat st;
  while (e = readdir(d))
    {
      int l = strlen(e->d_name);
      if (l != 15)
	continue;
      int ok = 1;
      while (l--)
	if (!((l == 8 && e->d_name[l] == '-') ||
	      (l != 8 && e->d_name[l] >= '0' && e->d_name[l] <= '9')))
	  ok = 0;
      if (!ok)
	continue;
      byte *n = alloca(strlen(state_dir) + 1 + strlen(e->d_name) + 1);
      sprintf(n, "%s/%s", state_dir, e->d_name);
      if (stat(n, &st) < 0)
	log(L_ERROR, "Cannot stat %s: %m", n);
      else if (!S_ISDIR(st.st_mode))
	log(L_ERROR, "State %s: not a directory", n);
      else
	states_scan_single(n);
    }
  closedir(d);
}

void
states_list(void)
{
  struct state *st;
  CLIST_WALK(st, state_list)
    printf("%s (%s)\n", st->name, phase_names[st->phase]);
}

static void
states_update_link(byte *name, struct state *st)
{
  byte n[strlen(state_dir) + 1 + strlen(name) + 1];
  sprintf(n, "%s/%s", state_dir, name);
  if (unlink(n) < 0 && errno != ENOENT)
    die("unlink(%s): %m", n);
  if (st)
    {
      byte *stn = strrchr(st->name, '/') + 1;
      if (symlink(stn, n) < 0)
	die("symlink(%s,%s): %m", stn, n);
    }
}

static void
states_fix(struct state *st)
{
  switch (st->phase)
    {
    case M_INCOMPLETE:
    case M_PREPARE:
    case M_PLAN:
    case M_MERGE:
    case M_FEEDBACK:
    case M_EQUIV:
    case M_SELECT:
    case M_FINISH:
    case M_RECORD:
    case M_SORT:
      log(L_INFO, "Rolling back open state %s (%s)", st->name, phase_names[st->phase]);
      state_delete(st, 1);
      break;

    case M_CORK:
    case M_CORKED:
      log(L_INFO, "Re-opening state %s (%s)", st->name, phase_names[st->phase]);
      break;

    case M_REAP:
    case M_ROLLBACK: ;
      /* Before trying to roll-back to a checkpoint, we need to check that there is one */
      int ok = 0;
      int fd = ucw_open(state_fn(st, "checkpoints"), O_RDONLY);
      if (fd >= 0)
	{
	  if (ucw_seek(fd, 0, SEEK_END) >= (ucw_off_t) sizeof(struct checkpoint_entry))
	    ok = 1;
	  close(fd);
	}
      if (ok)
	{
	  log(L_INFO, "Rolling back open state %s (%s) to nearest checkpoint", st->name, phase_names[st->phase]);
	  st->phase = M_ROLLBACK;
	}
      else
	{
	  log(L_INFO, "Rolling back open state %s (%s, no checkpoint)", st->name, phase_names[st->phase]);
	  state_delete(st, 1);
	}
      break;

    case M_CLEANUP:
    case M_RECOVER:
      log(L_INFO, "Recovering unsynced state %s (%s)", st->name, phase_names[st->phase]);
      st->phase = M_RECOVER;
      break;

    case M_MAX:
    case M_CLOSED:
    case M_BORROWED:
      ASSERT(0);
    }
}

static void NONRET
states_error(char *msg)
{
  die("Inconsistent states: %s. Needs fixing manually.", msg);
}

void
states_look(void)
{
  struct state *st;
  struct state *last_closed = NULL, *last_open = NULL, *last_corked = NULL;

  CLIST_WALK(st, state_list)
    {
      log(L_INFO, "Discovered state %s (%s)", st->name, phase_names[st->phase]);
      if (st->phase == M_CLOSED)
	{
	  last_closed = st;
	  if (last_open)
	    states_error("Closed state follows open state");
	  st->is_initial = 1;
	  if (opt_locked)
	    st->num_locks++;
	}
      else if (st->phase == M_CORKED)
	{
	  if (last_open)
	    states_error("Corked state follows open state");
	  last_corked = st;
	}
      else if (st->phase == M_BORROWED)
	states_error("Borrowed state exists");
      else
	{
	  if (last_open)
	    states_error("Multiple open states found");
	  last_open = st;
	}
    }
  if (!last_closed)
    die("Inconsistent states: No closed state found. Needs fixing manually.");
  if (!run_command("bin/buckettool", "-q", NULL))
    {
      log(L_ERROR, "Inconsistent bucket file found, running buckettool to fix it");
      if (!run_command("bin/buckettool", "-F", NULL))
	die("Inconsistent bucket file needs manual intervention");
    }
  if (!last_corked && (!last_open || last_open->phase != M_CORK))
    opt_reap = 0;
  if (last_open)
    states_fix(last_open);
  states_update_link("closed", last_closed);
  states_update_link("current", state_current());
}

static void
states_audit(void)
{
  struct state *st = state_current();
  ASSERT(st->phase == M_CLOSED);
  states_update_link("closed", st);
  states_update_link("current", NULL);
  st = clist_prev(&state_list, &st->n);
  uns num_closed = 0;
  uns num_corked = 0;
  uns num_exported = 0;
  while (st)
    {
      struct state *prev = clist_prev(&state_list, &st->n);
      int keep;
      switch (st->phase)
	{
	case M_CLOSED:
	  num_closed++;
	  keep = (num_closed <= keep_old_states);
	  if (st->export_count)
	    {
	      num_exported++;
	      keep |= (num_exported <= keep_exported_states);
	    }
	  break;
	case M_CORKED:
	  num_corked++;
	  keep = (num_corked <= keep_interim_states);
	  break;
	default:
	  ASSERT(0);
	}
      if (!st->num_locks && (!keep || scheduled_cleanup))
	{
	  int really = (st->phase == M_CLOSED && delete_old_states > 1||
			st->phase == M_CORKED && delete_old_states);
	  really = really && !(st->is_initial && opt_keep);
	  log(L_INFO, "%s old state %s (%s)", (really ? "Deleting" : "Dropping"), st->name, phase_names[st->phase]);
	  state_delete(st, really);
	}
      st = prev;
    }

  if (trace_level > 1)
    {
      log(L_DEBUG, "Known states:");
      CLIST_WALK(st, state_list)
	log(L_DEBUG, "\t%s (%s) locked=%d exports=%d", st->name, phase_names[st->phase], st->num_locks, st->export_count);
    }
}

void
format_status(byte *p)
{
  if (opt_idle)
    p += sprintf(p, "[idle]");
  if (opt_private)
    p += sprintf(p, "[priv]");
  if (opt_locked)
    p += sprintf(p, "[lock]");
  if (scheduled_cleanup)
    p += sprintf(p, "[over]");
  if (borrow_requested)
    p += sprintf(p, "[brrw]");
  if (low_on_space)
    p += sprintf(p, "[lowspace]");
  if (low_on_bucket_reserve)
    p += sprintf(p, "[lowreserve]");
  if (shutdown_requested)
    p += sprintf(p, "[shutdown]");
  *p = 0;
}

static void
recalc_title(void)
{
  byte status[256];
  format_status(status);
  setproctitle("shepherd: %s %s", phase_names[state_current()->phase], status);
}

static void
state_sync(struct state *st)
{
  static byte * const files[] = { "index", "sites", "journal", "contrib", "areas" };
  for (uns i=0; i<ARRAY_SIZE(files); i++)
    {
      byte *name = state_fn(st, files[i]);
      int fd = open(name, O_RDWR);
      if (fd >= 0)
	{
	  if (fsync(fd) < 0)
	    log(L_ERROR, "Cannot sync %s: %m", name);
	  close(fd);
	}
    }
  sync_dir(st->name);
}

static void
state_set_phase(struct state *st, enum phase phase)
{
  TRACE("### State %s: entering %s phase ###", st->name, phase_names[phase]);
  state_sync(st);
  st->phase = phase;
  recalc_title();
  byte *fn = state_fn(st, "control");
  int fd = open(fn, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd < 0)
    die("Cannot open %s: %m", fn);
  int l = strlen(phase_names[phase]);
  if (write(fd, phase_names[phase], l) != l || write(fd, "\n", 1) != 1)
    die("Error writing %s: %m", fn);
  if (fsync(fd) < 0)
    log(L_ERROR, "Cannot sync %s: %m", fn);
  close(fd);
}

static struct state *
state_clone(struct state *old)
{
  byte *name = create_new_state();
  struct state *new = state_new(name);
  xfree(name);
  clone_state(old->name, new->name);
  state_set_phase(new, old->phase);
  clist_add_tail(&state_list, &new->n);
  log(L_INFO, "### Created new state %s", new->name);
  states_update_link("current", new);
  return new;
}

/*** The state machine ***/

static void
action_run_command(char *cmd, ...)
{
  va_list args, args2;
  va_start(args, cmd);
  byte echo[256];
  va_copy(args2, args);
  echo_command_v(echo, sizeof(echo), cmd, args2);
  va_end(args2);
  TRACE("%%%%%% Running %s", echo);
  if (!process_fork(&process))
    {
      file_close_all();
      exec_command_v(cmd, args);
    }
}

static void
action_borrow_ack(struct state *st)
{
  st = state_clone(st);
  state_set_phase(st, M_BORROWED);
  log(L_INFO, "Borrowed state %s", st->name);
  ctrl_borrow_ack();
  mail_log(0, "State borrowed");
}

static void
action_start(void)
{
  struct state *st = state_current();
  switch (st->phase)
    {
    case M_CLOSED:
      states_audit();
      if (shutdown_requested)
	return;
      if (borrow_requested)
	{
	  action_borrow_ack(st);
	  return;
	}
      cycles_since_cleanup++;
      if (!scheduled_cleanup && cycles_since_cleanup > periodic_cleanup)
	{
	  log(L_INFO, "Scheduling periodic cleanup.");
	  scheduled_cleanup = 1;
	}
      mail_log((st->already_sent_report ? -1 : 0), "Status report");
      st->already_sent_report = 1;
      if (scheduled_cleanup && !clist_prev(&state_list, &st->n) && !st->num_locks)
	{
	  st = state_clone(st);
	  state_set_phase(st, M_CLEANUP);
	  states_update_link("closed", NULL);
	  states_update_link("current", NULL);
	  action_run_command("bin/shep-cleanup", st->name, NULL);
	  break;
	}
      if (opt_idle || low_on_space || low_on_bucket_reserve)
	return;
      st = state_clone(st);
      state_set_phase(st, M_PREPARE);
      st->current_hook = clist_head(&prepare_hooks);
      /* fall-thru */
    case M_PREPARE:
      if (st->current_hook)
	{
	  action_run_command("/bin/sh", "-c", st->current_hook->cmd, NULL);
	  break;
	}
    plan_again:
      state_set_phase(st, M_PLAN);
      /* fall-thru */
    case M_PLAN:
      action_run_command("bin/shep-plan", st->name, NULL);
      break;
    case M_REAP:
      action_run_command("bin/shep-reap", st->name, NULL);
      break;
    case M_CORK:
      action_run_command("bin/shep-cork", st->name, NULL);
      break;
    case M_CORKED:
      if (shutdown_requested)
	return;
      if (borrow_requested > 1)
	{
	  action_borrow_ack(st);
	  return;
	}
      st = state_clone(st);
      if (opt_reap)
	{
	  opt_reap = 0;
	  goto plan_again;
	}
      state_set_phase(st, M_MERGE);
      /* fall-thru */
    case M_MERGE:
      action_run_command("bin/shep-merge", st->name, NULL);
      break;
    case M_FEEDBACK:
      rename(fn_feedback, fn_feedback_run);
      if (!access(fn_feedback_run, R_OK))
	{
	  action_run_command("bin/shep-feedback", st->name, fn_feedback_run, NULL);
	  break;
	}
      state_set_phase(st, M_EQUIV);
      /* fall-thru */
    case M_EQUIV:
      action_run_command("bin/shep-equiv", st->name, NULL);
      break;
    case M_SELECT:
      action_run_command("bin/shep-select", st->name, NULL);
      break;
    case M_RECORD:
      action_run_command("bin/shep-record", st->name, NULL);
      break;
    case M_SORT:
      action_run_command("bin/shep-sort", st->name, NULL);
      break;
    case M_FINISH:
      if (st->current_hook)
	{
	  action_run_command("/bin/sh", "-c", st->current_hook->cmd, NULL);
	  break;
	}
      state_set_phase(st, M_CLOSED);
      if (rename(fn_feedback_run, fn_feedback_old) < 0 && errno != ENOENT)
	log(L_ERROR, "Error renaming %s to %s: %m", fn_feedback_run, fn_feedback_old);
      action_start();
      break;
    case M_RECOVER:
      states_update_link("closed", NULL);
      states_update_link("current", NULL);
      action_run_command("bin/shep-recover", st->name, "cleanup", NULL);
      break;
    case M_ROLLBACK:
      states_update_link("current", NULL);
      action_run_command("bin/shep-recover", st->name, "reap", NULL);
      break;
    case M_CLEANUP:
    case M_INCOMPLETE:
    case M_BORROWED:
    case M_MAX:
      ASSERT(0);
    }
}

static void
action_done(void)
{
  struct state *st = state_current();
  switch (st->phase)
    {
    case M_CLOSED:
      ASSERT(0);
    case M_PREPARE:
      ASSERT(st->current_hook);
      st->current_hook = clist_next(&prepare_hooks, &st->current_hook->n);
      break;
    case M_PLAN:			/* -> ST_REAP */
    case M_REAP:			/* -> ST_CORK */
    case M_CORK:			/* -> ST_CORKED */
    case M_MERGE:			/* -> ST_FEEDBACK */
    case M_FEEDBACK:			/* -> ST_EQUIV */
    case M_EQUIV:			/* -> ST_SELECT */
    case M_SELECT:			/* -> ST_RECORD */
      state_set_phase(st, st->phase+1);
      break;
    case M_CORKED:
      ASSERT(0);
    case M_RECORD:
      unlink(state_fn(st, "contrib"));
      if (auto_sort_index)
        {
          state_set_phase(st, M_SORT);
	  break;
	}
      /* fall-thru */
    case M_SORT:
      state_set_phase(st, M_FINISH);
      st->current_hook = clist_head(&finish_hooks);
      break;
    case M_FINISH:
      ASSERT(st->current_hook);
      st->current_hook = clist_next(&finish_hooks, &st->current_hook->n);
      break;
    case M_CLEANUP:
      state_set_phase(st, M_CLOSED);
      scheduled_cleanup = 0;
      cycles_since_cleanup = 0;
      low_on_bucket_reserve = 0;
      timer_add(&bucket_watch_timer, main_now + 1000*bucket_watch_period);
      break;
    case M_RECOVER:
      {
	/* shep-merge needs the contrib file, so make sure it exists */
	int fd = open(state_fn(st, "contrib"), O_RDWR | O_CREAT, 0666);
	close(fd);
	state_set_phase(st, M_CORKED);
	states_update_link("current", st);
	break;
      }
    case M_ROLLBACK:
      state_set_phase(st, M_CORK);
      break;
    case M_INCOMPLETE:
    case M_MAX:
    case M_BORROWED:
      ASSERT(0);
    }
}

static void
action_process_finished(struct main_process *pr)
{
  if (pr->status_msg[0])
    {
      if (pr->status >= 0 && WIFSIGNALED(pr->status) && WTERMSIG(pr->status) == SIGTERM && sigterm_sent)
	{
	  struct state *st = state_current();
	  log(L_INFO, "Subprocess %d killed by SIGTERM", pr->pid);
	  if (st->phase != M_REAP)
	    return;
	  /*
	   * shep-reap traps SIGTERM and exits normally, but we can terminate it before
	   * it gets to setting up the signal handler. In such cases, just ignore the signal
	   * and make sure that the output files exist.
	   */
	  state_touch_file(st, "contrib");
	  state_touch_file(st, "journal");
	}
      else
	die("Subprocess %d %s", pr->pid, pr->status_msg);
    }
  sigterm_sent = 0;
  pr->pid = 0;
  action_done();
  if (shutdown_requested < 3)
    action_start();
}

int
states_check(void)
{
  struct state *st = state_current();
  if (shutdown_requested || ((low_on_space || low_on_bucket_reserve || borrow_requested || opt_idle) && st->phase == M_REAP))
    {
      if (process.pid)
	{
	  if (!sigterm_sent)
	    {
	      switch (st->phase)
		{
		case M_PREPARE:
		case M_PLAN:
		case M_CORK:
		case M_MERGE:
		case M_FEEDBACK:
		case M_EQUIV:
		case M_SELECT:
		case M_FINISH:
		  /* In these cases, we can terminate if we really want */
		  if (shutdown_requested < 3)
		    break;
		  /* fall-thru */
		case M_REAP:
		  log(L_INFO, "Sending SIGTERM to subprocess %d", process.pid);
		  kill(process.pid, SIGTERM);
		  sigterm_sent++;
		  break;
		default:
		  ;
		}
	    }
	}
      else if ((shutdown_requested && clist_empty(&main_process_list)) || shutdown_requested > 2)
	return HOOK_SHUTDOWN;
    }
  return HOOK_IDLE;
}

void
states_rethink(void)
{
  struct state *st = state_current();
  if ((st->phase == M_CLOSED || st->phase == M_CORKED) && !process.pid)
    action_start();
  else
    states_check();
  recalc_title();
}

static void
bucket_watch(struct main_timer *tm)
{
  struct statvfs sf;
  u64 free_space;

  if (statvfs(".", &sf) < 0)
    {
      log(L_ERROR, "statfs on current directory failed: %m");
      free_space = 0;
    }
  else
    free_space = (u64)sf.f_bsize * sf.f_bavail;
  TRACE2("Free disk space sampled: %dM", (int)(free_space >> 20));
  if (free_space < min_free_space)
    {
      if (!low_on_space)
	{
	  log(L_ERROR, "Disk space dropped below the safety limit, cannot dare continue.");
	  mail_log(1, "ERROR: Short on disk space");
	  low_on_space = 1;
	  states_check();
	  recalc_title();
	}
    }
  else if (low_on_space)
    {
      log(L_INFO, "Recovered from low disk space, continuing");
      low_on_space = 0;
      states_rethink();
      recalc_title();
    }

  if (!scheduled_cleanup)
    {
      u64 size = obuck_get_pos(obuck_predict_last_oid(&bucket_file));
      TRACE2("Bucket file size sampled: %dM", (int)(size >> 20));
      if (size > bucket_file.max_size - min_bucket_reserve)
        {
	  low_on_bucket_reserve = 1;
	  scheduled_cleanup = 1;
	  log(L_ERROR, "Bucket file size exceeded the safety limit, scheduling cleanup.");
	  recalc_title();
	}
      if (size > expected_bucket_file_size + work_space)
	{
	  scheduled_cleanup = 1;
	  log(L_INFO, "Bucket file too large, scheduling cleanup.");
	  recalc_title();
	}
    }

  timer_add(tm, main_now + 1000*bucket_watch_period);
}

void
run_startup_hooks(void)
{
  struct hook *h;
  CLIST_WALK(h, startup_hooks)
    {
      TRACE("%%%%%% Running %s", h->cmd);
      if (!run_command("/bin/sh", "-c", h->cmd, NULL))
	die("Startup hook failed");
    }
}

void
states_init(void)
{
  bucket_open(0);

  fn_feedback = mp_strcat(cf_pool, db_dir, "/feedback");
  fn_feedback_run = mp_strcat(cf_pool, fn_feedback, ".run");
  fn_feedback_old = mp_strcat(cf_pool, fn_feedback, ".old");

  process.handler = action_process_finished;

  if ((u64)bucket_file.max_size <= min_bucket_reserve)
    die("Too large ShepMaster.MinBucketReserve");
}

void
states_kick(void)
{
  bucket_watch_timer.handler = bucket_watch;
  bucket_watch(&bucket_watch_timer);

  action_start();
  recalc_title();
}

void
states_cleanup(void)
{
  bucket_close();
}

void
states_reload(void)
{
  states_cleanup();
  states_init();
}
