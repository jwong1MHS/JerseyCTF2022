/*
 *	Sherlock Shepherd Daemon
 *
 *	(c) 2004--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/ipaccess.h"
#include "ucw/mainloop.h"
#include "ucw/fastbuf.h"
#include "ucw/stkstring.h"
#include "ucw/log.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/protocol.h"
#include "gather/shepherd/master.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

/*** Configuration ***/

char *log_name;
uns log_connections;
char **error_mails;
char **progress_mails;
uns trace_level;
uns ctrl_port = SHEPHERD_DEFAULT_PORT;
uns ctrl_listen_queue = 20;
uns ctrl_timeout = 100000;
clist ctrl_access_list;
clist prepare_hooks, finish_hooks, startup_hooks;
uns keep_old_states, keep_interim_states, keep_exported_states;
uns delete_old_states;
uns feeder_timeout;
u64 work_space = 1ULL << 62;
u64 min_free_space = 1ULL << 62;
u64 min_bucket_reserve;
uns bucket_watch_period = 60;
uns periodic_cleanup = ~0U;

static char *
hook_commit(struct hook *h)
{
  if (!h->cmd)
    return "Missing command definition";
  return NULL;
}

static struct cf_section hook_config = {\
  CF_TYPE(struct hook),
  CF_COMMIT(hook_commit),
  CF_ITEMS {
    CF_STRING("Command", PTR_TO(struct hook, cmd)),
    CF_END
  }
};

static struct cf_section shepmaster_config = {
  CF_ITEMS {
    CF_STRING("LogFile", &log_name),
    CF_UNS("LogConnections", &log_connections),
    CF_STRING_DYN("ErrorMail", &error_mails, CF_ANY_NUM),
    CF_STRING_DYN("ProgressMail", &progress_mails, CF_ANY_NUM),
    CF_UNS("Trace", &trace_level),
    CF_LIST("PrepareHook", &prepare_hooks, &hook_config),
    CF_LIST("FinishHook", &finish_hooks, &hook_config),
    CF_LIST("StartupHook", &startup_hooks, &hook_config),
    CF_UNS("Port", &ctrl_port),
    CF_UNS("ListenQueue", &ctrl_listen_queue),
    CF_UNS("CtrlTimeout", &ctrl_timeout),
    CF_LIST("Access", &ctrl_access_list, &ipaccess_cf),
    CF_UNS("KeepOldStates", &keep_old_states),
    CF_UNS("KeepInterimStates", &keep_interim_states),
    CF_UNS("KeepExportedStates", &keep_exported_states),
    CF_UNS("DeleteOldStates", &delete_old_states),
    CF_UNS("FeederTimeout", &feeder_timeout),
    CF_U64("WorkSpace", &work_space),
    CF_U64("MinFreeSpace", &min_free_space),
    CF_U64("MinBucketReserve", &min_bucket_reserve),
    CF_UNS("WatchPeriod", &bucket_watch_period),
    CF_UNS("PeriodicCleanup", &periodic_cleanup),
    CF_END
  }
};

/*** Other global structures ***/

volatile sig_atomic_t shutdown_requested;
static volatile sig_atomic_t reload_requested;

/*** Mail reports ***/

static int mail_enabled;
static off_t mail_pos;

static void
mail_get_pos(void)
{
  struct log_stream *ls = log_default_stream();
  if (!ls->name || !ls->name[0])
    mail_pos = -1;
  else
    mail_pos = lseek(2, 0, SEEK_END);
}

void
mail_log(int importance, byte *msg)
{
  struct log_stream *ls = log_default_stream();
  if (!mail_enabled || !ls->name || !ls->name[0] || mail_pos == (off_t)-1)
    return;

  byte *old_log_name = stk_strdup(ls->name);
  int switched = log_switch();
  if (importance >= 0 || switched)
    {
      char **addrs = (importance > 0) ? error_mails : progress_mails;
      if (DARY_LEN(addrs))
	{
	  char *addr = stk_strjoin((char**) addrs, DARY_LEN(addrs), ' ');
	  char *start = stk_printf("%d", (int)mail_pos + 1);
	  byte status[256];
	  format_status(status);
	  char *subj = stk_printf("%s%s%s", msg, (status[0] ? " " : ""), status);
	  run_command("bin/shep-mail", addr, subj, old_log_name, start, NULL);
	}
    }
  mail_get_pos();
}

static void
mail_die_hook(void)
{
  log_die_hook = NULL;
  mail_log(1, "FATAL ERROR");
}

static void
mail_init(void)
{
  log_file(log_name);
  log_switch_disable();
  mail_get_pos();
  log_die_hook = mail_die_hook;
  mail_enabled = 1;
}

void
mail_fork(void)
{
  mail_enabled = 0;
  log_fork();
  log_switch_enable();
}

/*** The scheduler ***/

static void
sighup_handler(int x UNUSED)
{
  if (!reload_requested)
    {
      msg(L_INFO | L_SIGHANDLER, "Reload requested");
      reload_requested = 1;
    }
}

static void
sigterm_handler(int x UNUSED)
{
  if (shutdown_requested < 2)
    {
      msg(L_INFO | L_SIGHANDLER, "Shutdown requested");
      shutdown_requested = 2;
    }
}

static void
sigquit_handler(int x UNUSED)
{
  if (shutdown_requested < 3)
    {
      msg(L_INFO | L_SIGHANDLER, "Hard shutdown requested");
      shutdown_requested = 3;
    }
}

static struct main_hook check_hook;

static void
reload(void)
{
  if (!cf_reload(DEFAULT_CONFIG))
    {
      states_reload();
      ctrl_reload();
      log(L_INFO, "Configuration reloaded");
    }
  else
    log(L_ERROR, "Attempt to reload configuration failed");
}

static int
do_check(struct main_hook *h UNUSED)
{
  if (reload_requested)
    {
      reload();
      reload_requested = 0;
    }
  return states_check();
}

enum {
  OPT_STATES = 0x1000,
  OPT_CLEANUP,
  OPT_IDLE,
  OPT_PRIVATE,
  OPT_REAP,
  OPT_CHECK_CONFIG,
  OPT_KEEP,
  OPT_LOCKED,
};

static struct option longopts[] = {
  CF_LONG_OPTS
  { "states",		0, 0, OPT_STATES },
  { "cleanup",		0, 0, OPT_CLEANUP },
  { "idle",		0, 0, OPT_IDLE },
  { "private",		0, 0, OPT_PRIVATE },
  { "reap",		0, 0, OPT_REAP },
  { "check-config",	0, 0, OPT_CHECK_CONFIG },
  { "keep",		0, 0, OPT_KEEP },
  { "locked",		0, 0, OPT_LOCKED },
  { NULL,		0, 0, 0 }
};

uns opt_idle;
uns opt_private;
uns opt_reap;
uns opt_keep;
uns opt_locked;

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shepherd [<options>]\n\
\n\
Options:\n\
" CF_USAGE "\
--check-config\t\tCheck configuration and exit\n\
--cleanup\t\tPerform cleanup of database as soon as possible\n\
--idle\t\t\tDo not run reapers, but still accept connections\n\
--keep\t\t\tDo not delete clean states which existed on daemon startup\n\
--locked\t\tLock existing states and avoid cleanups until unlocked manually\n\
--private\t\tRefuse bucket file sending requests\n\
--reap\t\t\tWhen started in the middle of reaping, continue\n\
--states\t\tList all known states and exit\n\
");
  exit(1);
}

int
main(int argc, char **argv)
{
  uns opt_states = 0, opt_check_config = 0;

  log_init(argv[0]);
  cf_declare_section("ShepMaster", &shepmaster_config, 0);

  setproctitle_init(argc, argv);

  int opt;
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS, longopts, NULL)) > 0)
    switch (opt)
      {
      case OPT_STATES:
	opt_states++;
	break;
      case OPT_CHECK_CONFIG:
	opt_check_config++;
	break;
      case OPT_CLEANUP:
	scheduled_cleanup++;
	break;
      case OPT_IDLE:
	opt_idle++;
	break;
      case OPT_PRIVATE:
	opt_private++;
	break;
      case OPT_REAP:
	opt_reap++;
	break;
      case OPT_KEEP:
	opt_keep++;
	break;
      case OPT_LOCKED:
	opt_locked++;
	break;
      default:
	usage();
      }
  if (optind != argc)
    usage();

  if (opt_states)
    {
      states_scan();
      states_list();
      return 0;
    }
  if (opt_check_config)
    return 0;

  setproctitle("shepherd: pondering");
  mail_init();
  setpgid(0, 0);
  log(L_INFO, "Shepherd starting.");

  struct sigaction sa;
  bzero(&sa, sizeof(sa));
  sa.sa_handler = SIG_IGN;
  sa.sa_flags = SA_RESTART;
  sigaction(SIGPIPE, &sa, NULL);
  sa.sa_handler = sigterm_handler;
  sigaction(SIGTERM, &sa, NULL);
  sigaction(SIGINT, &sa, NULL);
  sa.sa_handler = sigquit_handler;
  sigaction(SIGQUIT, &sa, NULL);
  sa.sa_handler = sighup_handler;
  sigaction(SIGHUP, &sa, NULL);

  main_init();
  check_hook.handler = do_check;
  hook_add(&check_hook);

  states_init();
  states_scan();
  states_look();
  ctrl_init();
  run_startup_hooks();
  states_kick();

  main_loop();

  signal(SIGTERM, SIG_IGN);
  kill(0, SIGTERM);	/* Kill the remaining processes in the current group */
  states_cleanup();
  log(L_INFO, "Shepherd shut down.");
  mail_log(0, "Shut down");
  return 0;
}
