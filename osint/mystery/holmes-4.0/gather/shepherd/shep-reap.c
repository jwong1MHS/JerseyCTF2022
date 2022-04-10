/*
 *	Sherlock Shepherd Daemon -- Gathering of URL's
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2004 Robert Spalek <robert@ucw.cz>
 *	(c) 2006--2009 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "gather/gather.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/reap.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <sys/time.h>

volatile sig_atomic_t shut_down;
struct buck2obj_buf *global_buck_buf;
struct mempool *global_buck_pool;

/*** Configuration ***/

static uns stat_period;
static uns checkpoint_period;
char *url_log_file;
char *current_state;
uns prefetch_limit = 10;
uns prefetch_threads;
uns robots_cache_threshold = 1;

static struct cf_section reap_config = {
  CF_ITEMS {
    CF_STRING("URLLogFile", &url_log_file),
    CF_UNS("StatPeriod", &stat_period),
    CF_UNS("CheckpointPeriod", &checkpoint_period),
    CF_UNS("PrefetchQueueSize", &prefetch_limit),
    CF_UNS("PrefetchThreads", &prefetch_threads),
    CF_UNS("RobotsCacheThreshold", &robots_cache_threshold),
    CF_END
  }
};

/*** Checkpointing ***/

static struct fastbuf *checkpoint_f;

static void
checkpoint(struct main_timer *t)
{
  struct checkpoint_entry ce;
  log(L_INFO, "Checkpointing");
  ce.time = main_now_seconds;
  ce.buckets_pos = jobs_checkpoint_buckets();
  ce.journal_pos = jobs_checkpoint_journal();
  ce.contrib_pos = contrib_checkpoint();
  ce.urls_pos = jobs_checkpoint_urls();
  bwrite(checkpoint_f, &ce, sizeof(ce));
  bfilesync(checkpoint_f);
  sync_dir(current_state);
  log(L_INFO, "Checkpoint written");
  timer_add(t, main_now + checkpoint_period * 1000);
}

static void
checkpoint_init(void)
{
  checkpoint_f = bopen(state_file_name(current_state, "checkpoints"), O_WRONLY | O_CREAT | O_TRUNC, sizeof(struct checkpoint_entry));
  static struct main_timer t;
  t.handler = checkpoint;
  timer_add(&t, main_now + checkpoint_period * 1000);
}

static void
checkpoint_cleanup(void)
{
  bclose(checkpoint_f);
}

/*** Signals ***/

static void
sigterm_handler(int s UNUSED)
{
  if (!shut_down)
    {
      msg(L_INFO | L_SIGHANDLER, "Shutdown requested");
      shut_down = 1;
    }
}

static void
setup_signals(void)
{
  struct sigaction sa;
  bzero(&sa, sizeof(sa));
  sa.sa_handler = SIG_IGN;
  sa.sa_flags = SA_RESTART;
  if (sigaction(SIGPIPE, &sa, NULL))
    die("Cannot establish SIGPIPE handler: %m");
  sa.sa_handler = sigterm_handler;
  if (sigaction(SIGTERM, &sa, NULL))
    die("Cannot establish SIGTERM handler: %m");
  if (sigaction(SIGINT, &sa, NULL))
    die("Cannot establish SIGINT handler: %m");
}

void
reset_signals(void)
{
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);
  signal(SIGPIPE, SIG_DFL);
}

/*** Statistics ***/

static s64 avg_num_active;
static s64 avg_num_prefetched;
static s64 avg_num_ready;

static double
get_avg(s64 sum, int period)
{
  return (period > 0) ? (double)sum / period : 0.0;
}

static void
update_avg(s64 *sum, uns val, int period)
{
  *sum += period * val;
}

static double
switch_avg(s64 *sum, int period)
{
  double r = get_avg(*sum, period);
  *sum = 0;
  return r;
}

static double
get_idle(int idle, int period)
{
  double r = (period > 0 && idle > 0) ? 100.0 * idle / period : 0.0;
  r = MIN(r, 100.0);
  return r;
}

static timestamp_t start_time, last_update_time, last_stat_time, last_stat_idle;
static uns total_jobs_ok, last_urls;

static void
stats_update(void)
{
  int period = main_now - last_update_time;
  update_avg(&avg_num_active, num_active_jobs, period);
  update_avg(&avg_num_prefetched, jobs_prefetched_count(), period);
  update_avg(&avg_num_ready, queue_ready_count(), period);
  last_update_time = main_now;
}

static void
log_continuous_stats(void)
{
  int period = main_now - last_stat_time;

  log(L_INFO, "Statistics for last %d seconds:", period / 1000);
  uns total = 0;
#ifdef CONFIG_SHEPHERD_CHILDREN
  total += child_stats();
#endif
  total_jobs_ok += total;

  uns urls = initial_plan_entry_count - current_plan_entry_count;
  uns u = urls - last_urls;
  last_urls = urls;
  uns qk = plan_count_qkeys();

  log(L_INFO, "     Total: %5d jobs ok (%.2f jobs/sec), %d URLs (%.2f URLs/sec), %.0f%% idle",
      total, get_avg(total, period), u, get_avg(u, period), get_idle(main_idle_time - last_stat_idle, period));
  log(L_INFO, "     Plan:  %.1f/%.1f/%.1f avg active/prefetched/ready, %d keys remain",
      switch_avg(&avg_num_active, period), switch_avg(&avg_num_prefetched, period), switch_avg(&avg_num_ready, period), qk);

  last_stat_time = main_now;
  last_stat_idle = main_idle_time;
}

static void
log_final_stats(void)
{
  int period = main_now - start_time;
  uns urls = initial_plan_entry_count - current_plan_entry_count;

  log(L_INFO, "Processed %d jobs in %d sec (%.2f jobs/sec)", total_jobs_ok, period / 1000, get_avg(total_jobs_ok, period));
  log(L_INFO, "Gathered %d of %d planned URLs (%.2f URLs/sec): %d new, %d refreshed, %d anticipated refresh",
      urls, initial_plan_entry_count, get_avg(urls, period), downloaded_count, refreshed_count, anticipated_count);
  log(L_INFO, "Idle for %.0f%% of time", get_idle(main_idle_time, period));
}

static void
stats_timer_handler(struct main_timer *t)
{
  log_continuous_stats();
  timer_add(t, main_now + stat_period * 1000);
}

static struct main_timer stats_timer = { .handler = stats_timer_handler };

static void
stats_init(void)
{
  start_time = last_stat_time = last_update_time = main_now;
  timer_add(&stats_timer, main_now + stat_period * 1000);
}

static void
stats_done(void)
{
  log_continuous_stats();
  log_final_stats();
}

/*** Main loop ***/

static void
timer_empty(struct main_timer *t)
{
  timer_del(t);
}

static void
timer_shutdown(struct main_timer *t)
{
  shut_down = 1;
  timer_del(t);
}

static struct main_timer loop_queue_timer = { .handler = timer_empty };
static struct main_timer loop_shutdown_timer = { .handler = timer_shutdown };

static int
loop_hook_handler(struct main_hook *h UNUSED)
{
  ucw_time_t wait_seconds = 100000;
  int have_nodes = queue_time_step(&wait_seconds);
  if (!num_active_jobs && !have_nodes)
    {
      log(L_INFO, "Plan exhausted.");
      shut_down = 1;
    }
  if (have_nodes)
    timer_add(&loop_queue_timer, main_now + wait_seconds * 1000);
  stats_update();
  return (shut_down && !num_active_jobs) ? HOOK_SHUTDOWN : HOOK_IDLE;
}

static struct main_hook loop_hook = { .handler = loop_hook_handler };

static void
start_loop(void)
{
  stats_init();

  uns child_inited = 0, conn_inited = 0;
#ifdef CONFIG_SHEPHERD_CHILDREN
  child_inited = child_init();
#endif
  if (!child_inited && !conn_inited)
    die("Please, configure ShepReap.MaxChildren or a list of reapers");

  setup_signals();
  hook_add(&loop_hook);
  timer_add(&loop_shutdown_timer, main_now + reap_cycle * 1000);

  main_loop();

  stats_done();
#ifdef CONFIG_SHEPHERD_CHILDREN
  child_cleanup();
#endif
}

static void
null_loop(void)
{
  setup_signals();
  log(L_INFO, "Nothing to gather, sleeping for %d seconds.", null_cycle);
  sleep(null_cycle);
  log(L_INFO, "Null cycle finished.");
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-reap [<config-options>] <state>\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  cf_declare_section("ShepReap", &reap_config, 0);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc-1)
    usage();

  current_state = argv[optind];

  main_init();

  global_buck_pool = mp_new(1<<14);
  global_buck_buf = buck2obj_alloc();

  log(L_INFO, "Loading plan");
  load_plan();

  checkpoint_init();
  jobs_init();
  contrib_init(current_state);

  if (initial_plan_entry_count)
    start_loop();
  else
    null_loop();

  contrib_cleanup();
  jobs_cleanup();
  buck2obj_free(global_buck_buf);
  mp_delete(global_buck_pool);
  checkpoint_cleanup();
  log(L_INFO, "Shut down.");

  return 0;
}
