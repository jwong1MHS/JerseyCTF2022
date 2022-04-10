/*
 *	Sherlock Shepherd Daemon -- Planning of a single gathering phase
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/md5.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/shep-plan.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static ucw_time_t now;

struct qkey_hash {
  u64 qkey;
  uns active_urls_multi;
  uns planned_gathers;
  uns planned_refreshes;
  uns planned_anticipates;
  uns planned_over;
  uns max_allowed_urls;
  uns picked_urls;
  uns picked_gathers;
  uns picked_refreshes;
  uns picked_anticipates;
  uns min_delay;
  uns max_conn;
  uns delay;
  uns tight_p;
  u64 avg_dtime;
};

struct tmp_plan_entry {
  struct site *site;
  struct plan_entry e;
  byte channel;
};

static void
plan_init_site(struct site *site)
{
  site->u.plan.robot_oid = OID_UNDEFINED;
}

static struct fastbuf *
plan_preprocess(byte *old_state)
{
  struct fastbuf *idx = read_state_file(old_state, "index");
  struct url_state s;
  struct fastbuf *t = temp_state_file();

  for (struct site *site=NULL; site=site_next(site);)
    site->skey = SKEY_UNRESOLVED;

  while (breadb(idx, &s, sizeof(s)))
    {
      struct site *site = site_lookup(&s.fp.site);
      if (!site)
	die("Inconsistent state -- site %08x%08x not found in site list", FP_PAIR(s.fp.site));
      if (s.type == UTYPE_SKEY)
	{
	  site->skey = (u32) s.oid;
	  continue;
	}
      if (s.flags & USF_ROBOTS)
	{
	  if (s.type == UTYPE_OK)
	    site->u.plan.robot_oid = s.oid;
	  else if (s.type == UTYPE_ERROR)
	    site->u.plan.robot_oid = OID_ERROR;
	}
      struct tmp_plan_entry te;
      te.site = site;
      if (plan_gather_p(&s, &te.e, now))
	bwrite(t, &te, sizeof(te));
      if (s.type != UTYPE_SLEEPING)
	site->u.plan.cnt += (s.refresh_freq ? : 1);
    }
  bclose(idx);
  bflush(t);
  bsetpos(t, 0);
  return t;
}

#define SORT_PREFIX(x) plan_##x
#define SORT_KEY_REGULAR struct tmp_plan_entry
#define SORT_UNIQUE
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
static inline int
plan_compare(struct tmp_plan_entry *tx, struct tmp_plan_entry *ty)
{
  struct plan_entry *x = &tx->e;
  struct plan_entry *y = &ty->e;

  REV_COMPARE(x->priority, y->priority);
  COMPARE(x->oid, y->oid);
  return 0;
}
#include "ucw/sorter/sorter.h"

#define HASH_NODE struct qkey_hash
#define HASH_PREFIX(x) qkey_##x
#define HASH_KEY_ATOMIC qkey
#define HASH_ATOMIC_TYPE u64
#define HASH_WANT_LOOKUP
#define HASH_ZERO_FILL
#define HASH_AUTO_POOL 16384
#include "ucw/hashtable.h"

static void
plan_analyse(void)
{
  struct site *site = NULL;
  uns tight_cnt = 0;

  qkey_init();
  while (site = site_next(site))
    {
      struct qkey_hash *q = qkey_lookup(site_qkey(site));
      q->active_urls_multi += site->u.plan.cnt;
      q->min_delay = MAX(q->min_delay, site->min_delay);
      q->avg_dtime += site->avg_download_time * site->u.plan.cnt;
      if (!q->max_conn || q->max_conn > site->max_conn)
	q->max_conn = site->max_conn;
      site->u.plan.cnt = 0;
      site->u.plan.qkey = q;
    }

  HASH_FOR_ALL(qkey, q)
    {
      if (q->max_conn < 2)
	q->max_conn = 1;
      if (q->active_urls_multi)
	q->avg_dtime = (q->avg_dtime + q->active_urls_multi/2) / q->active_urls_multi / 10;
      if ((q->qkey & SKEY_TYPE_MASK) == SKEY_UNRESOLVED)
	{
	  q->delay = q->min_delay = 0;
	  q->max_allowed_urls = ~0U;
	}
      else if ((q->qkey & SKEY_TYPE_MASK) == SKEY_NONEXISTENT)
	{
	  q->delay = q->min_delay = autoreply_delay;
	  q->max_allowed_urls = (autoreply_delay ? (reap_cycle * q->max_conn / autoreply_delay) : ~0U);
	}
      else
	{
	  if (!q->min_delay)
	    q->min_delay = min_server_delay;

	  uns t = q->active_urls_multi ? (uns)((double) (refresh_cycle * duty_factor * q->max_conn) / q->active_urls_multi * reap_slowdown_factor) : 0;
	  t = MAX(t, q->avg_dtime) - q->avg_dtime;
	  q->delay = CLAMP(t, (int)q->min_delay, (int)std_server_delay);

	  uns t0 = q->active_urls_multi ? (uns)((double) (refresh_cycle * duty_factor * q->max_conn) / q->active_urls_multi) : 0;
	  t0 = MAX(t0, q->avg_dtime) - q->avg_dtime;
	  q->tight_p = (t0 <= q->min_delay + server_overtake);
	  if (q->tight_p)
	    tight_cnt++;

	  /* The planning limit should not be influenced by the average download time, only the timing should */
	  uns limit = q->delay ? (reap_cycle * q->max_conn / q->delay) : ~0U;
	  q->max_allowed_urls = limit;
	}
      DBG("Qkey %04x:%08x: active_multi=%d delay=%d min_delay=%d max_conn=%d avg_dtime=%d limit=%d tight=%d",
	  QK_PAIR(q->qkey), q->active_urls_multi, q->delay, q->min_delay, q->max_conn, (uns)q->avg_dtime, q->max_allowed_urls, q->tight_p);
    }
  HASH_END_FOR;

  while (site = site_next(site))
    if (site->u.plan.qkey->tight_p && site->queue_bonus <= 1000000000)
      site->queue_bonus += 1000000000;

  log(L_DEBUG, "Found %d qkeys with tight timing", tight_cnt);
}

static struct fastbuf *
plan_pick(byte *state UNUSED, struct fastbuf *tmp)
{
  struct fastbuf *t = temp_state_file();
  struct tmp_plan_entry te;
  double experf = estimated_raw_performance * reap_cycle;

  uns global_limit = double_to_uns(experf * reap_optimism_factor);
  log(L_INFO, "Expected performance %d URL's per reap cycle, setting limit to %d", (uns)experf, global_limit);

#ifdef CONFIG_AREAS
  areas_init(state, 1);
  area_lookup(0, 1);
  for (uns i=0; i<areas_max_id; i++)
    area_lookup(i, -1)->num_planned = 0;
#endif

  while (breadb(tmp, &te, sizeof(te)) && global_limit)
    {
      struct site *site = te.site;
      struct plan_entry *e = &te.e;
      struct qkey_hash *q = site->u.plan.qkey;

      if (e->flags & PEF_ANTICIPATED)
	q->planned_anticipates++;
      else if (e->flags & PEF_OVER_AGED)
	q->planned_over++;
      else if (e->flags & PEF_REFRESH)
	q->planned_refreshes++;
      else
	q->planned_gathers++;

#ifdef CONFIG_AREAS
      struct area_info *area = area_lookup(te.e.area, 0);
      if (area->num_planned >= area->plan_limit)
	continue;
#endif

      if (!site->skey || site->u.plan.robot_oid == OID_UNDEFINED)
	{
	  if (site->u.plan.cnt)
	    continue;
	  /* Trickery: if robots.txt haven't been downloaded yet, any valid OID will suffice and the
	   * reaper will be able to construct the robots.txt URL from its URL when it spots the PEF_SYNTH_ROBOTS
	   * flag. This helps us avoid creating new buckets here.
	   */
	  e->priority = ~0U;
	  e->flags |= PEF_SYNTH_ROBOTS;
	}
      else
	{
	  if (q->picked_urls >= q->max_allowed_urls)
	    continue;
	}
      q->picked_urls++;
      if (e->flags & PEF_ANTICIPATED)
	q->picked_anticipates++;
      else if (e->flags & PEF_REFRESH)
	q->picked_refreshes++;
      else
	q->picked_gathers++;
      site->u.plan.cnt++;
      te.channel = (q->max_conn < 2) ? 0 : random_max(q->max_conn);
      bwrite(t, &te, sizeof(te));
      global_limit--;
#ifdef CONFIG_AREAS
      area->num_planned++;
#endif
    }

#ifdef CONFIG_AREAS
  areas_cleanup();
#endif

  bclose(tmp);
  brewind(t);
  return t;
}

#define SORT_PREFIX(x) plan_site_##x
#define SORT_KEY_REGULAR struct tmp_plan_entry
#define SORT_UNIQUE
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
static inline int
plan_site_compare(struct tmp_plan_entry *xx, struct tmp_plan_entry *yy)
{
  COMPARE(xx->site, yy->site);
  COMPARE(xx->channel, yy->channel);

  struct plan_entry *x = &xx->e;
  struct plan_entry *y = &yy->e;

  REV_COMPARE(x->priority, y->priority);
  COMPARE(x->oid, y->oid);
  return 0;
}
#include "ucw/sorter/sorter.h"

static void
plan_generate(byte *new_state, struct fastbuf *tmp)
{
  struct fastbuf *plan = create_state_file(new_state, "plan");
  struct fastbuf *channel_plan = fbgrow_create(1 << 16);
  struct tmp_plan_entry te;
  uns sites = 0, entries = 0, refreshes = 0, anticipates = 0, downloads = 0;

  uns cont = breadb(tmp, &te, sizeof(te));
  while (cont)
    {
      struct plan_site_entry p;
      struct site *site = te.site;
      struct qkey_hash *q = site->u.plan.qkey;
      uns count = site->u.plan.cnt;

      p.qkey = q->qkey;
      p.robot_oid = site->u.plan.robot_oid;
      p.delay = q->delay;
      p.qkey = qkey_set_channel(p.qkey, te.channel);
      uns last_channel = te.channel;
      p.entry_count = 0;
      fbgrow_reset(channel_plan);
      sites++;
      while (cont && te.site == site)
	{
	  if (te.channel != last_channel)
	    {
	      bwrite(plan, &p, sizeof(p));
	      fbgrow_rewind(channel_plan);
	      bbcopy(channel_plan, plan, bfilesize(channel_plan));
	      p.qkey = qkey_set_channel(p.qkey, te.channel);
	      last_channel = te.channel;
	      p.entry_count = 0;
	      fbgrow_reset(channel_plan);
	    }
	  struct plan_entry *e = &te.e;
	  plan_adjust_bonus(e, site);
	  bwrite(channel_plan, e, sizeof(*e));
	  p.entry_count++;
	  if (e->flags & PEF_ANTICIPATED)
	    anticipates++;
	  else if (e->flags & PEF_REFRESH)
	    refreshes++;
	  else
	    downloads++;
	  count--;
	  entries++;
	  cont = bread(tmp, &te, sizeof(te));
	}
      ASSERT(!count);
      bwrite(plan, &p, sizeof(p));
      fbgrow_rewind(channel_plan);
      bbcopy(channel_plan, plan, bfilesize(channel_plan));
    }
  bclose(channel_plan);
  bclose(tmp);
  log(L_INFO, "Generated plan with %d entries in %d sites (%d downloads, %d refreshes, %d anticipated refreshes)",
      entries, sites, downloads, refreshes, anticipates);
  bclose(plan);
}

static void
log_stats(byte *state)
{
  if (!planner_stats)
    return;

  struct fastbuf *f = temp_state_file();
  bprintf(f, "#        Qkey ActivMul Min Dly Con MAD T PlanGath PlanRfsh PlanOver PlanAnti MaxAllow   Picked PickGath PickRfsh PickAnti\n");
  HASH_FOR_ALL(qkey, q)
    {
      bprintf(f, "%04x:%08x %8d %3d %3d %3d %3d %c %8d %8d %8d %8d %8d %8d %8d %8d %8d\n",
	      QK_PAIR(q->qkey), q->active_urls_multi, q->min_delay, q->delay, q->max_conn, (uns)q->avg_dtime, "-+"[q->tight_p],
	      q->planned_gathers, q->planned_refreshes, q->planned_over, q->planned_anticipates,
	      q->max_allowed_urls, q->picked_urls, q->picked_gathers, q->picked_refreshes, q->picked_anticipates);
    }
  HASH_END_FOR;
  put_state_file(state, "plan-stats", f, 0);
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-plan [<config-options>] <state>\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc-1)
    usage();

  byte *state = argv[optind];
  now = time(NULL);

  byte hash[16];
  md5_hash_buffer(hash, (byte *) &now, sizeof(now));
  memcpy(&planner_random, hash, sizeof(planner_random));

  log(L_INFO, "Loading site list");
  site_hash_init(plan_init_site);
  site_hash_load(state, SITE_HASH_FILTER | SITE_HASH_NO_URLS);

  log(L_INFO, "Preprocessing URL's");
  struct fastbuf *tmp = plan_preprocess(state);

  log(L_INFO, "Sorting potential URL's by priority");
  tmp = plan_sort(tmp, NULL);

  log(L_INFO, "Analysing sites and queue keys");
  plan_analyse();

  log(L_INFO, "Picking plan");
  tmp = plan_pick(state, tmp);

  log(L_INFO, "Writing planner statistics");
  log_stats(state);

  log(L_INFO, "Sorting plan");
  tmp = plan_site_sort(tmp, NULL);
  plan_generate(state, tmp);

  log(L_INFO, "Saving site list");
  site_hash_save(state);

  return 0;
}
