/*
 *	Sherlock Shepherd Daemon -- Selection of Active URL Set
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

/* Maximum allowed refresh frequency (as index) for each stability time */
static byte stability_to_freq_index[256][256];

static void
setup_stability_tables(void)
{
  struct refresh_schema *s;
  for (uns si=0; si<256; si++)
    if (s = refresh_schemas[si])
      {
        s->frequencies[s->num] = 1;
        for (uns i=0; i<256; i++)
	  if (stability_factor < 0.01 || !i)
	    stability_to_freq_index[si][i] = 0;
	  else
	    {
	      double freq = refresh_cycle / (i * stable_time_unit * stability_factor);
	      uns ifreq = double_to_uns(freq);
	      ifreq = CLAMP(ifreq, 1, 255);
	      uns j = 0;
	      while (s->frequencies[j] > ifreq)
	        j++;
	      stability_to_freq_index[si][i] = j;
	    }
      }
}

typedef enum select_action {
  ACT_OK,
  ACT_SLEEP,
  ACT_KO,
  ACT_MAX
} select_action;

typedef enum select_cause {
  CAUSE_PERF,
  CAUSE_SITE,
  CAUSE_QKEY,
  CAUSE_SECTION,
  CAUSE_SPACE,
  CAUSE_AREA,
  CAUSE_MAX
} select_cause;

static const byte *cause_names[] = { "Perf", "Site", "Qkey", "Sect", "Space", "Area" };

struct select_qkey {
  u64 qkey;
  uns min_delay, max_conn;
  uns soft_limit, hard_limit;
  uns num_active, num_inactive;
  uns freq_total, freq_limit;
  int freq_limits[SHERLOCK_NUM_FREQS];
  uns schema;
  uns site_count;			/* The rest is statistics */
  uns lost_count[CAUSE_MAX];
};

static struct select_qkey null_qkey = {
  .min_delay = 1,
  .max_conn = 1,
  .soft_limit = ~0U,
  .hard_limit = ~0U
};

#define HASH_NODE struct select_qkey
#define HASH_PREFIX(x) qkey_hash_##x
#define HASH_KEY_ATOMIC qkey
#define HASH_ATOMIC_TYPE u64
#define HASH_WANT_LOOKUP
#define HASH_ZERO_FILL
#define HASH_AUTO_POOL 16384
#include "ucw/hashtable.h"

static struct section {
  u64 space;
  uns soft_limit, hard_limit;
  uns num_active, num_inactive, num_gathered;
  uns actions[ACT_MAX];
  uns cutoff[ACT_MAX];
  uns select_bonus;
} sections[SHERLOCK_NUM_SECTIONS];

static uns perf_soft_limit, perf_hard_limit;
static uns perf_num_active, perf_num_inactive;
static uns perf_freq_limit, perf_freq_total;
static uns space_soft_limit, space_hard_limit;
static uns space_num_active, space_num_inactive;
static u64 space_avail, space_current;
static uns freq_stats[256];

static uns site_cnt_died, site_cnt_revived;

static void
set_qkey_limits(struct select_qkey *q)
{
  /* XXX: shep.c:cmd_qkeystats() should follow the same rules */

  struct refresh_schema *schema = refresh_schemas[q->schema];
  double frequent_factor = schema ? schema->frequent_factor : 0;
  q->num_active = q->num_inactive = q->freq_total = 0;
  uns soft = q->min_delay ? double_to_uns(refresh_cycle * duty_factor * q->max_conn / q->min_delay) : ~0U;
  q->soft_limit = double_to_uns(soft * (1 - frequent_factor));
  q->hard_limit = double_to_uns(q->soft_limit * hard_limit_factor);

  uns soft2 = double_to_uns(refresh_cycle * duty_factor * q->max_conn / (q->min_delay ? : 1));
  q->freq_limit = double_to_uns(soft2 * frequent_factor);
  if (schema)
    for (uns i=0; i<schema->num; i++)
      q->freq_limits[i] = double_to_uns(q->freq_limit * schema->allocations[i]);
}

static void
set_limits(void)
{
  /* Global performance limit */
  uns perf = double_to_uns(estimated_raw_performance * refresh_cycle * duty_factor);
  perf_soft_limit = double_to_uns(perf * (1 - global_frequent_factor));
  perf_hard_limit = double_to_uns(perf_soft_limit * hard_limit_factor);
  perf_freq_limit = double_to_uns(perf * global_frequent_factor);
  log(L_INFO, "Performance limit: %u soft, %u hard, %u frequent refresh", perf_soft_limit, perf_hard_limit, perf_freq_limit);

  /* Global space limit */
  space_avail = expected_bucket_file_size;
  log(L_INFO, "Space: %uM available", (uns)(space_avail / 1048576));
  space_soft_limit = double_to_uns(space_avail / (double)(avg_bucket_size + (hard_limit_factor - 1)*avg_url_size));
  space_hard_limit = double_to_uns(space_soft_limit * hard_limit_factor);
  log(L_INFO, "Space limit: %u pages soft, %u URLs hard", space_soft_limit, space_hard_limit);

  /* Per-section space limits */
  struct section_config *sl;
  CLIST_WALK(sl, section_configs)
    {
      sections[sl->section].space = (u64)(space_avail * sl->limit);
      sections[sl->section].select_bonus = sl->select_bonus;
    }
  for (uns i=0; i<SHERLOCK_NUM_SECTIONS; i++)
    {
      struct section *s = &sections[i];
      s->soft_limit = MIN(s->space / avg_bucket_size, ~0U);
      uns h1 = MIN(s->space / avg_url_size, ~0U);
      uns h2 = double_to_uns(s->soft_limit * hard_limit_factor);
      s->hard_limit = MIN(h1, h2);
      log(L_INFO, "Section %d limit: %u soft, %u hard (%uM of space)",
	  i, s->soft_limit, s->hard_limit, (uns)(s->space / 1048576));
    }

  /* Default per-site limits */
  uns default_site_soft = default_soft_limit;
  uns default_site_hard = double_to_uns(default_site_soft * hard_limit_factor);
  uns default_site_fresh = default_fresh_limit;
  log(L_INFO, "Default per-site limits: %u soft, %u hard, %u fresh", default_site_soft, default_site_hard, default_site_fresh);

  /* Default per-qkey limits */
  struct select_qkey dq;
  bzero(&dq, sizeof(dq));
  dq.site_count = 1;
  dq.min_delay = min_server_delay;
  dq.max_conn = 1;
  set_qkey_limits(&dq);
  log(L_INFO, "Default per-qkey performance limits: %u soft, %u hard, %u frequent refresh", dq.soft_limit, dq.hard_limit, dq.freq_limit);

  /* Show frequency limits */
  if (refresh_schemas[0])
    {
      byte flims[SHERLOCK_NUM_FREQS * 16], *fli = flims;
      for (uns i=0; i<refresh_schemas[0]->num; i++)
	fli += sprintf(fli, " %d:%d", refresh_schemas[0]->frequencies[i], dq.freq_limits[i] / refresh_schemas[0]->frequencies[i]);
      log(L_INFO, "Default per-qkey frequency limits:%s", flims);
    }
  setup_stability_tables();

  /* Detailed per-qkey limits */
  qkey_hash_init();
  for (struct site *s = NULL; s = site_next(s);)
    {
      s->u.select.num_useful = 0;
      if (s->flags & SITE_REJECTED) /* We delete everything from rejected sites */
	continue;
      s->num_active = s->num_inactive = s->num_fresh = s->num_gathered = s->num_oscillations = 0;
      if (s->error_cycles > site_err_retry)
	{
	  s->skey = SKEY_NONEXISTENT;
	  s->error_cycles = 0;
	  site_cnt_died++;
	}
      if ((s->skey & SKEY_TYPE_MASK) == SKEY_UNRESOLVED ||
	  (s->skey & SKEY_TYPE_MASK) == SKEY_NONEXISTENT)
	{
	  s->u.select.qkey = &null_qkey;
	  s->soft_limit = dead_soft_limit;
	  s->hard_limit = double_to_uns(dead_soft_limit * hard_limit_factor);
	}
      else
	{
	  struct select_qkey *q = qkey_hash_lookup(site_qkey(s));
	  q->site_count++;
	  q->min_delay = MAX(q->min_delay, s->min_delay);
	  if (!q->max_conn || q->max_conn > s->max_conn)
	    q->max_conn = s->max_conn;
	  if (q->schema < s->refresh_schema && refresh_schemas[s->refresh_schema])
	    q->schema = s->refresh_schema;
	  s->u.select.qkey = q;
	}
    }
  uns qkey_cnt = 0;
  HASH_FOR_ALL(qkey_hash, q)
    {
      qkey_cnt++;
      set_qkey_limits(q);
    }
  HASH_END_FOR;
  log(L_INFO, "Set up limits for %d qkeys", qkey_cnt);

#ifdef CONFIG_AREAS
  for (area_t id=0; id<areas_max_id; id++)
    {
      struct area_info *a = area_lookup(id, -1);
      ASSERT(a);
      a->num_active = a->num_inactive = a->num_gathered = 0;
    }
#endif
}

static inline void
select_limit(select_action *actionp, uns active, uns inactive, uns soft, uns hard,
	     select_cause *causep, select_cause cause)
{
  if (*actionp != ACT_KO && active + inactive >= hard)
    {
      *actionp = ACT_KO;
      *causep = cause;
    }
  else if (*actionp == ACT_OK && active >= soft)
    {
      *actionp = ACT_SLEEP;
      *causep = cause;
    }
}

static inline void
update_limit(select_action action, uns *activep, uns *inactivep)
{
  if (action == ACT_OK)
    (*activep)++;
  else if (action == ACT_SLEEP)
    (*inactivep)++;
}

#define SORT_PREFIX(x) weight_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

static inline uns
select_weight(struct url_state *x)
{
  uns w = x->weight;
  if (USF_IS_SACRED(x->flags))
    w += 1000000;
  if (x->flags & USF_SELECT_PRIORITY)
    w += 100000;
  uns type = ustate_type(x);
  if (type == UTYPE_OK || type == UTYPE_ERROR)
    w += select_hysteresis;
  w += sections[x->section].select_bonus;
  return w;
}

static inline int
weight_compare(struct url_state *x, struct url_state *y)
{
  uns xw = select_weight(x);
  uns yw = select_weight(y);
  REV_COMPARE(xw, yw);
  int r;
  if (r = rest_fp_cmp(&x->fp.rest, &y->fp.rest))
    return r;
  return site_fp_cmp(&x->fp.site, &y->fp.site);
}

#include "gather/shepherd/index-sort.h"

static inline void
select_freq(struct url_state *s, struct select_qkey *qkey, struct site *site)
{
  uns f, fi;
  uns sid = qkey->schema;
  struct refresh_schema *schema;

  if (perf_freq_total >= perf_freq_limit ||
      qkey->freq_total >= qkey->freq_limit ||
      !(schema = refresh_schemas[sid]))
    {
      f = 1;
      DBG("\tFreq 1 (run out of freq_limit)");
    }
  else
    {
      uns stable_time = s->stable_time;
      if (site->refresh_boost > 1)
	stable_time /= site->refresh_boost;
      uns fi0 = stability_to_freq_index[sid][stable_time];	/* Try the optimum frequency */
      DBG("\tInitial frequency estimated to %d[%d] (stable time %d/%d)", schema->frequencies[fi0], fi0, s->stable_time, site->refresh_boost);
      if (s->flags & USF_ROBOTS)
        {
	  fi0 = MIN(min_robots_frequency, fi0);
	  DBG("\tAdjusted to %d[%d] by robot rule", schema->frequencies[fi0], fi0);
	}
      else if (ustate_type(s) == UTYPE_ERROR)
        {
	  fi0 = MAX(max_err_frequency, fi0);
	  DBG("\tAdjusted to %d[%d] for erroneous URL", schema->frequencies[fi0], fi0);
	}
      else if (s->flags & USF_NEEDED_BY_EQ)
        {
	  fi0 = MIN(min_eq_frequency, fi0);
	  DBG("\tAdjusted to %d[%d] for EQ page", schema->frequencies[fi0], fi0);
	}
      fi = fi0;
        {
	  while (fi > 0 && qkey->freq_limits[fi] <= 0) 		/* If we ran out of quota, try to use quota of the higher freq's */
	    fi--;
	}
      if (qkey->freq_limits[fi] <= 0)				/* It didn't help, so search for lower freq's */
        {
	  fi = fi0;
	  while (fi < schema->num && qkey->freq_limits[fi] <= 0)
	    fi++;
	}
      f = schema->frequencies[fi];
      DBG("\tFreq %d (optimum %d)", f, schema->frequencies[fi0]);
      if (fi < schema->num)
        {
	  qkey->freq_limits[fi] -= f - 1;
	  DBG("\tRemaining limit[%d]=%d", fi, qkey->freq_limits[fi]);
	}
    }
  freq_stats[f]++;
  perf_freq_total += f - 1;
  qkey->freq_total += f - 1;
  s->refresh_freq = f;
}

static inline uns
zombie_expired(struct url_state *s, time_t now)
{
  return now - s->last_seen > zombie_expire;
}

static struct fastbuf *
select_urls(struct fastbuf *in)
{
  struct fastbuf *out = temp_state_file();
  struct url_state s;
  uns num_sacred = 0;
  uns num_unref_errors = 0;
  uns in_by_type[UTYPE_MAX], out_by_type[UTYPE_MAX], ast[UTYPE_MAX][ACT_MAX][CAUSE_MAX];
  uns cutoff[UTYPE_MAX][ACT_MAX][CAUSE_MAX];
  time_t now = time(NULL);

  bzero(in_by_type, sizeof(in_by_type));
  bzero(out_by_type, sizeof(out_by_type));
  bzero(ast, sizeof(ast));
  bzero(cutoff, sizeof(cutoff));

  while (bread(in, &s, sizeof(s)))
    {
      struct site *site = site_lookup(&s.fp.site);
      ASSERT(site);
      if (site->flags & SITE_REJECTED)
	continue;
      struct select_qkey *qkey = site->u.select.qkey;
      struct section *sect = &sections[(s.section < SHERLOCK_NUM_SECTIONS) ? s.section : 0];
      select_action action = ACT_OK;
      select_cause cause = 0;
#ifdef CONFIG_AREAS
      struct area_info *area = area_lookup(s.area, 0);
#endif
      uns type = ustate_type(&s);

      if (type == UTYPE_SKEY)
	{
	  site->u.select.skey_found = 1;
	  if ((s.oid & SKEY_TYPE_MASK) != SKEY_NONEXISTENT)
	    {
	      if ((site->skey & SKEY_TYPE_MASK) == SKEY_NONEXISTENT)		/* Mark site as non-existent */
		{
		  s.oid = SKEY_NONEXISTENT;
		  s.last_seen = now;
		}
	      else
		ASSERT(site->skey == (u32) s.oid);
	    }
	  else									/* Expire the non-existent markings? */
	    {
	      ASSERT((site->skey & SKEY_TYPE_MASK) == SKEY_NONEXISTENT);
	      if (now - s.last_seen > site_err_expire)
		{
		  site->error_cycles = site_err_retry;
		  site->skey = SKEY_UNRESOLVED;
		  site_cnt_revived++;
		  continue;
		}
	    }
	  bwrite(out, &s, sizeof(s));
	  continue;
	}
      /* UTYPE_SKEY is free, so we re-use it for statistics on contributions */
#define UTYPE_X_CONTRIB UTYPE_SKEY
      uns stat_type = ((s.flags & USF_CONTRIB) && type == UTYPE_NEW) ? UTYPE_X_CONTRIB : type;
      in_by_type[stat_type]++;

      if (USF_IS_SACRISIMMUS(s.flags))
	num_sacred++;
      else if (type == UTYPE_ERROR && (s.flags & USF_UNREF))
	{
	  state_log(site, &s, LOG_SRC_SELECT, LOG_SELECT_UNREF, 0, s.weight);
	  num_unref_errors++;
	  continue;
	}
      else
        {
	  if (type == UTYPE_ZOMBIE)
	    {
	      action = zombie_expired(&s, now) ? ACT_KO : ACT_SLEEP;
	      /* cause = 0; */
	    }
	  else if ((s.type & UST_NO_TARGET) && now - s.last_seen > redirect_to_zombie_timeout)
	    {
	      ustate_set_type(&s, type = UTYPE_ZOMBIE);
	      s.oid = 2308;
	      action = ACT_SLEEP;
	      /* cause = 0 */
	    }
	  if (USF_IS_SACRED(s.flags) && site->hard_limit) /* even USF_NEEDED_BY_EQ should be deleted with zero site limit */
	    num_sacred++;
	  else
	    {
	      if (type != UTYPE_ZOMBIE && !(s.flags & USF_TRUE_WEIGHT))
	        {
	          site->num_fresh++;
		  if (site->num_fresh > site->fresh_limit)
		    {
		      action = ACT_SLEEP;
		      cause = CAUSE_SITE;
		    }
		}
#ifdef CONFIG_AREAS
	      select_limit(&action, area->num_active, area->num_inactive, area->soft_limit, area->hard_limit, &cause, CAUSE_AREA);
#endif
	      select_limit(&action, site->num_active, site->num_inactive, site->soft_limit, site->hard_limit, &cause, CAUSE_SITE);
	      select_limit(&action, qkey->num_active, qkey->num_inactive, qkey->soft_limit, qkey->hard_limit, &cause, CAUSE_QKEY);
	      select_limit(&action, sect->num_active, sect->num_inactive, sect->soft_limit, sect->hard_limit, &cause, CAUSE_SECTION);
	      select_limit(&action, perf_num_active, perf_num_inactive, perf_soft_limit, perf_hard_limit, &cause, CAUSE_PERF);
	      select_limit(&action, space_num_active, space_num_inactive, space_soft_limit, space_hard_limit, &cause, CAUSE_SPACE);
	    }
	}

#ifdef CONFIG_AREAS
      update_limit(action, &area->num_active, &area->num_inactive);
#endif
      update_limit(action, &site->num_active, &site->num_inactive);
      update_limit(action, &qkey->num_active, &qkey->num_inactive);
      update_limit(action, &sect->num_active, &sect->num_inactive);
      update_limit(action, &perf_num_active, &perf_num_inactive);
      update_limit(action, &space_num_active, &space_num_inactive);
      ast[stat_type][action][cause]++;
      if (!cutoff[stat_type][action][cause])
	cutoff[stat_type][action][cause] = select_weight(&s);
      if (cause == CAUSE_SECTION)
	{
	  sect->actions[action]++;
	  if (!sect->cutoff[action])
	    sect->cutoff[action] = select_weight(&s);
	}

      if (type == UTYPE_OK || type == UTYPE_ERROR) /* Extra statistics for downloaded pages */
	{
	  if (action == ACT_OK)
	    {
	      site->num_gathered++;
	      sect->num_gathered++;
#ifdef CONFIG_AREAS
	      area->num_gathered++;
#endif
	    }
	  else
	    {
	      site->num_oscillations++;
	      qkey->lost_count[cause]++;
	    }
	}

      switch (action)
	{
	case ACT_OK:
	  DBG("%08x%08x:%08x%08x %04x:%08x:%d: OK", FP_QUAD(s.fp), QK_TRIPLE(qkey->qkey));
	  space_current += avg_bucket_size;
	  if (type == UTYPE_SLEEPING)
	    {
	      ustate_set_type(&s, type = UTYPE_NEW);
	      state_log(site, &s, LOG_SRC_SELECT, LOG_SELECT_WAKEUP, 0, CLAMP(select_weight(&s), 0, 255));
	    }
	  select_freq(&s, qkey, site);
	  break;
	case ACT_SLEEP:
	  DBG("%08x%08x:%08x%08x %04x:%08x:%d: INACTIVE by %d", FP_QUAD(s.fp), QK_TRIPLE(qkey->qkey), cause);
	  if (type == UTYPE_ZOMBIE)
	    break;
	  space_current += avg_url_size;
	  if (type != UTYPE_SLEEPING)
	    state_log(site, &s, LOG_SRC_SELECT, LOG_SELECT_SLEEP, cause, CLAMP(select_weight(&s), 0, 255));
	  ustate_set_type(&s, type = UTYPE_SLEEPING);
	  break;
	case ACT_KO:
	  DBG("%08x%08x:%08x%08x %04x:%08x:%d: DISCARD by %d", FP_QUAD(s.fp), QK_TRIPLE(qkey->qkey), cause);
	  state_log(site, &s, LOG_SRC_SELECT, LOG_SELECT_DISCARD, cause, CLAMP(select_weight(&s), 0, 255));
	  continue;
	default:
	  ASSERT(0);
	}
      stat_type = ((s.flags & USF_CONTRIB) && type == UTYPE_NEW) ? UTYPE_X_CONTRIB : type;
      out_by_type[stat_type]++;
      if (!(s.flags & USF_ROBOTS))
	site->u.select.num_useful++;
      bwrite(out, &s, sizeof(s));
    }

  for (struct site *site=NULL; site=site_next(site);)
    if (!site->u.select.skey_found && (site->skey & SKEY_TYPE_MASK) == SKEY_NONEXISTENT && !(site->flags & SITE_REJECTED))
      {
	bzero(&s, sizeof(s));
	s.fp.site = site->fp;
	s.fp.rest = SKEY_FOOTPRINT;
	s.last_seen = now;
	s.oid = SKEY_NONEXISTENT;
	s.flags = USF_ROBOTS;
	ustate_set_type(&s, UTYPE_SKEY);
	bwrite(out, &s, sizeof(s));
      }

  uns in_gath = in_by_type[UTYPE_OK] + in_by_type[UTYPE_ERROR];
  uns in_act = in_gath + in_by_type[UTYPE_NEW] + in_by_type[UTYPE_X_CONTRIB];
  uns in_inact = in_by_type[UTYPE_SLEEPING] + in_by_type[UTYPE_ZOMBIE];
  uns in_all = in_act + in_inact;
  log(L_INFO, "Input: %d URL's (%d sacred; %d active [%d contrib + %d new + %d gathered], %d inactive [%d sleeping + %d zombies])",
      in_all, num_sacred, in_act, in_by_type[UTYPE_X_CONTRIB], in_by_type[UTYPE_NEW], in_gath, in_inact, in_by_type[UTYPE_SLEEPING], in_by_type[UTYPE_ZOMBIE]);

  uns out_gath = out_by_type[UTYPE_OK] + out_by_type[UTYPE_ERROR];
  uns out_act = out_gath + out_by_type[UTYPE_NEW] + out_by_type[UTYPE_X_CONTRIB];
  uns out_inact = out_by_type[UTYPE_SLEEPING] + out_by_type[UTYPE_ZOMBIE];
  uns out_all = out_act + out_inact;
  log(L_INFO, "Output: %d URL's (%d active [%d contrib + %d new + %d gathered], %d inactive [%d sleeping + %d zombies])",
      out_all, out_act, out_by_type[UTYPE_X_CONTRIB], out_by_type[UTYPE_NEW], out_gath, out_inact, out_by_type[UTYPE_SLEEPING], out_by_type[UTYPE_ZOMBIE]);

  log(L_INFO, "Active: %d gathered, %d new [%d inherited + %d woken up]",
      out_by_type[UTYPE_OK] + out_by_type[UTYPE_ERROR],
      out_by_type[UTYPE_NEW] + out_by_type[UTYPE_X_CONTRIB],
      ast[UTYPE_NEW][ACT_OK][0] + ast[UTYPE_X_CONTRIB][ACT_OK][0],
      ast[UTYPE_SLEEPING][ACT_OK][0]);

  uns num_lost = 0;
  for (select_action a=ACT_SLEEP; a<=ACT_KO; a++)
    {
      log(L_INFO, "%-11s      perf           site           qkey        section          "
#ifdef CONFIG_AREAS
	  "area           "
#endif
	  "space          total",
	  (a == ACT_SLEEP) ? "Inactive:" : "Discarded:");
      uns tab[5][CAUSE_MAX+1], cut[5][CAUSE_MAX];
      bzero(tab, sizeof(tab));
      for (select_cause c=0; c<CAUSE_MAX; c++)
	{
	  tab[0][c] = ast[UTYPE_SLEEPING][a][c];
	  cut[0][c] = cutoff[UTYPE_SLEEPING][a][c];
	  tab[1][c] = ast[UTYPE_X_CONTRIB][a][c];
	  cut[1][c] = cutoff[UTYPE_X_CONTRIB][a][c];
	  tab[2][c] = ast[UTYPE_NEW][a][c];
	  cut[2][c] = cutoff[UTYPE_NEW][a][c];
	  tab[3][c] = ast[UTYPE_OK][a][c] + ast[UTYPE_ERROR][a][c];
	  cut[3][c] = MAX(cutoff[UTYPE_OK][a][c], cutoff[UTYPE_ERROR][a][c]);
	  tab[4][c] = tab[0][c] + tab[1][c] + tab[2][c] + tab[3][c];
	  cut[4][c] = MIN(MIN(cut[0][c], cut[1][c]), MIN(cut[2][c], cut[3][c]));
	  for (uns i=0; i<5; i++)
	    tab[i][CAUSE_MAX] += tab[i][c];
	}
      for (uns i=0; i<5; i++)
	log(L_INFO, "  %-9s %9d[%3d] %9d[%3d] %9d[%3d] %9d[%3d] %9d[%3d] "
#ifdef CONFIG_AREAS
	    "%9d[%3d] "
#endif
	    "%9d",
	    ((byte *[]) { "Sleeping:", "Contrib:", "New:", "Gathered:", "Total:" })[i],
	    tab[i][CAUSE_PERF], cut[i][CAUSE_PERF],
	    tab[i][CAUSE_SITE], cut[i][CAUSE_SITE],
	    tab[i][CAUSE_QKEY], cut[i][CAUSE_QKEY],
	    tab[i][CAUSE_SECTION], cut[i][CAUSE_SECTION],
#ifdef CONFIG_AREAS
	    tab[i][CAUSE_AREA], cut[i][CAUSE_AREA],
#endif
	    tab[i][CAUSE_SPACE], cut[i][CAUSE_SPACE],
	    tab[i][CAUSE_MAX]);
      num_lost += tab[3][CAUSE_MAX];
    }

  log(L_INFO, "Lost %d gathered URL's", num_lost);
  log(L_INFO, "Deleted %d unreferenced erroneous documents", num_unref_errors);
  log(L_INFO, "Performance utilized: %d (%d for frequent refresh)", perf_num_active, perf_freq_total);
  log(L_INFO, "Space estimated: %dM", (uns)(space_current / 1048576));

  for (uns i=0; i<SHERLOCK_NUM_SECTIONS; i++)
    {
      struct section *s = &sections[i];
      log(L_INFO, "Section %d: %d gathered, %d active, %d inactive [%d], %d over [%d]", i,
	  s->num_gathered, s->num_active, s->num_inactive, s->cutoff[ACT_SLEEP], s->actions[ACT_KO], s->cutoff[ACT_KO]);
    }
  for (uns i=0; i<256; i++)
    if (freq_stats[i])
      log(L_INFO, "Frequency %d: %d pages", i, freq_stats[i]);

  if (safety_brake_limit && num_lost >= safety_brake_limit)
    die("At least SafetyBrakeLimit (%d) gathered documents lost. Pulling safety brake!", safety_brake_limit);

  return out;
}

static void
prune_sites(void)
{
  /*
   *  Prune sites which no longer have URL records.
   *  There still can exist SKEY records for them, but shep-record will delete them.
   */

  uns pruned=0, alive=0, dead=0, unres=0;
  for (struct site *s=NULL; s=site_next(s);)
    {
      if (!s->u.select.num_useful)
	{
	  site_delete(s);
	  pruned++;
	}
      else if ((s->skey & SKEY_TYPE_MASK) == SKEY_NONEXISTENT)
	dead++;
      else if ((s->skey & SKEY_TYPE_MASK) == SKEY_UNRESOLVED)
	unres++;
      else
	alive++;
    }
  log(L_INFO, "Sites: %d alive, %d unres, %d dead, %d pruned; %d died, %d revived", alive, unres, dead, pruned, site_cnt_died, site_cnt_revived);
}

static void
show_stats(byte *state)
{
  if (!select_stats)
    return;

  struct fastbuf *f = temp_state_file();
  bputs(f, "# Qkey            Sites  Active  Inactv   SoftL   HardL FreqTot   FreqL");
  for (uns i=0; i<SHERLOCK_NUM_FREQS; i++)
    bprintf(f, " Freq%02d", i);
  for (select_cause c=0; c<CAUSE_MAX; c++)
    bprintf(f, " %6s", cause_names[c]);
  bputsn(f, "   Lost");
  HASH_FOR_ALL(qkey_hash, q)
    {
      bprintf(f, "%04x:%08x:%-3d %5u %7u %7u %7u %7u %7u %7u", QK_TRIPLE(q->qkey), q->site_count, q->num_active, q->num_inactive, q->soft_limit, q->hard_limit, q->freq_total, q->freq_limit);
      for (uns i=0; i<SHERLOCK_NUM_FREQS; i++)
	bprintf(f, " %6u", q->freq_limits[i]);
      uns lost_total = 0;
      for (select_cause c=0; c<CAUSE_MAX; c++)
	{
	  bprintf(f, " %6u", q->lost_count[c]);
	  lost_total += q->lost_count[c];
	}
      bprintf(f, " %6u\n", lost_total);
    }
  HASH_END_FOR;
  put_state_file(state, "select-qkey-log", f, 0);
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-select [<config-options>] <state>\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc-1)
    usage();

  byte *state = argv[optind];

  log(L_INFO, "Loading site list");
  site_hash_init(NULL);
  site_hash_load(state, SITE_HASH_FILTER | SITE_HASH_NO_URLS);
#ifdef CONFIG_AREAS
  areas_init(state, 1);
#endif
  state_log_open(state);

  log(L_INFO, "Calculating limits");
  set_limits();
  if (!access(state_file_name(state, "disable-brake"), R_OK))
    {
      safety_brake_limit = 0;
      log(L_WARN, "Disabling safety brake. The risk is yours.");
    }

  log(L_INFO, "Sorting known URL's by weight");
  struct fastbuf *orig_idx = weight_sort(read_state_file(state, "index"), NULL);

  log(L_INFO, "Selecting URL's");
  struct fastbuf *idx = select_urls(orig_idx);
  if (select_stats > 1)
    put_state_file(state, "select-index", orig_idx, 0);
  else
    bclose(orig_idx);
  prune_sites();
  site_hash_save(state);
  put_state_file(state, "index", idx, STATE_FLAG_SORTED);
  show_stats(state);

  state_log_close();
#ifdef CONFIG_AREAS
  areas_cleanup();
#endif

  return 0;
}
