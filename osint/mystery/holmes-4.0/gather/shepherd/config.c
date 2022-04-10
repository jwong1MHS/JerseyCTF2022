/*
 *	Sherlock Shepherd Daemon -- Configuration
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/shepherd/shepherd.h"
#include "sherlock/conf.h"

char *shepherd_filter_name = "cf/filter";
char *db_dir = "db";
char *state_dir = "db/state";
uns std_server_delay = 1;
uns min_server_delay = 1;
uns autoreply_delay = 1;
uns server_overtake;
uns req_err_retry = 255;
uns site_err_retry = 255;
uns site_err_expire = ~0U;
uns conn_err_delay = 1;
uns trace_refs;
uns ignore_refs;
uns auto_go_root;
uns max_resolvers = 1;
uns max_flushers = 1;
uns doc_change_mix = 128;
uns contrib_cache_size = 16384;
uns contrib_gap = 255;
uns refresh_cycle = 86400;
uns anticipated_refresh_age = ~0U;
uns reap_cycle = 300;
uns null_cycle;
double estimated_raw_performance = 1;
double reap_optimism_factor = 1;
double reap_slowdown_factor = 1;
double hard_limit_factor = 2;
uns default_soft_limit;
uns dead_soft_limit;
uns default_fresh_limit;
double duty_factor = 1;
u64 expected_bucket_file_size = 1000000000;
uns avg_bucket_size = 1024;
uns avg_url_size = 1024;
clist section_configs;
uns select_hysteresis;
uns stable_time_unit = 3600;
double global_frequent_factor;
uns min_robots_frequency = 1;
uns min_eq_frequency = 1;
uns max_err_frequency = ~0U;
double stability_factor;
uns default_insert_weight = 100;
uns planner_stats;
uns select_stats;
uns safety_brake_limit;
struct unsrange *zombie_gerr_ranges;
uns zombie_expire = ~0U;
uns redirect_to_zombie_timeout = ~0U;
struct unsrange *prune_site_gerr_ranges;
char *url_database_file;
char *url_sorted_file;
uns auto_sort_index;
struct refresh_schema *refresh_schemas[256];

#define TRY(x) do{ byte *_err=(x); if (_err) return _err; }while(0)

static char *
section_config_commit(struct section_config *s)
{
  if (s->section >= SHERLOCK_NUM_SECTIONS)
    return "Invalid section number";
  if (s->limit > 1 || s->limit < 0)
    return "Invalid section limit";
  return NULL;
}

struct refresh_item {
  cnode n;
  uns frequency;
  double allocation;
};

struct refresh_schema_cf {
  cnode n;
  clist items;
  uns id;
  double frequent_factor;
};

static clist refresh_schema_cfs;

static char *
refresh_schema_commit(struct refresh_schema_cf *c)
{
  if (c->id > 255)
    return "Refresh schema ID must be between 0 and 255";
  if (clist_empty(&c->items))
    return "Missing list of frequencies and allocations";
  double total = 0;
  uns prev = 256;
  CLIST_FOR_EACH(struct refresh_item *, i, c->items)
    {
      if (i->frequency < 2 || i->frequency > 255)
	return "Refresh frequencies must be between 2 and 255";
      if (i->frequency >= prev)
	return "Refresh frequencies must be strictly decreasing";
      if (!(i->allocation >= 0) || i->allocation > 1)
	return "Refresh allocations must be between 0 and 1";
      prev = i->frequency;
      total += i->allocation;
    }
  if (total > 1.001 || total < 0.999)
    return "Refresh allocations must sum to 1";
  return NULL;
}

static char *
shepherd_commit(void *p UNUSED)
{
  CF_JOURNAL_VAR(refresh_schemas);
  bzero(refresh_schemas, sizeof(refresh_schemas));
  CLIST_FOR_EACH(struct refresh_schema_cf *, c, refresh_schema_cfs)
    {
      if (refresh_schemas[c->id])
	return cf_printf("Duplicate refresh schemas with Id=%u", c->id);
      struct refresh_schema *s = refresh_schemas[c->id] = cf_malloc_zero(sizeof(**refresh_schemas));
      uns n = 0;
      CLIST_FOR_EACH(struct refresh_item *, i, c->items)
        {
	  s->frequencies[n] = i->frequency;
	  s->allocations[n] = i->allocation;
	  n++;
	}
      s->num = n;
      s->frequent_factor = c->frequent_factor;
    }
  return NULL;
}

static struct cf_section section_config = {
  CF_TYPE(struct section_config),
  CF_COMMIT(section_config_commit),
  CF_ITEMS {
#define F(x) PTR_TO(struct section_config, x)
    CF_UNS("Section", F(section)),
    CF_DOUBLE("Limit", F(limit)),
    CF_UNS("SelectBonus", F(select_bonus)),
#undef F
    CF_END
  }
};

static struct cf_section refresh_item_config = {
  CF_TYPE(struct refresh_item),
  CF_ITEMS {
#define F(x) PTR_TO(struct refresh_item, x)
    CF_UNS("Frequency", F(frequency)),
    CF_DOUBLE("Allocation", F(allocation)),
    CF_END
#undef F
  }
};

static struct cf_section refresh_schema_config = {
  CF_TYPE(struct refresh_schema_cf),
  CF_COMMIT(refresh_schema_commit),
  CF_ITEMS {
#define F(x) PTR_TO(struct refresh_schema_cf, x)
    CF_UNS("Id", F(id)),
    CF_LIST("List", F(items), &refresh_item_config),
    CF_DOUBLE("FrequentFactor", F(frequent_factor)),
#undef F
    CF_END
  }
};

static struct cf_section shepherd_config = {
  CF_COMMIT(shepherd_commit),
  CF_ITEMS {
    CF_STRING("Filter", &shepherd_filter_name),
    CF_STRING("DBDir", &db_dir),
    CF_STRING("StateDir", &state_dir),
    CF_UNS("StdServerDelay", &std_server_delay),
    CF_UNS("MinServerDelay", &min_server_delay),
    CF_UNS("AutoreplyDelay", &autoreply_delay),
    CF_UNS("ServerOvertake", &server_overtake),
    CF_UNS("ReqErrRetry", &req_err_retry),
    CF_UNS("SiteErrRetry", &site_err_retry),
    CF_UNS("SiteErrExpire", &site_err_expire),
    CF_UNS("ConnErrDelay", &conn_err_delay),
    CF_UNS("TraceRefs", &trace_refs),
    CF_UNS("IgnoreRefs", &ignore_refs),
    CF_UNS("AutoGoRoot", &auto_go_root),
    CF_UNS("MaxResolvers", &max_resolvers),
    CF_UNS("MaxFlushers", &max_flushers),
    CF_UNS("DocChangeMix", &doc_change_mix),
    CF_UNS("ContribCacheSize", &contrib_cache_size),
    CF_UNS("ContribGap", &contrib_gap),
    CF_UNS("RefreshCycle", &refresh_cycle),
    CF_UNS("AnticipatedRefreshAge", &anticipated_refresh_age),
    CF_UNS("ReapCycle", &reap_cycle),
    CF_UNS("NullCycle", &null_cycle),
    CF_DOUBLE("EstRawPerformance", &estimated_raw_performance),
    CF_DOUBLE("ReapOptimism", &reap_optimism_factor),
    CF_DOUBLE("ReapSlowdown", &reap_slowdown_factor),
    CF_DOUBLE("HardLimitFactor", &hard_limit_factor),
    CF_UNS("PerSiteLimit", &default_soft_limit),
    CF_UNS("DeadSiteLimit", &dead_soft_limit),
    CF_UNS("SiteFreshLimit", &default_fresh_limit),
    CF_DOUBLE("DutyFactor", &duty_factor),
    CF_U64("BucketFileSize", &expected_bucket_file_size),
    CF_UNS("AvgBucketSize", &avg_bucket_size),
    CF_UNS("AvgURLSize", &avg_url_size),
    CF_LIST("Section", &section_configs, &section_config),
    CF_UNS("SelectHysteresis", &select_hysteresis),
    CF_UNS("StableTimeUnit", &stable_time_unit),
    CF_DOUBLE("GlobalFrequentFactor", &global_frequent_factor),
    CF_UNS("MinRobotsFrequency", &min_robots_frequency),
    CF_UNS("MinEQFrequency", &min_eq_frequency),
    CF_UNS("MaxErrFrequency", &max_err_frequency),
    CF_DOUBLE("StabilityFactor", &stability_factor),
    CF_UNS("DefaultInsertWeight", &default_insert_weight),
    CF_UNS("PlannerStats", &planner_stats),
    CF_UNS("SelectStats", &select_stats),
    CF_UNS("SafetyBrakeLimit", &safety_brake_limit),
    CF_USER_DYN("ZombieErrors", &zombie_gerr_ranges, &cf_type_unsrange, CF_ANY_NUM),
    CF_UNS("RedirectToZombieTimeout", &redirect_to_zombie_timeout),
    CF_UNS("ZombieExpire", &zombie_expire),
    CF_USER_DYN("PruneSiteErrors", &prune_site_gerr_ranges, &cf_type_unsrange, CF_ANY_NUM),
    CF_STRING("URLDatabaseFile", &url_database_file),
    CF_STRING("URLSortedFile", &url_sorted_file),
    CF_UNS("SortIndex", &auto_sort_index),
    CF_LIST("RefreshSchema", &refresh_schema_cfs, &refresh_schema_config),
    CF_END
  }
};

static void CONSTRUCTOR
read_config(void)
{
  cf_declare_section("ShepherD", &shepherd_config, 0);
}
