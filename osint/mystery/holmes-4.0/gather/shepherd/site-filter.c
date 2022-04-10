/*
 *	Sherlock Shepherd Daemon -- Site Filters
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/conf.h"
#include "ucw/mempool.h"
#include "filter/filter.h"
#include "gather/shepherd/shepherd.h"

#define COPY_INTS \
  F(monitor) \
  F(select_bonus) \
  F(max_conn) \
  F(refresh_schema) \
  F(refresh_boost)

struct site_filter_data {
  struct url url_s;
  struct site site;
  uns error_code;
#define F(x) uns x;
  COPY_INTS
#undef F
};

static struct filter_binding site_bindings[] = {
  /* Site URL ("url" and "rest" are undefined) */
  { "protocol",		OFFSETOF(struct site_filter_data, url_s.protocol) },
  { "host",		OFFSETOF(struct site_filter_data, url_s.host) },
  { "port",		OFFSETOF(struct site_filter_data, url_s.port) },
  /* Attributes specific for Shepherd */
  { "soft_limit",	OFFSETOF(struct site_filter_data, site.soft_limit) },
  { "hard_limit",	OFFSETOF(struct site_filter_data, site.hard_limit) },
  { "fresh_limit",	OFFSETOF(struct site_filter_data, site.fresh_limit) },
  { "min_delay",	OFFSETOF(struct site_filter_data, site.min_delay) },
  { "queue_key",	OFFSETOF(struct site_filter_data, site.skey) },
  { "queue_bonus",	OFFSETOF(struct site_filter_data, site.queue_bonus) },
  { "select_bonus",	OFFSETOF(struct site_filter_data, select_bonus) },
  { "monitor",		OFFSETOF(struct site_filter_data, monitor) },
  { "max_conn",		OFFSETOF(struct site_filter_data, max_conn) },
  { "error_code",       OFFSETOF(struct site_filter_data, error_code) },
  { "refresh_schema",	OFFSETOF(struct site_filter_data, refresh_schema) },
  { "refresh_boost",	OFFSETOF(struct site_filter_data, refresh_boost) },
  { NULL,		0 }
};

static struct filter *sf_filter;
static struct filter_args *sf_args;
static uns sf_inited;

static void
site_filter_init(void)
{
  if (shepherd_filter_name)
    {
      sf_filter = filter_load(shepherd_filter_name, filter_builtin_vars, site_bindings, NULL);
      sf_args = filter_intr_new(sf_filter);
      sf_args->pool = mp_new(4096);
  }
  sf_inited = 1;
}

uns
site_filter(struct site *s, byte *hostname)
{
  if (!sf_inited)
    site_filter_init();
  s->soft_limit = default_soft_limit;
  s->hard_limit = (uns)(default_soft_limit * hard_limit_factor);
  s->fresh_limit = default_fresh_limit;
  s->min_delay = min_server_delay;
  s->max_conn = 1;
  s->queue_bonus = 0;
  s->flags = 0;
  s->refresh_schema = 0;
  s->refresh_boost = 0;
  if (sf_filter)
    {
      struct site_filter_data d;

      d.url_s.protocol = url_proto_names[s->proto];
      d.url_s.host = hostname ? : s->hostname;
      ASSERT(*d.url_s.host);
      d.url_s.port = s->port;
      d.error_code = 0;
      d.site = *s;
#define F(x) d.x = s->x;
      COPY_INTS;
#undef F

      mp_flush(sf_args->pool);
      sf_args->raw = &d;
      sf_args->attr = NULL;
      uns rejected = !filter_intr_run(sf_args);

      *s = d.site;
#define F(x) s->x = d.x;
      COPY_INTS;
#undef F

      if (rejected)
	{
	  DBG("FILTER %s://%s:%d/: REJECT", d.url_s.protocol, d.url_s.host, d.url_s.port);
            for (uns i = 0; i < DARY_LEN(prune_site_gerr_ranges); i++)
	      {
		struct unsrange *r = prune_site_gerr_ranges + i;
		if (d.error_code >= r->min && d.error_code <= r->max)
		  {
	            s->soft_limit = s->hard_limit = s->fresh_limit = 0;
		    s->flags |= SITE_REJECTED;
		    DBG("Rejecting the whole site");
		    break;
		  }
	      }
	  return 0;
	}
      else
	{
	  DBG("FILTER %s://%s:%d/: ACCEPT", d.url_s.protocol, d.url_s.host, d.url_s.port);
	  return 1;
	}
    }
  return 1;
}

void
site_hash_filter(void)
{
  struct site *s = NULL;
  while (s = site_next(s))
    site_filter(s, NULL);
}
