/*
 *	Sherlock Shepherd Daemon -- Planning Rules
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

static u32 planner_random;

static inline int
plan_gather_p(struct url_state *x, struct plan_entry *e, ucw_time_t now)
{
  switch (ustate_type(x))
    {
    case UTYPE_SKEY:
    case UTYPE_SLEEPING:
    case UTYPE_ZOMBIE:
      return 0;
    case UTYPE_OK:
    case UTYPE_ERROR:
      e->flags = PEF_REFRESH;
      break;
    case UTYPE_NEW:
      e->flags = 0;
      break;
    default:
      ASSERT(0);
    }

  /*
   *  These are the rules for gathering priorities:
   *
   *	0 - 1000M	anticipated refresh (fills otherwise idle time)
   *	1000M - 2000M	normal gathering
   *	2000M - 3000M	tight sites
   *	3000M - 4000M	manually specified queue_bonus can raise normal entries here
   *	4000M - 4294M	explicit refresh
   *
   *  Inside every gathering range:
   *
   *	0 - 100M	frequent refresh
   *	200M - 300M	new pages
   *	400M - 500M	new pages belonging to the initial set
   *	500M - 600M	normal and slightly frequent refresh
   *	900M - 1000M	over-aged pages
   *
   *  In addition to that, we add a penalty for retrying and a small random
   *  value (larger in case of anticipated refreshes) in order to balance load.
   */

  uns pri;
  u32 random = x->fp.site.x[0] ^ x->fp.rest.x[0] ^ planner_random;
  uns raw_age = (x->last_seen > now) ? 0 : now - x->last_seen;
  uns freq = x->refresh_freq ? : 1;
  uns freq2 = (freq > 3) ? (freq/3) : 1;
  uns age = raw_age + random%(reap_cycle/4);
  uns fage1 = freq*age;
  uns fage2 = freq2*age + random%(refresh_cycle/8);

  if (x->flags & USF_REGATHER)
    {
      pri = 4000000000U + MIN(age, 294000000);
      e->flags &= ~PEF_REFRESH;
    }
  else if (x->type == UTYPE_NEW)
    {
      pri = x->weight * 100000;
      uns cage = MIN(age, 2*refresh_cycle);
      pri += (u64)cage * 5000000 / (2*refresh_cycle+4*86400);
      ASSERT(pri < 100000000);
      if (x->flags & USF_INIT)
	pri += 1400000000;
      else
	pri += 1200000000;
    }
  else if (age >= 3*refresh_cycle/2)
    {
      pri = 1900000000 + MIN(age, 90000000);
      e->flags |= PEF_OVER_AGED;
    }
  else if (fage2 >= refresh_cycle)
    pri = 1500000000 + MIN(fage2/8, 90000000);
  else if (fage1 >= refresh_cycle)
    pri = 1000000000 + MIN(fage1/8, 90000000);
  else if (fage2 >= anticipated_refresh_age)
    {
      pri = MIN(fage2/8, 90000000);
      e->flags |= PEF_ANTICIPATED;
    }
  else
    return 0;

  if (x->flags & USF_ROBOTS)
    e->flags |= PEF_ROBOTS;
  if (USF_IS_SACRISIMMUS(x->flags))
    e->flags |= PEF_SACRISIMMUS;

  e->priority = pri;
  e->oid = (u32) x->oid;
  e->retry_count = x->retry_count;
  e->weight = x->weight;
  e->section = x->section;
  e->area = x->area;
  return 1;
}

static inline void
plan_adjust_bonus(struct plan_entry *e, struct site *site)
{
  if (e->priority >= 1000000000 && e->priority < 2000000000 &&
      site->skey && (site->skey & 0xffff0000) != SKEY_NONEXISTENT)
    e->priority += MIN(site->queue_bonus, 2000000000);
}
