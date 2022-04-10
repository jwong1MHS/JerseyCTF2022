/*
 *	Sherlock Shepherd Reaper -- Robot Rules
 *
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 *	(c) 2004 Robert Spalek <robert@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "sherlock/object.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/reap.h"

void
prefetch_robots(struct job *job)
{
  struct qsite *site = job->site;
  ASSERT(!job->robots);
  if (!job->prefetched_robots && site->robot_id < OID_ERROR && !site->robot_cache)
    {
      job->prefetched_robots_ctx.hdr.oid = site->robot_id;
      job->prefetched_robots = obuck_fetch_oid(&bucket_file, &job->prefetched_robots_ctx, OBUCK_NO_LOCK);
    }
}

void
fetch_robots(struct job *job, struct buck2obj_buf *buck_buf, struct mempool *buck_pool, struct mempool *robots_pool)
{
  struct qsite *site = job->site;
  if (site->robot_id >= OID_ERROR || (job->robots = site->robot_cache))
    return;
  prefetch_robots(job);
  ASSERT(job->prefetched_robots);

  mp_flush(buck_pool);
  struct odes *o = obj_read_bucket(buck_buf, buck_pool, job->prefetched_robots_ctx.hdr.type, job->prefetched_robots_ctx.hdr.length, job->prefetched_robots, NULL, 1);

  struct mempool *dest_pool = ((uns)(job->site->plan_end - job->site->plan_start) < robots_cache_threshold) ? job->pool : robots_pool;
  struct odes *r = obj_new(dest_pool);	// copy 'r' attributes to resulting mempool
  if (o)
    {
      for (struct oattr *oa = obj_find_attr(o, 'r'); oa; oa=oa->same)
        obj_add_attr(r, 'r', oa->val);
    }
  bclose(job->prefetched_robots);
  job->prefetched_robots = NULL;
  DBG("Loaded robots.txt %08x len=%d", job->prefetched_robots_ctx.hdr.oid, job->prefetched_robots_ctx.hdr.length);
  if (dest_pool == robots_pool)
    site->robot_cache = r;
  job->robots = r;
}

uns
check_robots(struct job *job)
{
  if (!job->robots)
    {
      DBG("ROBOTS: no rules");
      return 1;
    }

  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], rest[MAX_URL_SIZE];
  struct url ur;
  int err = url_canon_split(job->url, buf1, buf2, &ur);
  if (!err)
    err = url_enescape(ur.rest, rest);
  ASSERT(!err);
  if (err)
    return 0;
  if (!strcmp(rest, "/robots.txt"))
    {
      DBG("ROBOTS: don't filter robots.txt");
      return 1;
    }
  DBG("ROBOTS: checking %s", rest);

  for (struct oattr *oa = obj_find_attr(job->robots, 'r'); oa; oa=oa->same)
  {
    DBG("ROBOTS: rule %s", oa->val);
    if (!strncmp(rest, oa->val, strlen(oa->val)))
    {
      DBG("ROBOTS: FORBIDDEN");
      return 0;
    }
  }
  DBG("ROBOTS: PASSED");
  return 1;
}
