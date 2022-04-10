/*
 *	Sherlock Shepherd Daemon -- Site Hash Table
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "gather/shepherd/shepherd.h"

#include <string.h>

static struct mempool *site_pool;
static uns site_hash_shift, site_hash_size, site_hash_cnt, site_hash_limit, site_hash_need_reload;
static byte *site_hash_state;
static struct site **site_hash;
static void (*site_constructor)(struct site *site);

static void
site_hash_alloc(void)
{
  site_hash_size = 1 << (32-site_hash_shift);
  site_hash_limit = site_hash_size;
  site_hash = xmalloc_zero(site_hash_size * sizeof(struct site *));
}

static void
site_empty_constructor(struct site *site UNUSED)
{
}

void
site_hash_init(void (*constructor)(struct site *site))
{
  site_hash_cnt = 0;
  site_hash_need_reload = 0;
  site_hash_shift = 28;
  site_hash_state = NULL;
  site_hash_alloc();
  site_pool = mp_new(65536);
  site_constructor = constructor ? : site_empty_constructor;
}

static inline uns
site_hash_fn(struct site_fp *fp)
{
  return fp->x[0] >> site_hash_shift;
}

struct site *
site_create(struct site_fp *fp, uns proto, byte *host, uns port)
{
  uns h = site_hash_fn(fp);
  struct site *site = mp_alloc_zero(site_pool, sizeof(*site) + strlen(host));

  site->fp = *fp;
  site->norm_fp = *fp;
  site->proto = proto;
  strcpy(site->hostname, host);
  site->port = port;
  site_constructor(site);

  site->hash_next = site_hash[h];
  site_hash[h] = site;
  if (unlikely(++site_hash_cnt > site_hash_limit))
    {
      site_hash_shift--;
      struct site **oldh = site_hash, *s, *n;
      uns oldn = site_hash_size;
      site_hash_alloc();
      for (uns i=0; i<oldn; i++)
	for (s=oldh[i]; s; s=n)
	  {
	    n = s->hash_next;
	    h = site_hash_fn(&s->fp);
	    s->hash_next = site_hash[h];
	    site_hash[h] = s;
	  }
      xfree(oldh);
    }

  return site;
}

struct site *
site_lookup(struct site_fp *fp)
{
  for (struct site *s = site_hash[site_hash_fn(fp)]; s; s=s->hash_next)
    if (!memcmp(&s->fp, fp, sizeof(struct site_fp)))
      return s;
  return NULL;
}

struct site *
site_next(struct site *site)
{
  uns h = 0;

  if (site)
    {
      if (site->hash_next)
	return site->hash_next;
      h = site_hash_fn(&site->fp) + 1;
    }
  while (h < site_hash_size)
    {
      if (site_hash[h])
	return site_hash[h];
      h++;
    }
  return NULL;
}

void
site_delete(struct site *site)
{
  for (struct site **sp = &site_hash[site_hash_fn(&site->fp)]; *sp; sp=&(*sp)->hash_next)
    if (*sp == site)
      {
	*sp = site->hash_next;
	return;
      }
  ASSERT(0);
}

/*** The site list file ***/

static void
site_put(struct fastbuf *f, struct site *s, byte *hostname)
{
  struct site_list_entry e;
  ASSERT(*hostname);
  e.fp = s->fp;
  e.num_active = s->num_active;
  e.num_inactive = s->num_inactive;
  e.num_gathered = s->num_gathered;
  e.num_oscillations = s->num_oscillations;
  e.num_fresh = s->num_fresh;
  e.skey = s->skey;
  e.norm_fp = s->norm_fp;
  e.port = s->port;
  e.proto = s->proto;
  e.avg_download_time = s->avg_download_time;
  e.error_cycles = s->error_cycles;
  e.rfu = 0;
  bwrite(f, &e, sizeof(e));
  bputs0(f, hostname);
}

void
site_hash_save(byte *state)
{
  struct site *s;
  struct fastbuf *f = temp_state_file();
  uns cnt = 0;

  bputl(f, SITE_LIST_MAGIC);

  /* Take names if needed */
  if (site_hash_need_reload)
    {
      ASSERT(site_hash_state);
      for (s = NULL; s = site_next(s); )
        s->flags &= ~SITE_WRITTEN;
      struct site_list_entry e;
      struct fastbuf *org = read_state_file(site_hash_state, "sites");
      if (bgetl(org) != SITE_LIST_MAGIC)
	ASSERT(0);
      while (breadb(org, &e, sizeof(e)))
        {
          byte host[MAX_URL_SIZE];
          struct site *s = site_lookup(&e.fp);
          if (!bgets0(org, host, sizeof(host)))
	    ASSERT(0);
          if (!s || *s->hostname)
	    continue;
	  s->flags |= SITE_WRITTEN;
	  site_put(f, s, host);
	  cnt++;
        }
      bclose(org);
      for (s = NULL; s = site_next(s); )
        if (!(s->flags & SITE_WRITTEN))
	  site_put(f, s, s->hostname), cnt++;
    }
  else
    for (s = NULL; s = site_next(s); )
      site_put(f, s, s->hostname), cnt++;

  put_state_file(state, "sites", f, 0);
  DBG("Wrote %d sites", cnt);
}

void
site_hash_load(byte *state, uns flags)
{
  ASSERT(state);
  ASSERT(!site_hash_cnt);
  struct fastbuf *f = read_state_file(state, "sites");
  struct site_list_entry e;
  uns cnt = 0;
  byte host[MAX_URL_SIZE];

  if (bgetl(f) != SITE_LIST_MAGIC)
    die("Site list file: Invalid magic number");
  while (breadb(f, &e, sizeof(e)))
    {
      struct site *s = site_lookup(&e.fp);
      if (s)
	die("Corrupted site list file %s: duplicate record found", f->name);
      if (!bgets0(f, host, sizeof(host)))
	die("Corrupted site list file %s: missing host name", f->name);
      s = site_create(&e.fp, e.proto, (flags & SITE_HASH_NO_URLS) ? (byte *)"" : host, e.port);
      s->num_active = e.num_active;
      s->num_inactive = e.num_inactive;
      s->num_gathered = e.num_gathered;
      s->num_oscillations = e.num_oscillations;
      s->num_fresh = e.num_fresh;
      s->avg_download_time = e.avg_download_time;
      s->error_cycles = e.error_cycles;
      s->skey = e.skey;
      s->norm_fp = e.norm_fp;
      cnt++;
      if (flags & SITE_HASH_FILTER)
	site_filter(s, host);
    }
  site_hash_need_reload = cnt && (flags & SITE_HASH_NO_URLS);
  site_hash_state = mp_strdup(site_pool, state);
  bclose(f);
  DBG("Read %d sites", cnt);
}

void
site_hash_cleanup(void)
{
  xfree(site_hash);
  mp_delete(site_pool);
  site_pool = NULL;
  site_hash = NULL;
  site_constructor = NULL;
  site_hash_state = NULL;
}

void
site_hash_reset(void)
{
  if (site_hash)
    {
      struct site *delete_list = NULL;
      for (struct site *site = NULL; site = site_next(site); )
	if (site->flags & SITE_TEMP)
	  {
	    site->u.delete_next = delete_list;
	    delete_list = site;
	  }
        else
	  bzero(&site->u, sizeof(site->u));
      while (delete_list)
        {
	  struct site *s = delete_list;
	  delete_list = s->u.delete_next;
	  site_delete(s);
	}
    }
}

void
maybe_load_sites(byte *state, int filter)
{
  static int sites_filtered;
  if (site_hash && (!site_hash_state || strcmp(site_hash_state, state)))
    site_hash_cleanup();
  if (!site_hash_state)
    {
      site_hash_init(NULL);
      site_hash_load(state, 0);
    }
  if (filter && !sites_filtered)
    {
      site_hash_filter();
      sites_filtered = 1;
    }
}

int
num_sites(void)
{
  return site_pool ? (int)site_hash_cnt : -1;
}
