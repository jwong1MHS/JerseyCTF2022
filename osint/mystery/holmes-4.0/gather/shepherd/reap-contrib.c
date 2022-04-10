/*
 *	Sherlock Shepherd Reaper -- Contributions
 *
 *	(c) 2003--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/string.h"
#include "filter/filter.h"
#include "gather/gather.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/reap.h"

static struct fastbuf *contrib;

static struct filter_binding cfilter_bindings[] = {
  /* URL and its parts */
  { "url",		OFFSETOF(struct cfilter_data, url) },
  { "protocol",		OFFSETOF(struct cfilter_data, url_s.protocol) },
  { "host",		OFFSETOF(struct cfilter_data, url_s.host) },
  { "port",		OFFSETOF(struct cfilter_data, url_s.port) },
  { "path",		OFFSETOF(struct cfilter_data, url_s.rest) },
  { "username",		OFFSETOF(struct cfilter_data, url_s.user) },
  { "password",		OFFSETOF(struct cfilter_data, url_s.pass) },
  /* Gatherer attributes */
  { "content_type",	OFFSETOF(struct cfilter_data, content_type) },
  { "content_encoding",	OFFSETOF(struct cfilter_data, content_encoding) },
  { "error_code",	OFFSETOF(struct cfilter_data, error_code) },
  { "user_mark",	OFFSETOF(struct cfilter_data, user_mark) },
  /* Section */
  { "section",		OFFSETOF(struct cfilter_data, section) },
#ifdef CONFIG_AREAS
  /* Area */
  { "area",		OFFSETOF(struct cfilter_data, area) },
#endif
  /* Stable time */
  { "stable_time",	OFFSETOF(struct cfilter_data, stable_time) },
  /* Filtering of robots.txt */
  { "filter_robots",	OFFSETOF(struct cfilter_data, filter_robots) },
  /* Transformation of links */
  { "want_xform",	OFFSETOF(struct cfilter_data, want_xform) },
  { "url_xform",	OFFSETOF(struct cfilter_data, url_xform) },
  { "src_url",		OFFSETOF(struct cfilter_data, src_url) },
  { NULL,		0 }
};

static struct filter *cfilter;
static struct filter_args *cfilter_args;
static struct mempool *cfilter_pool;

struct contrib_entry {
  byte weight;
  byte section;
  byte flags;
  byte written;
  area_t area;
  byte url[1];
};

#define HASH_NODE struct contrib_entry
#define HASH_PREFIX(x) chash_##x
#define HASH_KEY_ENDSTRING url
#define HASH_WANT_CLEANUP
#define HASH_WANT_LOOKUP
#define HASH_ZERO_FILL
#define HASH_GIVE_ALLOC
#define HASH_USE_POOL chash_pool

static struct mempool *chash_pool;
static uns chash_total_size;

static inline void *
chash_alloc(uns size)
{
  chash_total_size += size;	/* Deliberately ignoring alignment and mempool overhead */
  return mp_alloc(chash_pool, size);
}

#include "ucw/hashtable.h"

byte *
verify_contrib(struct cfilter_data *d, uns want_xform)
{
  uns err;

  mp_flush(cfilter_pool);
  d->section = 0;
  d->area = AREA_NONE;
  d->error_code = 2307;
  d->user_mark = F_UNDEF_INT;
  d->filter_robots = 0;
  if (err = url_canon_split(d->url, d->buf1, d->buf2, &d->url_s))
    return url_error(err);
  d->content_type = d->content_encoding = NULL;
  guess_content_by_name(d->url_s.rest, &d->content_type, &d->content_encoding);
  d->stable_time = 0;
  d->url_xform = want_xform ? d->url : NULL;
  d->want_xform = 0;

  if (cfilter)
    {
      struct filter_args *a = cfilter_args;
      a->pool = cfilter_pool;
      a->raw = d;
      a->attr = NULL;
      if (!filter_intr_run(a))
	return a->msg ? : (byte *) "Filtered out";
    }

  if (d->content_encoding && !identify_content_encoding(d->content_encoding))
    return "Unknown content encoding";
  if (d->content_type && !identify_content_type(d->content_type))
    return "Unknown content type";

  if (!want_xform)
    d->url_xform = d->url;
  else if (d->url_xform && d->url_xform != d->url && strcmp(d->url_xform, d->url))
    {
      if (err = url_auto_canonicalize(d->url_xform, d->buf3))
	return url_error(err);
      d->url_xform = d->buf3;
    }

  return NULL;
}

void
contrib_flush(void)
{
  uns cnt = 0;
  DBG("Flushing contributions...");
  HASH_FOR_ALL(chash, e)
    {
      if (!e->written)
	{
	  struct contrib c;
	  if (url_footprint(&c.fp, e->url))
	    log(L_ERROR, "Unable to construct fingerprint for URL %s which passed the filters", e->url);
	  else
	    {
	      ASSERT(!(btell(contrib) & (CONTRIB_ALIGN-1)));
	      c.url_len = strlen(e->url);
	      c.weight = e->weight;
	      c.section = e->section;
	      c.area = e->area;
	      c.flags = e->flags;
	      bwrite(contrib, &c, sizeof(c));
	      bwrite(contrib, e->url, c.url_len);
	      static byte contrib_padding[CONTRIB_ALIGN];
	      uns len = sizeof(c) + c.url_len;
	      bwrite(contrib, contrib_padding, ALIGN_TO(len, CONTRIB_ALIGN) - len);
	      ASSERT(btell(contrib) < (ucw_off_t)0xff000000 << CONTRIB_SHIFT);
	      cnt++;
	    }
	  e->written = 1;
	}
    }
  HASH_END_FOR;
  log(L_INFO, "Wrote %d contributions", cnt);
}

static void
contrib_reset(void)
{
  contrib_flush();
  mp_flush(chash_pool);
  chash_cleanup();
  chash_init();
  chash_total_size = 0;
}

byte *
add_contrib(byte *url, byte *src_url, uns want_xform, int weight, uns flags, int section, area_t area UNUSED, area_t parent_area UNUSED)
{					/* Remember to canonicalize the URL before calling this function! */
  struct cfilter_data cfd;
  struct contrib_entry *e;
  byte *msg;

  cfd.url = url;
  cfd.src_url = src_url;
  if (msg = verify_contrib(&cfd, want_xform))
    return msg;

  if (chash_total_size >= contrib_cache_size)
    contrib_reset();

  e = chash_lookup(cfd.url_xform);

#ifdef CONFIG_AREAS
  if (area == AREA_ANY)
    area = cfd.area;
  if (parent_area != AREA_ANY && area != parent_area)
    return "Different area";
  if (e->area != area)
    {
      e->area = area;
      e->written = 0;
    }
#endif

  if (e->weight < weight)
    {
      e->weight = weight;
      e->written = 0;
    }

  if (section < 0)
    section = cfd.section;
  if (e->section != section || e->flags != flags)
    {
      e->section = section;
      e->flags = flags;
      e->written = 0;
    }

  return "OK";
}

void
write_contribs_obj(struct odes *o, uns want_xform, int parent_weight, area_t parent_area)
{
  struct oattr *a;
  char *attrs = "FRYdI";
  char *src_url = obj_find_aval(o, 'U');

  if (ignore_refs == 2) /* Ignore all references */
    return;
  else if (ignore_refs == 1) /* Ignore all references except "redirected to" */
    attrs = "Y";
  int weight = parent_weight - contrib_gap;
  weight = MAX(0, weight);
  while (*attrs)
    for (a=obj_find_attr(o, *attrs++); a; a=a->same)
      {
	char *w[3];
	int nw = str_wordsplit(a->val, w, 3);
	ASSERT(nw > 0);
	char *m;
	if (nw > 2 && strcmp(w[2], "0"))
	  m = "ignored";
	else
	  m = add_contrib(w[0], src_url, want_xform, weight, 0, -1, AREA_ANY, parent_area);
	if (trace_refs)
	  log(L_INFO, "Ref: %s: %s", w[0], m);
      }
}

void
write_contribs(struct job *j)
{
  write_contribs_obj(j->ctrl, j->want_xform, j->plane->weight, j->plane->area);
}

void
contrib_init(byte *new_state)
{
  if (new_state)
    {
      contrib = append_state_file(new_state, "contrib");
      chash_pool = mp_new(65536);
      chash_init();
    }
  cfilter = filter_load(shepherd_filter_name, filter_builtin_vars, cfilter_bindings, NULL);
  cfilter_args = filter_intr_new(cfilter);
  cfilter_pool = mp_new(4096);
}

void
contrib_cleanup(void)
{
  contrib_flush();
  bclose(contrib);
}

ucw_off_t
contrib_checkpoint(void)
{
  contrib_flush();
  bfilesync(contrib);
  return btell(contrib);
}
