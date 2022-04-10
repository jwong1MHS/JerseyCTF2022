/*
 *	Sherlock Gatherer Daemon -- Processing of References
 *
 *	(c) 1997--2003 Martin Mares <mj@ucw.cz>
 *	(c) 2001 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/bitsig.h"
#include "ucw/string.h"
#include "filter/filter.h"
#include "gather/daemon/gatherd.h"

#include <string.h>
#include <setjmp.h>

/* Filtering of references: the same filter rules as the gatherer filter, but different bindings */

static struct filter_binding rf_bindings[] = {
  /* URL and its parts */
  { "url",		OFFSETOF(struct rfilter_data, url) },
  { "protocol",		OFFSETOF(struct rfilter_data, url_s.protocol) },
  { "host",		OFFSETOF(struct rfilter_data, url_s.host) },
  { "port",		OFFSETOF(struct rfilter_data, url_s.port) },
  { "path",		OFFSETOF(struct rfilter_data, url_s.rest) },
  { "username",		OFFSETOF(struct rfilter_data, url_s.user) },
  { "password",		OFFSETOF(struct rfilter_data, url_s.pass) },
  /* Gatherer attributes */
  { "content_type",	OFFSETOF(struct rfilter_data, content_type) },
  { "content_encoding",	OFFSETOF(struct rfilter_data, content_encoding) },
  /* Sections and limits */
  { "section",		OFFSETOF(struct rfilter_data, section) },
  { "section_soft_max",	OFFSETOF(struct rfilter_data, section_soft_max) },
  { "section_hard_max",	OFFSETOF(struct rfilter_data, section_hard_max) },
  /* Queue bonus */
  { "queue_bonus",	OFFSETOF(struct rfilter_data, queue_bonus) },
  /* This one gets patched out if kill_qkeys is set */
  { "queue_key",	OFFSETOF(struct rfilter_data, qkey) },
  { NULL,		0 }
};

static struct filter *rf_filter;
static struct filter_args *rf_filter_args;
static struct mempool *rf_pool;

static void
ref_filter_init(uns kill_qkeys)
{
  if (kill_qkeys)
    {
      uns i=0;
      while (rf_bindings[i].offset != OFFSETOF(struct rfilter_data, qkey))
	i++;
      rf_bindings[i].name = NULL;
    }
  rf_filter = filter_load(gather_filter_name, filter_builtin_vars, rf_bindings, NULL);
  rf_filter_args = filter_intr_new(rf_filter);
  rf_pool = mp_new(4096);
}

byte *
ref_filter(struct rfilter_data *r)
{
  uns err;

  mp_flush(rf_pool);
  r->section = 0;
  r->queue_bonus = 0;
  r->section_soft_max = soft_max_obj_count;
  r->section_hard_max = hard_max_obj_count;
  if (err = url_canon_split(r->url, r->buf1, r->buf2, &r->url_s))
    return url_error(err);
  r->content_type = r->content_encoding = NULL;
  guess_content_by_name(r->url_s.rest, &r->content_type, &r->content_encoding);
  r->url_key = url_key(r->url, r->kbuf);

  if (rf_filter)
    {
      struct filter_args *a = rf_filter_args;
      a->pool = rf_pool;
      a->raw = r;
      a->attr = NULL;
      if (!filter_intr_run(a))
	return a->msg ? : (byte *) "Filtered out";
    }

  if (r->content_encoding && !identify_content_encoding(r->content_encoding))
    return "Unknown content encoding";
  if (r->content_type && !identify_content_type(r->content_type))
    return "Unknown content type";

  return NULL;
}

/* URL Trickster: Filtering of references using bit signature arrays */

static struct bitsig *trickster_bitsig;
static uns known_url_count, trickster_limit;

static void
count_known_urls(struct qhost *h)
{
  uns i;

  for (i=0; i<SHERLOCK_NUM_SECTIONS; i++)
    known_url_count += h->obj_count[i];
}

static void
trickster_rebuild(void)
{
  uns n, colls;
  byte url[MAX_URL_SIZE], kbuf[URL_KEY_BUF_SIZE];
  struct urlrec ur;

  do
    {
      if (trickster_bitsig)
	bitsig_free(trickster_bitsig);
      trickster_limit = known_url_count + trickster_step;
      trickster_bitsig = bitsig_init(trickster_err_prob, trickster_limit);
      urldb_rewind();
      n = colls = 0;
      while (urldb_get_next(url, &ur))
	{
	  n++;
	  if (n <= trickster_limit)
	    {
	      byte *key = url_key(url, kbuf);
	      if (bitsig_insert(trickster_bitsig, key))
		colls++;
	    }
	}
      if (n != known_url_count)
	{
	  log(L_ERROR, "Initial URL count estimate based on host counters was wrong (estimated %d, real %d)", known_url_count, n);
	  known_url_count = n;
	}
    }
  while (n > trickster_limit);
  log(L_DEBUG, "Bitsig array built (%d collisions).", colls);
}

void
refs_init(uns quick)
{
  url_key_init();
  ref_filter_init(quick);
  if (!quick)
    {
      walk_hosts(count_known_urls);
      log(L_INFO, "Discovered %d known URL's", known_url_count);
      if (trickster_err_prob)
	trickster_rebuild();
    }
}

void
add_ref(byte *r, uns flags)
{
  byte buf0[MAX_URL_SIZE], buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], url[MAX_URL_SIZE], urlx[MAX_URL_SIZE];
  struct url u;
  uns err, watermark;
  struct qhost *h;
  struct urlrec ur;
  int nw;
  int add_root = 0;
  struct rfilter_data rfdata;
  char *m, *words[3];

  if (ignore_refs && !(flags & URF_INITIAL)) /* If ignore_refs is set, accept only URL's listed initially */
    return;

  strcpy(buf0, r);			/* Remove ref-id and check ignoration flag */
  r = buf0;
  nw = str_wordsplit(r, words, 3);
  ASSERT(nw >= 0);
  if (nw > 2 && strcmp(words[2], "0"))
    {
      if (trace_refs)
	log(L_INFO, "Ref: %s: To be ignored", r);
      return;
    }

  if (err = url_canon_split(r, buf1, buf2, &u))
    {
      if (log_ref_errors)
	log(L_ERROR, "Ref: %s: %s", r, url_error(err));
      return;
    }

  if (!u.protoid || u.protoid >= URL_PROTO_MAX)
    {
      if (log_ref_errors)
	log(L_INFO, "Ref: %s: Unknown protocol", r);
      return;
    }

restart:
  if ((err = url_pack(&u, urlx)) || (err = url_enescape(urlx, url)))
    {
      log(L_ERROR, "Ref: %s: %s", r, url_error(err));
      return;
    }

  rfdata.url = url;
  rfdata.qkey = F_UNDEF_INT;
  if (m = ref_filter(&rfdata))
    {
      if (trace_refs)
	log(L_INFO, "Ref: %s: %s", url, m);
      return;
    }

  if (!(flags & URF_INITIAL) && trickster_bitsig && bitsig_member(trickster_bitsig, rfdata.url_key))
    {
      if (trace_refs > 1)
	log(L_DEBUG, "Ref: %s: Considered known with key %s", url, rfdata.url_key);
      return;
    }

  if (urldb_lookup(url, &ur))		/* Consult the URL database */
    {
      if (trace_refs > 1)
	log(L_DEBUG, "Ref: %s: Already known (id=%08x, q=%d, age=%d)",
	    url, ur.oid, ur.access, now-ur.access);
      if ((ur.flags & flags) != flags)
	{
	  ur.flags |= flags;
	  urldb_store(url, &ur);
	}
      return;
    }

  h = find_host(u.protoid, u.host, u.port);
  if (!h)
    h = new_host(u.protoid, u.host, u.port);
  if (!h->obj_count && auto_enqueue_root && strcmp(u.rest, "/"))
    add_root = 1;

  if (h->obj_count[rfdata.section] >= rfdata.section_hard_max)
    {
      watermark = 2;
      m = "dropped";
    }
  else if (h->obj_count[rfdata.section] >= rfdata.section_soft_max)
    {
      watermark = 1;
      m = "not queued";
    }
  else
    {
      watermark = 0;
      m = "queued";
    }
  if (trace_refs > 1)
    log(L_DEBUG, "Ref: %s: %s (%d of %d/%d in section %d)",
	url, m, h->obj_count[rfdata.section]+1, rfdata.section_soft_max, rfdata.section_hard_max, rfdata.section);

  if (watermark <= 1)
    {
      known_url_count++;
      h->obj_count[rfdata.section]++;
      touch_host(h);
      ur.access = now;
      ur.oid = OID_UNDEFINED;
      ur.http_last_mod = 0;
      ur.avg_change_time = 0;
      ur.flags = flags | URF_QUEUED;
      ur.retries = 0;
      urldb_store(url, &ur);
      if (trickster_bitsig)
	{
	  if (known_url_count > trickster_limit)
	    trickster_rebuild();
	  bitsig_insert(trickster_bitsig, rfdata.url_key);
	}
      if (!watermark)
	enqueue_item(h, u.rest, rfdata.queue_bonus);	/* Kick it to the queue */
    }

  put_host(h);

  if (add_root)				/* Add also server root if requested to do so */
    {
      strcpy(u.rest, "/");
      add_root = 0;
      goto restart;
    }
}
