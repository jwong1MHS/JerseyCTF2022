/*
 *	Sherlock Shepherd Daemon -- Corking the State
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define SORT_PREFIX(x) cork_index_##x
#define SORT_INPUT_FILE
#define SORT_OUTPUT_FB
#define SORT_UNIQUE
#define SORT_BY_FP
#include "gather/shepherd/index-sort.h"

#define SORT_PREFIX(x) cork_journal_##x
#define SORT_INPUT_FILE
#define SORT_OUTPUT_FB
#define SORT_HASH_BITS 32
static inline int
cork_journal_compare(struct url_state *x, struct url_state *y)
{
  int r = fp_cmp(&x->fp, &y->fp);
  if (r)
    return r;
  REV_COMPARE(x->last_seen, y->last_seen);
  return 0;
}

static inline uns
cork_journal_hash(struct url_state *x)
{
  return x->fp.site.x[0];
}
#include "gather/shepherd/index-sort.h"

static void
cork_site_init(struct site *s)
{
  s->u.cork.total_download_time = 0;
  s->u.cork.entries_seen = 0;
  s->u.cork.update_mask = 0;
}

/* Calculation of average download times */

static inline void
site_update_dt(struct site *s, uns time)
{
  /* XXX: This could overflow on sites which are both very large and very slow. Not likely. */
  s->u.cork.total_download_time += time;
  s->u.cork.entries_seen++;
}

static void
site_eval_dt(void)
{
  struct site *s = NULL;
  while (s = site_next(s))
    s->avg_download_time = (s->u.cork.entries_seen ? (s->u.cork.total_download_time / s->u.cork.entries_seen) : 0);
}

/* Determining site errors */

struct qkey_hash {
  u64 qkey;
  u32 update_mask;
};

#define HASH_NODE struct qkey_hash
#define HASH_PREFIX(x) qkey_##x
#define HASH_KEY_ATOMIC qkey
#define HASH_ATOMIC_TYPE u64
#define HASH_WANT_LOOKUP
#define HASH_ZERO_FILL
#define HASH_AUTO_POOL 16384
#include "ucw/hashtable.h"

static void
site_eval_errors(void)
{
  struct site *s = NULL;
  qkey_init();
  while (s = site_next(s))
    qkey_lookup(site_qkey(s))->update_mask |= s->u.cork.update_mask;
  while (s = site_next(s))
    {
      u32 conn_mask = qkey_lookup(site_qkey(s))->update_mask;
      u32 site_mask = s->u.cork.update_mask;
      if ((conn_mask & (1 << ERRT_TEMP_CONNECTION)) && !(conn_mask & (1 << ERRT_NONE)) ||
	  (site_mask & (1 << ERRT_TEMP_SITE)) && !(site_mask & (1 << ERRT_NONE)))
	s->error_cycles++;
      else
	s->error_cycles = 0;
      DBG("Site %08x%08x: conn_mask=%04x site_mask=%04x ec=%d", FP_PAIR(s->fp), conn_mask, site_mask, s->error_cycles);
      /* Here we only set the error counter, the rest is handled by shep-select */
    }
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-cork [<config-options>] <state>\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc-1)
    usage();

  byte *state = argv[optind];

  site_hash_init(cork_site_init);
  site_hash_load(state, SITE_HASH_NO_URLS);

  struct fastbuf *oidx;
  if (state_flags_get(state) & STATE_FLAG_SORTED)
    oidx = read_state_file(state, "index");
  else
    {
      log(L_INFO, "Sorting original index");
      oidx = cork_index_sort(state_file_name(state, "index"), NULL);
    }

  log(L_INFO, "Sorting reaper's journal");
  struct fastbuf *jour = cork_journal_sort(state_file_name(state, "journal"), NULL);

  log(L_INFO, "Applying changes from the journal");
  struct fastbuf *nidx = temp_state_file();
  struct url_state os, js, s;
  uns on = breadb(oidx, &os, sizeof(os));
  uns jn = breadb(jour, &js, sizeof(js));
  uns cnt = 0;
  while (on || jn)
    {
      int cmp;
      if (!on)
	cmp = 1;
      else if (!jn)
	cmp = -1;
      else
	cmp = fp_cmp(&os.fp, &js.fp);
      if (cmp < 0)
	{
	  /* Not changed => just copy */
	  bwrite(nidx, &os, sizeof(os));
	  on = breadb(oidx, &os, sizeof(os));
	}
      else
	{
	  struct site *site = site_lookup(&js.fp.site);
	  ASSERT(site);
	  if (!cmp)
	    {
	      /* Updating an existing entry */
	      s = os;
	      on = breadb(oidx, &os, sizeof(os));
	    }
	  else
	    {
	      /* Creating a new entry -- can happen only with robots and skeys */
	      s = js;
	      ASSERT(s.flags & USF_ROBOTS);
	      ASSERT(!memcmp(&s.fp.rest, &SKEY_FOOTPRINT, sizeof(struct rest_fp)) && ustate_type(&s) == UTYPE_SKEY ||
		     !memcmp(&s.fp.rest, &ROBOTS_TXT_FOOTPRINT, sizeof(struct rest_fp)) && ustate_type(&s) != UTYPE_SKEY);
	    }
	  /* Process all updates */
	  uns dup = 0;
	  do
	    {
	      /* There should not be duplicates in the journal (except SKeys and temporary errors) */
	      ASSERT(ustate_type(&js) == UTYPE_SKEY || ustate_type(&js) == UTYPE_TEMP_ERROR || !dup++);

	      cnt++;
	      ASSERT(js.oid != OID_UNDEFINED);
	      s.oid = js.oid;
	      if (ustate_type(&js) == UTYPE_TEMP_ERROR)
		{
		  s.retry_count = js.retry_count;
		  site->u.cork.update_mask |= 1 << js.section;
		}
	      else
		{
		  if (ustate_type(&js) != UTYPE_SKEY && !s.retry_count)
		    site->u.cork.update_mask |= 1 << ERRT_NONE;
		  s.last_seen = js.last_seen;
		  s.retry_count = 0;
		  /* s.weight is kept */
		  s.flags &= ~USF_REGATHER;
		  ustate_set_type(&s, ustate_type(&js));
		  s.stable_time = js.stable_time;
		  s.download_time = js.download_time;
		  ASSERT(!(ustate_type(&s) == UTYPE_ZOMBIE && USF_IS_SACRISIMMUS(s.flags)));
		}
	      jn = breadb(jour, &js, sizeof(js));
	    }
	  while (jn && !fp_cmp(&s.fp, &js.fp));
	  if (ustate_type(&s) == UTYPE_TEMP_ERROR)
	    ustate_set_type(&s, UTYPE_NEW);
	  else if (ustate_type(&s) == UTYPE_SKEY)
	    site->skey = (u32) s.oid;
	  bwrite(nidx, &s, sizeof(s));
	  site_update_dt(site, s.download_time);
	}
    }
  put_state_file(state, "index", nidx, 0);
  state_flags_set(state, STATE_FLAG_SORTED);

  bclose(jour);
  bclose(oidx);
  log(L_INFO, "Processed %d journal entries", cnt);
  site_eval_dt();
  site_eval_errors();
  site_hash_save(state);
  return 0;
}
