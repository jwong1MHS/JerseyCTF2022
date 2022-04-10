/*
 *	Sherlock Shepherd Daemon -- Merging of new URL's and pruning of old ones
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/shepherd.h"
#include "ucw/mempool.h"
#include "ucw/clists.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#define SORT_PREFIX(x) contrib_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_UNIFY
#define SORT_HASH_FP
static inline int
contrib_compare(struct url_state *x, struct url_state *y)
{
  int r = fp_cmp(&x->fp, &y->fp);
  if (r)
    return r;
  REV_COMPARE(x->weight, y->weight);
  return 0;
}
#include "gather/shepherd/index-sort.h"

#define SORT_PREFIX(x) merge_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_HASH_BITS 32
static inline int
merge_compare(struct url_state *x, struct url_state *y)
{
  /* Sort by normalized footprints */
  struct site *xsite = site_lookup(&x->fp.site);
  struct site *ysite = site_lookup(&y->fp.site);
  ASSERT(xsite && ysite);
  int r;
  if (r = site_fp_cmp(&xsite->norm_fp, &ysite->norm_fp))
    return r;
  return rest_fp_cmp(&x->fp.rest, &y->fp.rest);
}

static inline uns
merge_hash(struct url_state *x)
{
  struct site *site = site_lookup(&x->fp.site);
  ASSERT(site);
  return site->norm_fp.x[0];
}
#include "gather/shepherd/index-sort.h"

static struct fastbuf *
scan_contrib(byte *old_state)
{
  struct fastbuf *cont = read_state_file(old_state, "contrib");
  struct fastbuf *out = temp_state_file();
  struct contrib c;
  struct site *site;
  uns in_cnt = 0, new_site_cnt = 0;
  time_t now = time(NULL);

  for(;;)
    {
      ucw_off_t pos = btell(cont);
      ASSERT(!(pos & (CONTRIB_ALIGN - 1)));
      if (!breadb(cont, &c, sizeof(c)))
	break;
      byte url[MAX_URL_SIZE];
      ASSERT(c.url_len < MAX_URL_SIZE);
      url[breadb(cont, url, c.url_len)] = 0;
      uns len = sizeof(c) + c.url_len;
      bskip(cont, ALIGN_TO(len, CONTRIB_ALIGN) - len);
      in_cnt++;
      if (!(site = site_lookup(&c.fp.site)))
	{
	  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
	  struct url ur;
	  int err = url_canon_split(url, buf1, buf2, &ur);
	  ASSERT(!err);
	  ASSERT(ur.protoid);
	  site = site_create(&c.fp.site, ur.protoid, ur.host, ur.port);
	  new_site_cnt++;
	  if (auto_go_root && memcmp(&c.fp.rest, &ROOT_FOOTPRINT, sizeof(struct rest_fp)))
	    {
	      /* Automatically add site root URL if requested */
	      struct url_state s;
	      bzero(&s, sizeof(s));
	      s.fp.site = c.fp.site;
	      s.fp.rest = ROOT_FOOTPRINT;
	      s.oid = OID_UNDEFINED;
	      s.weight = c.weight;
	      s.flags = USF_NEEDED_BY_EQ | USF_CONTRIB;
	      s.type = UTYPE_NEW;
	      s.last_seen = now;
	      s.area = c.area;
	      bwrite(out, &s, sizeof(s));
	    }
	}

      struct url_state s;
      bzero(&s, sizeof(s));
      s.fp = c.fp;
      s.oid = pos >> CONTRIB_SHIFT;
      s.weight = c.weight;
      s.flags = c.flags | USF_CONTRIB;
      s.type = UTYPE_NEW;
      s.last_seen = now;
      s.section = c.section;
      s.area = c.area;
      bwrite(out, &s, sizeof(s));
    }
  bclose(cont);
  bflush(out);
  bsetpos(out, 0);
  log(L_INFO, "Found %d contributions and %d new sites", in_cnt, new_site_cnt);
  return out;
}

struct record {
  struct site *site;
  struct url_state state;
};

static uns cnt_sacred, cnt_contrib, cnt_normal;
static uns cnt_dups, cnt_regather;
static struct record *block_buf, **block_ptr;
static uns block_len, block_id, block_limit = 1024;

static inline int
filtering_cmp(struct record *x, struct record *y)
{
  /* First of all contributions go last */
  if (x->state.flags & USF_CONTRIB)
    {
      if (!(y->state.flags & USF_CONTRIB))
	return 1;
    }
  else if (y->state.flags & USF_CONTRIB)
    return -1;

  /* Then sacred URL's go first */
  if (USF_IS_SACRED(x->state.flags))
    {
      if (!USF_IS_SACRED(y->state.flags))
	return -1;
    }
  else if (USF_IS_SACRED(y->state.flags))
    return 1;

  /* Then zombies go last */
  if (ustate_type(&x->state) == UTYPE_ZOMBIE)
    {
      if (ustate_type(&y->state) != UTYPE_ZOMBIE)
	return 1;
    }
  else if (ustate_type(&y->state) == UTYPE_ZOMBIE)
    return -1;

  /* Then weight decides (which includes "already gathered" status) */
  REV_COMPARE(x->state.weight, y->state.weight);

  /* Finally, the original FP will decide */
  int r;
  if (r = site_fp_cmp(&x->state.fp.site, &y->state.fp.site))
    return r;

  return 0;
}

#define ASORT_PREFIX(x) filtering_##x
#define ASORT_KEY_TYPE struct record *
#define ASORT_ELT(i) block_ptr[(i)]
#define ASORT_LT(x,y) (filtering_cmp((x),(y)) < 0)
#include "ucw/sorter/array-simple.h"

static void
filtering_block(struct fastbuf *dest)
{
  DBG("Filtering block id=%d len=%d", block_id, block_len);

  /* Sort all records in the current block  */
  struct record *rec = block_buf;
  for (uns i = 0; i < block_len; i++, rec++)
    block_ptr[i] = rec;
  filtering_sort(block_len);

  /* Filter unwanted records */
  uns len = 0;
  for (uns i = 0; i < block_len; i++)
    {
      rec = block_ptr[i];
      struct url_state *s = &rec->state;
      struct site *site = rec->site;

      /* Contriburion record */
      if (s->flags & USF_CONTRIB)
	{
	  /* Contributions with USF_REGATHER overwrite zombies */
	  uns last_index;
	  if ((s->flags & USF_REGATHER) && site->u.merge.last_written == block_id &&
	      ustate_type(&block_ptr[last_index = site->u.merge.last_index]->state)  == UTYPE_ZOMBIE)
	    {
	      DBG("Regathering zombie");
	      struct record *last = block_ptr[last_index];
	      if (USF_IS_SACRED(last->state.flags))
		cnt_sacred--;
	      else
		cnt_normal--;
	      cnt_regather++;
	      state_log(site, &last->state, LOG_SRC_MERGE, LOG_MERGE_REGATHER, 0, last->state.weight);
	      block_ptr[last_index] = rec;
	  }
	  /* Non-sacred contribs survive only if there are no other records */
	  else if (len && !USF_IS_SACRED(s->flags))
	    {
	      state_log(site, s, LOG_SRC_MERGE, LOG_MERGE_DUP_CONTRIB, 0, s->weight);
	      continue;
	    }
	  else
	    cnt_contrib++;
	  if (!memcmp(&s->fp.rest, &ROBOTS_TXT_FOOTPRINT, sizeof(struct rest_fp)))
	    s->flags |= USF_ROBOTS;
	  else if (!memcmp(&s->fp.rest, &ROOT_FOOTPRINT, sizeof(struct rest_fp)))
	    s->flags |= USF_NEEDED_BY_EQ;
	  state_log(site, s, LOG_SRC_MERGE, LOG_MERGE_CONTRIB, 0, s->weight);
	}
      /* Sacred record from the original index (including sacred zombies) */
      else if (USF_IS_SACRED(s->flags))
	cnt_sacred++;
      /* Duplicate record in the original index */
      else if (len)
	{
	  state_log(site, s, LOG_SRC_MERGE, LOG_MERGE_DUP, 0, s->weight);
	  cnt_dups++;
	  continue;
	}
      /* First FP record from the original index, if it is not sacred */
      else
	cnt_normal++;
      /* Set the selection priority flag */
      s->flags = (s->flags & ~USF_SELECT_PRIORITY) | (site->select_bonus ? USF_SELECT_PRIORITY : 0);
      /* This is a little trick to avoid writing the same FP twice.
       * Does nothing if regathering a zombie record. */
      if (site->u.merge.last_written != block_id)
	{
	  site->u.merge.last_written = block_id;
	  site->u.merge.last_index = len;
	  block_ptr[len++] = rec;
	}
    }

  DBG("Writing %d records", len);
  /* Flush records */
  for (uns i = 0; i < len; i++)
    bwrite(dest, &block_ptr[i]->state, sizeof(struct url_state));
}

static void
filtering(struct fastbuf *dest, struct fastbuf *src)
{
  struct footprint last_nfp;
  struct url_state s;
  block_buf = xmalloc(block_limit * sizeof(*block_buf));
  block_ptr = xmalloc(block_limit * sizeof(*block_ptr));
  while (breadb(src, &s, sizeof(s)))
    {
      ASSERT(!(ustate_type(&s) == UTYPE_ZOMBIE && (USF_IS_SACRISIMMUS(s.flags) || (s.flags & USF_CONTRIB))));
      struct site *site = site_lookup(&s.fp.site);
      if (!block_id ||
	  memcmp(&last_nfp.site, &site->norm_fp, sizeof(struct site_fp)) ||
	  memcmp(&last_nfp.rest, &s.fp.rest, sizeof(struct rest_fp)))
        {
	  if (block_id)
	    filtering_block(dest);
	  last_nfp.site = site->norm_fp;
	  last_nfp.rest = s.fp.rest;
	  block_id++;
	  block_len = 0;
	}
      if (unlikely(block_len == block_limit))
        {
	  block_limit <<= 1;
	  block_buf = xrealloc(block_buf, block_limit * sizeof(*block_buf));
	  block_ptr = xrealloc(block_ptr, block_limit * sizeof(*block_ptr));
	}
      block_buf[block_len].state = s;
      block_buf[block_len].site = site;
      block_len++;
    }
  if (block_id)
    filtering_block(dest);
  xfree(block_buf);
  xfree(block_ptr);
  bflush(dest);
  log(L_INFO, "Done: %d sacred entries, %d new contribs, %d others", cnt_sacred, cnt_contrib, cnt_normal);
  log(L_INFO, "Deleted %d duplicates, regathered %d zombies", cnt_dups, cnt_regather);
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-merge [<config-options>] <state>\n");
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
    // Needed for the `monitor' flag, `select_bonus' and pruning of rejected sites
  state_log_open(state);

  log(L_INFO, "Scanning contributions");
  struct fastbuf *contrib_index = scan_contrib(state);

  log(L_INFO, "Sorting contributions");
  // FIXME: add append support to directfb
  struct fastbuf *f = contrib_sort(contrib_index, NULL);
  contrib_index = bopen(f->name, O_RDWR, 65536);
  bclose(f);

  log(L_INFO, "Merging contributions");
  struct fastbuf *orig_index = read_state_file(state, "index");
  bseek(contrib_index, 0, SEEK_END);
  bbcopy_slow(orig_index, contrib_index, ~0U);
  bflush(contrib_index);
  bsetpos(contrib_index, 0);
  bclose(orig_index);

  log(L_INFO, "Sorting index");
  contrib_index = merge_sort(contrib_index, NULL);

  log(L_INFO, "Filtering");
  struct fastbuf *new_index = temp_state_file();
  filtering(new_index, contrib_index);
  bclose(contrib_index);
  put_state_file(state, "index", new_index, STATE_FLAG_SORTED);

  log(L_INFO, "Writing new site list");
  site_hash_save(state);
  state_log_close();

  return 0;
}
