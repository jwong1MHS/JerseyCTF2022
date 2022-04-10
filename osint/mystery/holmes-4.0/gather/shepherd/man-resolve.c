/*
 *	Sherlock Shepherd -- Manual Control -- Resolving of Buckets or URL's
 *
 *	(c) 2004--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006--2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "sherlock/bucket.h"
#include "sherlock/object.h"
#include "ucw/mempool.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/man.h"

#include <stdio.h>

#define SORT_PREFIX(x) url_state_by_oid_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_INT(x) (x).oid
#include "gather/shepherd/index-sort.h"

/*** Resolving of URL's ***/

struct odes *resolve_object;

static struct obuck_context obuck_ctx;
static struct fastbuf *current_bucket;
static uns resolve_i, resolve_max;
static uns use_slurp_p;
static uns want_body_p;
static struct url_db *resolve_url_db;
static struct sel_text_src *resolve_text_src;
static int resolve_order;
static struct mempool *resolve_pool;
static struct buck2obj_buf *buck_buf;

struct fastbuf *
resolve_add(struct fastbuf *f, struct url_state *s)
{
  if (unlikely(!f))
    f = bopen_tmp(65536);
  bwrite(f, s, sizeof(*s));
  return f;
}

struct fastbuf *
resolve_go(struct fastbuf *f, int want_body, int order)
{
  want_body_p = want_body;
  brewind(f);
  resolve_max = (uns)(bfilesize(f) / sizeof(struct url_state));
  resolve_i = 0;
  resolve_order = INDEX_ORDER_BY_OID;

  /* Resolve from the sorted URL database */
  if (!want_body && order != INDEX_ORDER_BY_OID && (resolve_text_src = sel_text_try_open_file(url_sorted_file)))
    {
      MAN_VERBOSE("Resolving %u objects from sorted URL database", resolve_max);
      resolve_order = INDEX_ORDER_BY_FP;
    }

  /* Resolve from the URL database */
  else if (!want_body && (resolve_url_db = url_db_open(O_RDONLY, 1)))
    MAN_VERBOSE("Resolving %u objects from URL database", resolve_max);

  /* Resolve from the bucket file */
  else
    {
      MAN_VERBOSE("Resolving %u objects from bucket file", resolve_max);
      bucket_open(0);
      current_bucket = NULL;
      resolve_object = NULL;
      resolve_pool = mp_new(1<<14);
      buck_buf = buck2obj_alloc();
      use_slurp_p = (resolve_max > 10000);
    }

  /* Sort wanted URL states */
  if (resolve_order != order)
    {
      if (resolve_order == INDEX_ORDER_BY_OID)
        {
          DBG("Sorting query objects by OID");
          f = url_state_by_oid_sort(f, NULL, (u32)~0U);
	}
      else
        {
          DBG("Sorting query objects by FP");
          f = url_state_by_fp_sort(f, NULL);
	}
    }
  return f;
}

uns
resolve_non_url(byte *url, struct url_state *s)
{
  if (ustate_type(s) == UTYPE_SKEY)
    {
      u32 k = s->oid;
      sprintf(url, "%d.%d.%d.%d", (k >> 24) & 0xff, (k >> 16) & 0xff, (k >> 8) & 0xff, k & 0xff);
      return 1;
    }
  if (ustate_type(s) == UTYPE_ZOMBIE)
    {
      sprintf(url, "Error %d", s->oid);
      return 1;
    }
  if (s->flags & USF_CONTRIB)
    {
      sprintf(url, "Contrib %08x", s->oid);
      return 1;
    }
  return 0;
}

int
resolve_read(struct fastbuf *f, struct url_state *s, byte *url)
{
  if (current_bucket)
    {
      if (!use_slurp_p)
	bclose(current_bucket);
      current_bucket = NULL;
      resolve_object = NULL;
    }

  if (!breadb(f, s, sizeof(*s)))
    return 0;

  if (man_opt.verbose > 1 && !(++resolve_i % 1024))
    {
      fprintf(stderr, "Resolved %d\r", resolve_i);
      fflush(stderr);
    }

  /* Deal with non-URL objects */
  if (resolve_non_url(url, s))
    return 1;

  /* Resolve from the sorted URL database */
  if (resolve_text_src)
    {
      struct sel_text_record *rec;
      DBG("Seeking footprint %08x%08x:%08x%08x", FP_QUAD(s->fp));
      uns gt;
      if (unlikely(!(rec = sel_text_find_forward(resolve_text_src, &s->fp, &gt)) || gt))
        die("Cannot find footprint %08x%08x:%08x%08x in URL database", FP_QUAD(s->fp));
      byte *u = obj_find_aval(rec->o, 'U');
      if (unlikely(!u))
	die("URL database record %08x%08x:%08x%08x has no URL", FP_QUAD(s->fp));
      strcpy(url, u);
      return 1;
    }

  /* Resolve from the incremental URL database */
  if (resolve_url_db)
    {
      struct url_record *rec;
      DBG("Seeking OID %08x", s->oid);
      uns gt;
      if (unlikely(!(rec = url_db_find_forward(resolve_url_db, s->oid, &gt)) || gt))
        die("Cannot find OID %08x in URL database", s->oid);
      strcpy(url, rec->url);
      return 1;
    }

  /* Resolve from the bucket file */
  else
    {
      if (use_slurp_p)
        current_bucket = obuck_slurp_pool(&bucket_file, &obuck_ctx.hdr, s->oid);
      else
        {
          obuck_ctx.hdr.oid = s->oid;
          obuck_find_by_oid(&bucket_file, &obuck_ctx, 0);
          current_bucket = obuck_fetch(&bucket_file, &obuck_ctx);
        }
      ASSERT(current_bucket);
      mp_flush(resolve_pool);
      uns body;
      resolve_object = obj_read_bucket(buck_buf, resolve_pool, obuck_ctx.hdr.type, obuck_ctx.hdr.length, current_bucket,
				   (want_body_p ? NULL : &body), 1);
      if (!resolve_object)
        die("Error loading bucket %08x", obuck_ctx.hdr.oid);
      byte *U = obj_find_aval(resolve_object, 'U');
      if (U)
        {
          strcpy(url, U);
          return 1;
        }
      die("Bucket %08x has no URL", obuck_ctx.hdr.oid);
    }
}

void
resolve_close(struct fastbuf *f)
{
  bclose(f);
  if (resolve_url_db)
    {
      url_db_close(resolve_url_db);
      resolve_url_db = NULL;
    }
  else if (resolve_text_src)
    {
      sel_text_close(resolve_text_src);
      resolve_text_src = NULL;
    }
  else
    {
      if (use_slurp_p)
        obuck_slurp_end(&bucket_file);
      else if (current_bucket)
        bclose(current_bucket);
      bucket_close();
      buck2obj_free(buck_buf);
      mp_delete(resolve_pool);
      current_bucket = NULL;
      resolve_object = NULL;
    }
}
