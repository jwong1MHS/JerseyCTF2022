/*
 *	Sherlock Indexer -- Sorting of String Index
 *
 *	(c) 2001 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/unicode.h"
#include "ucw/ff-unicode.h"
#include "ucw/ff-binary.h"
#include "ucw/heap.h"
#include "indexer/indexer.h"
#include "indexer/params.h"

#include <stdio.h>
#include <stdlib.h>

struct ssk {
  u32 size;
  struct fingerprint fp;
};

#define SORT_PREFIX(x) ss_##x
#define SORT_KEY struct ssk
#define SORT_DATA_SIZE(k) ((k).size)
#define SORT_UNIFY
#define SORT_UNIFY_WORKSPACE(k) REFCHAIN_UNIFY_WORKSPACE
#define SORT_HASH_BITS 32
#define SORT_DELETE_INPUT sort_delete_src
#define SORT_INPUT_FILE
#define SORT_OUTPUT_FB

static inline int
ss_compare(struct ssk *x, struct ssk *y)
{
  return memcmp(&x->fp, &y->fp, sizeof(struct fingerprint));
}

static inline uns
ss_hash(struct ssk *x)
{
  return get_u32_be((void *)&x->fp);
}

static inline int
ss_read_key(struct fastbuf *f, struct ssk *x)
{
  if (!breadb(f, &x->fp, sizeof(x->fp)))
    return 0;
  x->size = bgetl(f);
  return 1;
}

static inline void
ss_write_key(struct fastbuf *f, struct ssk *x)
{
  bwrite(f, &x->fp, sizeof(x->fp));
  bputl(f, x->size);
}

#include "indexer/refmerger.h"
#include "indexer/refslicer.h"

static void
ss_write_merged(struct fastbuf *dest, struct ssk **keys, void **data, uns n, void *buf)
{
  bwrite(dest, &keys[0]->fp, sizeof(struct fingerprint));
  refchain_write_merged(n, (void *)keys, data, dest, buf);
}

static void
ss_copy_merged(struct ssk **keys, struct fastbuf **data, uns n, struct fastbuf *dest)
{
  bwrite(dest, &keys[0]->fp, sizeof(struct fingerprint));
  refchain_copy_merged(n, (void *)keys, data, dest);
}

#include "ucw/sorter/sorter.h"

static uns
split(struct fastbuf *sorted)
{
  struct fastbuf *smap, *refs;
  struct fingerprint key;
  uns entries = 0;

  smap = index_bopen(fn_string_map, O_WRONLY | O_CREAT | O_TRUNC, 1);
  refs = index_bopen(fn_references, O_WRONLY | O_CREAT | O_APPEND, 0);
  slice_init();

  bseek(refs, 0, SEEK_END);
  while (breadb(sorted, &key, sizeof(key)))
    {
      bwrite(smap, &key, sizeof(key));
      bputo(smap, btell(refs));
      slice_chain(sorted, refs, ~0U, 0);
      entries++;
      if (unlikely(!entries))
	die("Too many strings indexed. Try decreasing Chewer.StringMax as a work-around.");
    }
  memset(&key, 255, sizeof(key));
  bwrite(smap, &key, sizeof(key));
  bputo(smap, btell(refs));
  if ((u64)btell(refs) >= ((u64)1 << 8 * BYTES_PER_O))
    die("Too large references. Check CONFIG_LARGE_DB configuration switch.");

  slice_cleanup();
  bclose(smap);
  bclose(refs);
  log(L_INFO, "Indexed %d strings", entries);
  return entries;
}

static void
mk_hash(uns cnt)
{
  uns shift = 1;
  struct fastbuf *smap, *shash;
  uns bsize, bsmax;
  int buck, nbuck;
  struct fingerprint fp;
  ucw_off_t pos;
  u32 hh;

  while ((cnt >> shift) > string_avg_bucket)
    shift++;
  smap = index_bopen(fn_string_map, O_RDONLY, 1);
  shash = index_bopen(fn_string_hash, O_WRONLY | O_CREAT | O_TRUNC, 1);

  nbuck = 1 << shift;
  buck = -1;
  bsize = 0;
  bsmax = 0;
  for(;;)
    {
      pos = btell(smap) / (sizeof(struct fingerprint) + BYTES_PER_O);
      if (!breadb(smap, &fp, sizeof(fp)))
	break;
      bskip(smap, BYTES_PER_O);
      hh = fp_hash(&fp) >> (32 - shift);
      while (buck < (int) hh)
	{
	  if (bsize > bsmax)
	    bsmax = bsize;
	  bsize = 0;
	  bputl(shash, pos);
	  buck++;
	}
      bsize++;
    }
  if (bsize > bsmax)
    bsmax = bsize;
  while (buck < nbuck)			/* one more as "last" marker due to buck starting at -1 */
    {
      bputl(shash, pos);
      buck++;
    }

  bclose(smap);
  bclose(shash);
  log(L_INFO, "Hashed string references to %d buckets, %d entries/bucket max", nbuck, bsmax);
}

int
main(int argc, char **argv)
{
  struct fastbuf *sorted;
  uns cnt;

  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  log(L_INFO, "Sorting string index");
  sorted = ss_sort(index_name(fn_string_index), NULL);
  log(L_INFO, "Splitting string index");
  cnt = split(sorted);
  bclose(sorted);
  mk_hash(cnt);
  return 0;
}
