/*
 *	Sherlock Indexer -- Sorting of Fingerprints
 *
 *	(c) 2001--2004 Martin Mares <mj@ucw.cz>
 *	(c) 2008 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/unaligned.h"
#include "indexer/indexer.h"

#include <stdio.h>
#include <stdlib.h>

#define SORT_PREFIX(x) card_print_##x
#define SORT_KEY_REGULAR struct card_print
#define SORT_HASH_BITS 32
#define SORT_INPUT_FILE
#define SORT_OUTPUT_FILE

static inline int
card_print_compare(struct card_print *a, struct card_print *b)
{
  int e = memcmp(&a->fp, &b->fp, sizeof(a->fp));
  if (e)
    return e;
  COMPARE(a->cardid, b->cardid);
  return 0;
}

static inline uns
card_print_hash(struct card_print *a)
{
  return get_u32_be((void *)&a->fp);
}

#include "ucw/sorter/sorter.h"

static struct fastbuf *fps, *splits;
static uns fp_per_block;
static uns max_block, num_blocks;

static void
build_node(uns prefix, uns start, uns end, uns est_uniq)
{
  if (est_uniq <= fp_per_block || prefix == 32)
    {
      max_block = MAX(max_block, end - start);
      bputl(splits, end - start);
      num_blocks++;
      return;
    }
  struct card_print cp;
  struct fingerprint last_fp;
  bsetpos(fps, (ucw_off_t)start * sizeof(cp));
  uns bits = sorter_min_radix_bits;
  while (bits < sorter_max_radix_bits && (est_uniq >> bits) > fp_per_block / 2)
    bits++;
  bits = MIN(32 - prefix, bits);
  bputl(splits, bits | RESOLVE_SPLIT);
  uns shift = 32 - prefix - bits;
  uns mask = (1 << bits) - 1;
  uns block = 0, hash;
  uns split[mask + 2], ests[mask + 1], est = 0;
  split[0] = start;
  for (uns pos = start; 1; pos++)
  {
    if (pos < end)
      {
        breadb(fps, &cp, sizeof(cp));
        hash = (fp_hash(&cp.fp) >> shift) & mask;
      }
    else
      hash = ~0U;
    while (hash > block)
    {
      ests[block] = est;
      split[++block] = pos;
      est = 0;
      if (block > mask)
      {
	for (uns i = 0; i <= mask; i++)
	  build_node(prefix + bits, split[i], split[i + 1], ests[i]);
	return;
      }
    }
    if (memcmp(&last_fp, &cp.fp, sizeof(cp.fp)) || pos == start)
      est++;
    memcpy(&last_fp, &cp.fp, sizeof(cp.fp));
  }
}

static void
build_splits(void)
{
  fp_per_block = sorter_bufsize / sizeof(struct card_print) * resolve_max_hash_density;
  fp_per_block = MAX(16, fp_per_block);
  fps = index_bopen(fn_fingerprints, O_RDONLY, 1);
  splits = index_bopen(fn_fp_splits, O_WRONLY | O_CREAT | O_TRUNC, 1);
  uns size = bfilesize(fps) / sizeof(struct card_print);
  build_node(0, 0, size, size);
  log(L_INFO, "Built splitting tree with %u leaves (max=%u)", num_blocks, max_block);
  bclose(fps);
  bclose(splits);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  log(L_INFO, "Sorting fingerprints");
  card_print_sort(index_name(fn_fingerprints), index_name(fn_fingerprints));
  build_splits();

  return 0;
}
