/*
 *	Sherlock Indexer -- Sorting of Card Fingerprints
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/unaligned.h"
#include "indexer/indexer.h"

#include <stdio.h>
#include <stdlib.h>

/* This is almost the same as fpsort.c */

#define SORT_PREFIX(x) cp_##x
#define SORT_KEY_REGULAR struct card_print
#define SORT_HASH_BITS 32
#define SORT_INPUT_FILE
#define SORT_OUTPUT_FILE

static inline int
cp_compare(struct card_print *a, struct card_print *b)
{
  int e = memcmp(&a->fp, &b->fp, sizeof(a->fp));
  if (e)
    return e;
  COMPARE(a->cardid, b->cardid);
  return 0;
}

static inline uns
cp_hash(struct card_print *a)
{
  return get_u32_be((void *)&a->fp);
}

#include "ucw/sorter/sorter.h"

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

  log(L_INFO, "Sorting card prints");
  cp_sort(index_name(fn_card_prints), index_name(fn_card_prints));

  return 0;
}
