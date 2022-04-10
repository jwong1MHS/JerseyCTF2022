/*
 *	Sherlock Indexer -- Merging Documents According to Checksums
 *
 *	(c) 2001--2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/bitarray.h"
#include "ucw/unaligned.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"

#include <stdio.h>
#include <stdlib.h>

#define SORT_PREFIX(x) csum_##x
#define SORT_KEY_REGULAR struct csum
#define SORT_HASH_BITS 32
#define SORT_INPUT_FILE
#define SORT_OUTPUT_FILE
static inline int
csum_compare(struct csum *a, struct csum *b)
{
  return memcmp(a, b, sizeof(struct csum));
}

static inline uns
csum_hash(struct csum *a)
{
  return get_u32_be((void *)a);
}
#include "ucw/sorter/sorter.h"

int
main(int argc, char **argv)
{
  struct fastbuf *in;
  uns sum_cnt=0, dup_cnt=0, ign_cnt=0;
  struct csum old, this;

  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  if (!index_name_defined(fn_checksums))
    return 0;

  log(L_INFO, "Merging documents according to checksums");

  csum_sort(index_name(fn_checksums), index_name(fn_checksums));

  bitarray_t card_ignored;

  attrs_part_map(0);
  READ_ATTR_BIT(card_ignored, flags, CARD_FLAG_EMPTY | CARD_FLAG_FRAMESET);
  attrs_part_unmap();

  merges_map(1);
  in = index_bopen(fn_checksums, O_RDONLY, 1);
  bzero(&old, sizeof(old));
  old.cardid = ~0U;
  while (breadb(in, &this, sizeof(this)))
    {
      ASSERT(this.cardid < card_count);
      ASSERT(this.cardid != old.cardid);
      sum_cnt++;
      if (bit_array_isset(card_ignored, this.cardid))
	ign_cnt++;
      else
	{
	  if (!memcmp(&old.md5, &this.md5, 16) && old.cardid != (u32)~0U)
	    {
	      merges_union(old.cardid, this.cardid);
	      dup_cnt++;
	    }
	  old = this;
	}
    }
  bclose(in);

  merges_unmap();
  log(L_INFO, "Processed %d checksums (%d ignored), found %d duplicates", sum_cnt, ign_cnt, dup_cnt);
  return 0;
}
