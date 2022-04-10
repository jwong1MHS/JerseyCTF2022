/*
 *	Sherlock Indexer -- Merging Documents According to Fingerprints
 *
 *	(c) 2003--2004 Martin Mares <mj@ucw.cz>
 *	(c) 2003 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/bitarray.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

int
main(int argc, char **argv)
{
  struct fastbuf *in;
  uns sum_cnt=0, dup_cnt=0, ign_cnt=0, not_downloaded=0;
  struct card_print old, this;

  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }
  log(L_INFO, "Merging documents according to fingerprints");

  attrs_part_map(0);
  bitarray_t card_ignored;
  READ_ATTR_BIT(card_ignored, flags, CARD_FLAG_EMPTY | CARD_FLAG_FRAMESET);
  attrs_part_unmap();

  merges_map(1);

  in = index_bopen(fn_fingerprints, O_RDONLY, 1);
  bzero(&old, sizeof(old));
  old.cardid = ~0U;
  while (breadb(in, &this, sizeof(this)))
    {
      sum_cnt++;
      if (this.cardid >= FIRST_ID_SKEL)
      {
	not_downloaded++;
	continue;
      }
      ASSERT(this.cardid < card_count);
      ASSERT(this.cardid != old.cardid);
      if (bit_array_isset(card_ignored, this.cardid))
	ign_cnt++;
      else
	{
	  if (!memcmp(&old.fp, &this.fp, sizeof(struct fingerprint)) && old.cardid != (u32)~0U)
	    {
	      merges_union(old.cardid, this.cardid);
	      dup_cnt++;
	    }
	  old = this;
	}
    }
  bclose(in);

  merges_unmap();
  log(L_INFO, "Processed %d fingerprints (%d not downloaded, %d ignored), found %d duplicates", sum_cnt, not_downloaded, ign_cnt, dup_cnt);

  return 0;
}
