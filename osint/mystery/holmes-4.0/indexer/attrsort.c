/*
 *	Sherlock Indexer -- Sorting of Attributes for Stage 2
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "indexer/indexer.h"

#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

#define SORT_PREFIX(x) info_##x
#define SORT_KEY_REGULAR struct card_info
#define SORT_INT(k) ((k).attr.card)
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
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

  log(L_INFO, "Preparing attributes and notes for stage 2");

  struct fastbuf *attrs = index_bopen(fn_attributes, O_RDONLY, 0);
  struct fastbuf *notes = index_bopen(fn_notes, O_RDONLY, 0);
  struct fastbuf *infos = index_bopen_tmp(1);
  struct card_info info;
  info.orig_card = 0;
  uns last_oid = 0;
  uns max_oid = 0;
  uns jumps = 0;
  while (breadb(attrs, &info.attr, sizeof(info.attr)) &&
	 breadb(notes, &info.note, sizeof(info.note)))
    {
      if (!(info.attr.flags & CARD_FLAG_EMPTY) &&
	  !(info.attr.flags & CARD_FLAG_DUP))
	{
	  bwrite(infos, &info, sizeof(info));
	  if (info.attr.card < last_oid)
	    jumps++;
	  last_oid = info.attr.card;
	  max_oid = MAX(max_oid, last_oid);
	}
      info.orig_card++;
    }
  log(L_INFO, "Stage 2 will receive %d objects", (uns)(btell(infos) / sizeof(struct card_info)));
  brewind(infos);

  if (jumps)
    {
      ITRACE("Sorting attributes (%d jumps found)", jumps);
      infos = info_sort(infos, NULL, max_oid);
    }

  bfix_tmp_file(infos, index_name(fn_card_info));
  return 0;
}
