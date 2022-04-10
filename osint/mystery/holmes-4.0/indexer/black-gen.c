/*
 *	Sherlock Indexer -- Blacklist Generator
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "indexer/indexer.h"

#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

struct ble { u32 id; };
#define SORT_PREFIX(x) blk_##x
#define SORT_KEY_REGULAR struct ble
#define SORT_INT(k) ((k).id)
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#include "ucw/sorter/sorter.h"

static void NONRET
usage(void)
{
  fputs("\
Usage: black-gen <base> <sub1> ... <subN>\n\
\n\
<base>\t\tBase index for which the blacklist will be generated\n\
<subX>\t\tSub-indices with entries to be blacklisted\n\
", stderr);
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  int opt;
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      default:
	usage();
      }
  if (optind+1 >= argc)
    usage();

  int N = argc-optind;
  struct fastbuf *in[N];
  struct card_print cp[N];
  for (int i=0; i<N; i++)
    {
      fn_directory = argv[optind+i];
      in[i] = index_bopen("card-prints", O_RDONLY, 1);
      bzero(&cp[i], sizeof(cp[i]));
    }
  struct fastbuf *tmp = index_bopen_tmp(1);
  uns max = 0;
  while (breadb(in[0], &cp[0], sizeof(cp[0])))
    {
      int match = 0;
      for (int i=1; i<N; i++)
	{
	  int c;
	  while ((c = memcmp(&cp[i].fp, &cp[0].fp, sizeof(struct fingerprint))) <= 0)
	    {
	      if (!c)
		match = 1;
	      if (!breadb(in[i], &cp[i], sizeof(cp[i])))
		memset(&cp[i], 0xff, sizeof(cp[i]));
	    }
	}
      if (match)
        {
	  bputl(tmp, cp[0].cardid);
	  max = MAX(max, cp[0].cardid);
	}
    }
  for (int i=0; i<N; i++)
    bclose(in[i]);

  fn_directory = argv[optind];
  brewind(tmp);
  tmp = blk_sort(tmp, NULL, max);
  struct fastbuf *out = index_bopen(fn_blacklist, O_WRONLY | O_CREAT | O_TRUNC, 1);
  uns x, last = ~0U;
  while ((x = bgetl(tmp)) != ~0U)
    {
      if (x != last)
	bputl(out, x);
      last = x;
    }
  log(L_INFO, "Generated blacklist with %d entries", (int)(btell(out)/4));
  bclose(out);
  return 0;
}
