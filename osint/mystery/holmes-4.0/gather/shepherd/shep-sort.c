/*
 *	Sherlock Shepherd Daemon -- Sorting of the Index
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <stdlib.h>

#define SORT_PREFIX(x) index_##x
#define SORT_INPUT_FILE
#define SORT_OUTPUT_FB
#define SORT_UNIQUE
#define SORT_BY_FP
#include "gather/shepherd/index-sort.h"

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-sort [<config-options>] <state>\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc-1)
    usage();

  byte *state = argv[optind];

  if (state_flags_get(state) & STATE_FLAG_SORTED)
    log(L_INFO, "Index already sorted");
  else
    {
      log(L_INFO, "Sorting the index");
      struct fastbuf *sorted = index_sort(state_file_name(state, "index"), NULL);
      put_state_file(state, "index", sorted, 0);
      state_flags_set(state, STATE_FLAG_SORTED);
    }

  return 0;
}
