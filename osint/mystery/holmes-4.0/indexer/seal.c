/*
 *	Sherlock Indexer -- Seal a Finished Index
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "indexer/indexer.h"
#include "indexer/params.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

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

  struct index_params params;
  params_load(&params);
  params.version = INDEX_VERSION;
  params_save(&params);

  log(L_INFO, "Index sealed");
  return 0;
}
