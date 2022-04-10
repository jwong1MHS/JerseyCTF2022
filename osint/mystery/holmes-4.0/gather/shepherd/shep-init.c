/*
 *	Sherlock Shepherd Daemon -- Create Empty State
 *
 *	(c) 2003--2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "sherlock/bucket.h"
#include "ucw/mempool.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-init [<config-options>] [<state>]\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind < argc-1)
    usage();

  byte *state;
  if (optind == argc-1)
    state = argv[optind];
  else
    state = create_new_state();
  log(L_INFO, "Initializing state %s", state);

  state_params_new(state);

  bucket_open(1);
  bucket_close();

  struct fastbuf *x = temp_state_file();
  put_state_file(state, "index", x, 0);

  site_hash_init(NULL);
  site_hash_save(state);

#ifdef CONFIG_AREAS
  x = temp_state_file();
  struct area_info a;
  bzero(&a, sizeof(a));
  a.soft_limit = 1000000;
  a.hard_limit = 2000000;
  a.plan_limit = 1000000;
  bwrite(x, &a, sizeof(a));
  put_state_file(state, "areas", x, 0);
#endif

  x = temp_state_file();
  bputs(x, "closed\n");
  put_state_file(state, "control", x, 0);

  if (symlink(strrchr(state, '/')+1, mp_strcat(cf_pool, state_dir, "/current")) < 0)
    log(L_ERROR, "Cannot link the state as current: %m");
  else
    log(L_INFO, "State marked as current");

  return 0;
}
