/*
 *	Sherlock Shepherd Daemon -- Maintain Equivalance Classes
 *
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "sherlock/index.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static void
build_equivs(void)
{
  struct site *site = NULL;

  while (site = site_next(site))
    {
      struct url u;
      byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], keybuf[URL_KEY_BUF_SIZE];
      u.protoid = site->proto;
      u.protocol = url_proto_names[site->proto];
      u.host = site->hostname;
      u.port = site->port;
      u.user = u.pass = NULL;
      u.rest = "/";
      if (url_pack(&u, buf1) || url_enescape(buf1, buf2))
	die("Unable to construct URL for site %s", site->hostname);
      byte *key = url_key(buf2, keybuf);
      struct footprint fp;
      if (url_footprint(&fp, key))
	die("No footprint for URL %s which is an URL key for %s", key, buf2);
      site->norm_fp = fp.site;
    }
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-equiv [<config-options>] <state>\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc-1)
    usage();

  byte *state = argv[optind];

  site_hash_init(NULL);
  site_hash_load(state, 0);

  log(L_INFO, "Generating equivalence classes");
  url_key_init();
  build_equivs();

  site_hash_save(state);
  return 0;
}
