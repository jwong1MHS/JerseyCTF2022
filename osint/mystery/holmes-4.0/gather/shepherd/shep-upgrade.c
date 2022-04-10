/*
 *	Sherlock Shepherd Daemon -- Upgrading of Data Files
 *
 *	(c) 2004--2007 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-upgrade [<config-options>] <state>\n");
  exit(1);
}

static int
load_params(byte *state, struct state_params *par)
{
  state_params_init(par);
  byte *name = state_file_name(state, "params");
  int fd = open(name, O_RDONLY);
  if (fd < 0)
    {
      if (errno == ENOENT)
	{
	  log(L_INFO, "No parameter file found, assuming version 0");
	  par->format_version = 0;
	  return 0;
	}
      die("Cannot open %s: %m", name);
    }
  int n = read(fd, par, sizeof(*par));
  if (n < 0)
    die("Error reading %s: %m", name);
  if (n < 8 || par->params_magic != PARAMS_MAGIC)
    die("Invalid parameter file %s", name);
  return n;
}

static void
convert_v0_sites(byte *state)
{
  struct fastbuf *s_in = read_state_file(state, "sites");
  u32 ver = bgetl(s_in);
  if (ver == SITE_LIST_MAGIC)
    {
      log(L_INFO, "Site file is up to date");
      bclose(s_in);
      return;
    }

  struct fastbuf *s_out = temp_state_file();
  log(L_INFO, "Site file came from ice age, converting it");
  bsetpos(s_in, 0);
  bputl(s_out, SITE_LIST_MAGIC);
  u32 x;
  uns cnt = 0;
  while ((x = bgetl(s_in)) != ~0U)
    {
      bputl(s_out, x);
      bbcopy(s_in, s_out, 4+4+4);
      bputl(s_out, 0);			/* num_gathered */
      bbcopy(s_in, s_out, 4+8+3);
      bputc(s_out, 0);			/* avg_download_time */
      bputl(s_out, 0);			/* rfu[0..3] */
      bputl(s_out, 0);
      bputl(s_out, 0);
      bputl(s_out, 0);
      byte host[MAX_URL_SIZE];
      bgets0(s_in, host, sizeof(host));
      bputs0(s_out, host);
      cnt++;
    }
  log(L_INFO, "Converted %d sites", cnt);
  bclose(s_in);
  put_state_file(state, "sites", s_out, 0);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc-1)
    usage();

  byte *state = argv[optind];
  struct state_params par;
  int param_len = load_params(state, &par);

  log(L_INFO, "Upgrading from version %08x", par.format_version);
  switch (par.format_version)
    {
    case 0:				/* Format prior to v3.11 */
      convert_v0_sites(state);
      log(L_INFO, "Created parameter file");
      break;
    case PARAMS_VERSION_CURRENT:	/* v3.11 */
      ASSERT(param_len == sizeof(struct state_params));
      log(L_INFO, "This is the current version, nothing to convert");
      break;
    default:
      die("Unknown version, don't know how to handle");
    }

  byte *ctrl = state_file_name(state, "control");
  if (access(ctrl, R_OK))
    log(L_WARN, "This state has no control file, you need to create one before running shepherd");

  par.format_version = PARAMS_VERSION_CURRENT;
  state_params_write(state, &par);
  log(L_INFO, "Done");
  return 0;
}
