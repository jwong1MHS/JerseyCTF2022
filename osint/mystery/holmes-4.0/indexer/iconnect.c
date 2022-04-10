/*
 *	Sherlock Indexer -- Connecting to Remote Shepherd Server
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 *	(c) 2008 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/conf.h"
#include "ucw/stkstring.h"
#include "ucw/simple-lists.h"
#include "sherlock/object.h"
#include "gather/shepherd/protocol.h"
#include "indexer/indexer.h"

#include <stdio.h>
#include <setjmp.h>
#include <alloca.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>

static sigjmp_buf errjmp;

static void
my_shepp_error(uns code, char *err)
{
  log(L_ERROR, "%d: %s", code, err);
  siglongjmp(errjmp, code);
}

static void NONRET
usage(void)
{
  fprintf(stderr, "\
Usage: iconnect <src_1> ... <src_n> - <indexer-command> <indexer-params>...\n\
   or: iconnect --disconnect [<standard-options>]\n\
");
  exit(1);
}

static void
ic_connect_source(char **src, char **norm)
{
  char *s = *norm = *src;
  if (strncmp(s, "remote:", 7))
    return;
  s += 7;

  shepp_error_cb = my_shepp_error;
  volatile uns retry = ic_retry_count;
  struct odes *attrs;
  byte *host = NULL, *port = NULL;
  byte **extras = NULL;
  for(;;)
    {
      int err;
      if ((err = sigsetjmp(errjmp, 1)))
        {
	  if (shepp_fd > 0)
	    {
	      close(shepp_fd);
	      shepp_fd = -1;
	    }
	  if (err == SHEPP_REPLY_NO_SUCH_STATE)
	    die("The state is not available");
	  if (!retry--)
	    die("No input data available (cannot connect to Shepherd server)");
	  log(L_INFO, "Sleeping for %d seconds before trying again", ic_retry_delay);
	  sleep(ic_retry_delay);
	  continue;
	}
      shepp_timeout = ic_connect_timeout;
      extras = shepp_connect(s);
      host = extras[SHEP_CONNECT_HOST];
      port = extras[SHEP_CONNECT_PORT];

      log(L_INFO, "Asking for state lock");
      attrs = shepp_send_mode(extras);
      break;
    }

  uns objects = obj_find_anum(attrs, 'N', 0);
  byte *state = obj_find_aval(attrs, 'S');
  log(L_INFO, "Connected to gatherer %s:%s (state %s, %d entries) on fd %d", host, port, state, objects, shepp_fd);

  *src = mp_printf(cf_pool, "fd:%d:%d", shepp_fd, objects);

  /* Normalize the source: add the port number and, if it has not been
   * specified explicitly, also the name of the current state. */
  *norm = mp_printf(cf_pool,  "remote:%s:%s", host, port);
  if (!extras[0] || extras[0][0] != 'S')
    *norm = mp_printf_append(cf_pool, *norm, ":S%s", state);
  for (uns i=0; extras[i]; i++)
    *norm = mp_printf_append(cf_pool, *norm, ":%s", extras[i]);
}

static void
ic_disconnect_source(char *src)
{
  if (strncmp(src, "fd:", 3))
    return;
  int fd = atol(src + 3);
  shutdown(fd, 2);
  log(L_INFO, "Disconnected from Shepherd server on fd %d", fd);
}

static void
ic_connect(int argc, char **argv)
{
  cf_load(cf_def_file);

  int i, n;
  for (n = 1; n < argc; n++)
    if (!strcmp(argv[n], "-"))
      break;
  if (n-- == argc || !n)
    usage();

  char *src[n], *norm[n];
  for (i = 0; i < n; i++)
    {
      src[i] = argv[i + 1];
      ic_connect_source(&src[i], &norm[i]);
    }

  setenv("SH_ICONNECT_NORM", stk_strjoin(norm, n, ' '), 1);
  setenv("SH_ICONNECT_SRC", stk_strjoin(src, n, ' '), 1);
  execv(argv[n + 2], argv + n + 2);
  die("Cannot execute %s: %m", argv[n + 2]);
}

static void
ic_disconnect(int argc, char **argv)
{
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 || optind < argc)
    usage();
  CLIST_FOR_EACH(struct simp_node *, s, indexer_sources)
    ic_disconnect_source(s->s);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  /*
   * This program doesn't parse the standard options, because it needs to pass everything including
   * the options to the recursively called indexer. However, it reads the default config file.
   */
  if (argc > 1 && !strcmp(argv[1], "--disconnect"))
    ic_disconnect(argc-1, argv+1);
  else if (argc >= 3)
    ic_connect(argc, argv);
  else
    usage();
  return 0;
}
