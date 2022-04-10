/*
 *	Sherlock Gatherer -- Batch Gathering
 *
 *	(c) 2001 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/url.h"
#include "sherlock/bucket.h"
#include "gather/gather.h"

#include <stdio.h>
#include <setjmp.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <sys/wait.h>

static uns use_subprocess;

static struct cf_section gbatch_config = {
  CF_ITEMS {
    CF_UNS("Subprocess", &use_subprocess),
    CF_END
  }
};

static jmp_buf gath_err_jmp;

static void
error_hook(void)
{
  longjmp(gath_err_jmp, 1);
}

static struct url base_url;
static byte basebuf[MAX_URL_SIZE];

static void
set_base_url(void)
{
  if (!getcwd(basebuf, sizeof(basebuf) - 25))
    die("Cannot get cwd: %m");
  strcat(basebuf, "/.");
  base_url.protocol = "file";
  base_url.protoid = URL_PROTO_FILE;
  base_url.user = NULL;
  base_url.pass = NULL;
  base_url.host = "localhost";
  base_url.port = ~0;
  base_url.rest = basebuf;
}

static void
set_url(byte *u)
{
  byte buf[MAX_URL_SIZE];
  int e;

  if (e = url_auto_canonicalize_rel(u, buf, &base_url))
    die("URL error on %s: %s", u, url_error(e));
  gthis->url = gstrdup(buf);
}

static char *help = "\
Usage: gbatch [<options>] [<URL> ...]\n\
\n\
Options:\n"
CF_USAGE
;

static void
do_url(byte *url)
{
  gthis = gobj_new(NULL);
  gthis->error_hook = error_hook;
  set_url(url);
  if (!setjmp(gath_err_jmp))
    {
      gthis->url = gobj_parse_url(&gthis->url_s, gthis->url, "document", 0);
      gather_download();
      gather_parse();
      gather_analyse();
    }
  if (gthis->error_code <= 1)
    {
      struct fastbuf *b = obuck_create(&bucket_file);
      uns out_format = gobj_write(b, BUCKET_TYPE_V33_LIZARD, GWF_DUMP_BODY);
      struct obuck_header buck;
      obuck_create_end(&bucket_file, b, out_format, &buck);
      log(L_INFO, "%s: %04d %s [%x]", gthis->url, gthis->error_code, gthis->error_msg, buck.oid);
    }
  else
    log(L_INFO, "%s: %04d %s", gthis->url, gthis->error_code, gthis->error_msg);
  gobj_free(gthis);
}

static void
do_one(byte *url)
{
  pid_t pid, xpid;
  int stat;

  if (!use_subprocess)
    {
      do_url(url);
      return;
    }

  if ((pid = fork()) < 0)
    die("fork: %m");
  if (!pid)
    {
      do_url(url);
      exit(0);
    }
  xpid = wait(&stat);
  if (xpid < 0)
    die("wait: %m");
  if (xpid != pid)
    die("wait: pid %d instead of %d", xpid, pid);
  byte err[EXIT_STATUS_MSG_SIZE];
  if (format_exit_status(err, stat))
    die("Subprocess %s", err);
}

int
main(int argc, char **argv)
{
  log_init("gbatch");
  cf_declare_section("GBatch", &gbatch_config, 0);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0)
    {
      fputs(help, stderr);
      return 1;
    }

  gatherer_init();
  set_base_url();
  bucket_open(1);
  if (optind == argc)
    {
      byte x[MAX_URL_SIZE];
      struct fastbuf *b =  bfdopen_shared(0, 4096);
      while (bgets(b, x, MAX_URL_SIZE))
	do_one(x);
    }
  else while (optind < argc)
    do_one(argv[optind++]);
  bucket_close();
  return 0;
}
