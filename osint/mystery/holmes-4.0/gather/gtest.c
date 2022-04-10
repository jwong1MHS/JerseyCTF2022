/*
 *	Sherlock Gatherer -- Simple Testing Program
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/url.h"
#include "gather/gather.h"

#include <setjmp.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>

static jmp_buf gath_err_jmp;

static void
error_hook(void)
{
  longjmp(gath_err_jmp, 1);
}

static void
set_url(byte *u)
{
  struct url cwurl;
  byte buf[MAX_URL_SIZE], buf1[MAX_URL_SIZE];
  int e;

  if (!getcwd(buf1, sizeof(buf1) - 25))
    die("Cannot get cwd: %m");
  strcat(buf1, "/.");
  cwurl.protocol = "file";
  cwurl.protoid = URL_PROTO_FILE;
  cwurl.user = NULL;
  cwurl.pass = NULL;
  cwurl.host = "localhost";
  cwurl.port = ~0;
  cwurl.rest = buf1;

  if (e = url_auto_canonicalize_rel(u, buf, &cwurl))
    die("URL error on %s: %s", u, url_error(e));
  gthis->url = gstrdup(buf);
}

static char *options = CF_SHORT_OPTS "adfim:qrstw:D:E:L:P:RT:";

static char *help = "\
Usage: gtest <options> <URL>\n\
\n\
Options:\n"
CF_USAGE
"-a\t\t\tAvoid filtering (undefines Gather.Filter)\n\
-d\t\t\tStop after downloading the document\n\
-f\t\t\tTest filters only\n\
-i\t\t\tAvoid initial content-type filtering\n\
-m <mark>\t\tSet the user_mark presented to the filters\n\
-q\t\t\tResolve queue key only\n\
-r\t\t\tRefresh, original version passed on stdin\n\
-s\t\t\tTest charset detection instead of parsing\n\
-t\t\t\tTest content type detection instead of parsing\n\
-tt\t\t\tTest content type detection before downloading only\n\
-w <type>\t\tRaw output of bucket of a given type\n\
-D <file>\t\tRaw output of downloaded data to <file>\n\
-E <enc>\t\tForce content encoding\n\
-L <lang>\t\tForce language\n\
-P <set>\t\tForce charset\n\
-R\t\t\tSwitch to robots.txt mode\n\
-T <type>\t\tForce content type\n\
";

static int mode_dwnonly;
static int mode_charset;
static int mode_type;
static byte *force_ctype;
static byte *force_cenc;
static byte *force_charset;
static byte *force_language;
static byte *dump_downloaded_to;
static int mode_qkey;
static int avoid_trans;
static int mode_filter_only;
static int mode_refresh;
static int kill_filter;
static int force_robots;
static int user_mark, user_mark_set;
static uns changes, changes_set;
static uns bucket_type = BUCKET_TYPE_V30;

static void NONRET
usage(void)
{
  fputs(help, stderr);
  exit(1);
}

int
main(int argc, char **argv)
{
  struct fastbuf *b;
  int opt;

  log_init("gtest");
  while ((opt = cf_getopt(argc, argv, options, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      case 'a':
	kill_filter = 1;
	break;
      case 'd':
	mode_dwnonly = 1;
	break;
      case 'f':
	mode_filter_only = 1;
	break;
      case 'i':
	avoid_trans = 1;
	break;
      case 'm':
	user_mark = atol(optarg);
	user_mark_set = 1;
	break;
      case 'r':
	mode_refresh = 1;
	break;
      case 's':
	mode_charset = 1;
	break;
      case 't':
	mode_type++;
	break;
      case 'D':
	dump_downloaded_to = optarg;
	break;
      case 'E':
	force_cenc = optarg;
	break;
      case 'L':
	force_language = optarg;
	break;
      case 'P':
	force_charset = optarg;
	break;
      case 'R':
	force_robots = 1;
	break;
      case 'T':
	force_ctype = optarg;
	break;
      case 'q':
	mode_qkey = 1;
	break;
      case 'w':
	bucket_type = strtoul(optarg, NULL, 16);
	if (bucket_type < 10)
	  bucket_type += BUCKET_TYPE_PLAIN;
	break;
      default:
	usage();
      }
  if (optind != argc - 1)
    usage();

  if (kill_filter)
    gather_filter_name = NULL;
  gatherer_init();
  gthis = gobj_new(NULL);
  gthis->error_hook = error_hook;
  set_url(argv[optind]);
  if (user_mark_set)
    gthis->filter_user_mark = user_mark;
  if (mode_refresh)
    {
      b = bfdopen_shared(0, 4096);
      gthis->refreshing = obj_new(gthis->pool);
      while (obj_read(b, gthis->refreshing))
	;
      bclose(b);
    }
  if (!setjmp(gath_err_jmp))
    {
      gthis->url = gobj_parse_url(&gthis->url_s, gthis->url, "document", 0);
      gthis->robot_file_p = force_robots;
      if (mode_qkey)
	gather_create_key();
      if (mode_filter_only)
	gather_filter(0);
      else if (mode_type < 2)
	gather_download();
      if (dump_downloaded_to && gthis->contents)
	{
	  struct fastbuf *out = bopen(dump_downloaded_to, O_WRONLY | O_CREAT | O_TRUNC, 4096);
	  struct fastbuf *in = fbmem_clone_read(gthis->contents);
	  bbcopy_slow(in, out, ~0U);
	  bclose(in);
	  bclose(out);
	  log(L_INFO, "Dumped downloaded data to %s", dump_downloaded_to);
	}
      if (force_charset)
	gthis->charset = force_charset;
      if (force_ctype)
	{
	  if (avoid_trans)
	    gthis->content_type = force_ctype;
	  else
	    set_content_type(force_ctype);
	}
      if (force_cenc)
	{
	  if (avoid_trans)
	    gthis->content_encoding = force_cenc;
	  else
	    set_content_encoding(force_cenc);
	}
      if (force_language)
	gthis->language = force_language;
      if (mode_dwnonly || mode_filter_only)
	;
      else if (mode_charset)
	convert_charset(NULL);
      else if (mode_type)
	guess_content();
      else
	{
	  gather_parse();
	  gather_analyse();
	}
      if (mode_refresh)
	{
	  changes = gobj_check_update();
	  changes_set = 1;
	}
    }
  b = bfdopen_shared(1, 4096);
  gobj_write(b, bucket_type, GWF_DUMP_BODY | (dump_downloaded_to ? 0 : GWF_DUMP_SOURCE));
  if (changes_set)
    {
      bputs(b, ".Changes:");
      if (!changes)
	bputs(b, " none");
      byte *diffs[] = { "text-small", "text-large", "refs", "http-meta", "forced", "redirect" };
      for (uns i=0; i<ARRAY_SIZE(diffs); i++)
	if (changes & (1 << i))
	  {
	    bprintf(b, " %s", diffs[i]);
	    changes &= ~(1 << i);
	  }
      if (changes)
	bprintf(b, " ?(%x)", changes);
      bputs(b, "\n");
    }
  bclose(b);
  gobj_free(gthis);

  return 0;
}
