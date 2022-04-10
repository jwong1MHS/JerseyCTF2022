/*
 *	A simple program for testing of parsers
 *
 *	(c) 2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-unicode.h"
#include "ucw/url.h"
#include "sherlock/tagged-text.h"
#include "sherlock/index.h"
#include "gather/gather.h"

static byte *wt_names[16] = { WORD_TYPE_USER_NAMES };
static byte *mt_names[16] = { META_TYPE_USER_NAMES };

static void
dump_stream(struct fastbuf *out, struct fastbuf *in, byte **t_names)
{
  if (!in)
    return;
  in = fbmem_clone_read(in);
  uns chars = 0;
  uns c;
  while ((c = bget_tagged_char(in)) != ~0U)
    {
      chars++;
      if (c < 0x80000000)
	bput_utf8(out, c);
      else if (c < 0x80010000)
	bprintf(out, "<%s%s>", t_names[c & 0x0f], (c & 0x10) ? "!" : "");
      else if (c < 0x80020000)
	bprintf(out, "<ref>");
      else if (c < 0x80030000)
	bprintf(out, "</ref>");
      else
	bprintf(out, "<%08x>", c);
    }
  if (chars)
    bputc(out, '\n');
  bclose(in);
}

static void
error_hook(void)
{
  die("Parsing failed: %s", gthis->error_msg);
}

static void NONRET
usage(void)
{
  die("Usage: parser-test -c<content-type>");
}

int
main(int argc, char **argv)
{
  int opt;
  byte *ctype = "text/plain";

  log_init("parser-test");
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS "c:", CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      case 'c':
	ctype = optarg;
	break;
      default:
	usage();
      }
  if (optind != argc)
    usage();

  gather_filter_name = NULL;
  gatherer_init();
  gthis = gobj_new(NULL);
  gthis->error_hook = error_hook;
  gthis->url = gobj_parse_url(&gthis->url_s, "file://localhost/stdin", "document", 0);
  gthis->charset = "utf-8";
  set_content_type(ctype);

  struct fastbuf *b = bfdopen_shared(0, 4096);
  gthis->contents = fbmem_create(4096);
  bbcopy_slow(b, gthis->contents, ~0U);
  bclose(b);

  gather_parse();

  b = bfdopen_shared(1, 4096);
#if 0
  gobj_write(b, BUCKET_TYPE_V30, GWF_DUMP_BODY);
#endif
  dump_stream(b, gthis->meta, mt_names);
  dump_stream(b, gthis->text, wt_names);
  bclose(b);

  gobj_free(gthis);
  return 0;
}
