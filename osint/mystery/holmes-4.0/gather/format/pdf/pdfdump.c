/*
 *	An utility for dumping of PDF file internals
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "gather/gather.h"
#include "gather/format/pdf/pdf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <setjmp.h>
#include <fcntl.h>

static char *options = CF_SHORT_OPTS "e:io:rs:w:";

static char *help = "\
Usage: pdfdump <options> <file>\n\
\n\
Options:\n"
CF_USAGE
"\
-e <obj>\t\tDump encoding of the given font object\n\
-i\t\t\tDump info object\n\
-o <obj>\t\tDump the given object\n\
-r\t\t\tDump root object (default)\n\
-s <obj>\t\tDump the given stream object\n\
-w <obj>\t\tDump raw contents of the given stream object\n\
";

static void NONRET
usage(void)
{
  fputs(help, stderr);
  exit(1);
}

static void
error_hook(void)
{
  die("Error %04d: %s", gthis->error_code, gthis->error_msg);
}

static void
dump(s32 id)
{
  OBJECT o = get_i_obj(id);
  for(;;)
    {
      printobj(o);
      if (o.type == OT_UTOK && (!strcmp(o.value.s, "endobj") || !strcmp(o.value.s, "stream")))
	break;
      o = get_obj();
    }
}

static void
dump_fontenc(struct fencoding *enc)
{
  if (!enc)
    {
      log(L_DEBUG, "Null encoding");
      return;
    }
  log(L_DEBUG, "%d-byte encoding, len=%d, start=%d", enc->bytes, enc->len, enc->start);
  for (uns i=0; i<enc->len; i++)
    log(L_DEBUG, "%04x %04x", enc->table[i].fcode, enc->table[i].unicode);
}

int
main(int argc, char **argv)
{
  int opt;
  enum {
    CMD_DUMP_ROOT,
    CMD_DUMP_INFO,
    CMD_DUMP_OBJ,
    CMD_DUMP_STREAM,
    CMD_DUMP_STREAM_RAW,
    CMD_DUMP_ENCODING
  } cmd = CMD_DUMP_ROOT;
  int arg = -1;

  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, options, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      case 'e':
	cmd = CMD_DUMP_ENCODING;
	arg = atol(optarg);
	break;
      case 'i':
	cmd = CMD_DUMP_INFO;
	break;
      case 'o':
	cmd = CMD_DUMP_OBJ;
	arg = atol(optarg);
	break;
      case 'r':
	cmd = CMD_DUMP_ROOT;
	break;
      case 's':
	cmd = CMD_DUMP_STREAM;
	arg = atol(optarg);
	break;
      case 'w':
	cmd = CMD_DUMP_STREAM_RAW;
	arg = atol(optarg);
	break;
      default:
	usage();
      }
  if (optind != argc-1)
    usage();

  gatherer_init();
  gthis = gobj_new(NULL);
  gthis->error_hook = error_hook;

  struct fastbuf *in = bopen(argv[optind], O_RDONLY, 65536);
  struct fastbuf *pdf = fbmem_create(65536);
  bbcopy_slow(in, pdf, ~0U);
  bclose(in);

  pdf_in = fbmem_clone_read(pdf);
  page_pool=mp_new(16384);
  global_pool=mp_new(16384);
  sf_pool=mp_new(2048);

  pdf_setup();
  switch (cmd)
    {
    case CMD_DUMP_ROOT:
      if (pdf_rootref)
	{
	  log(L_DEBUG, "Dumping root object %d:", pdf_rootref);
	  dump(pdf_rootref);
	}
      break;
    case CMD_DUMP_INFO:
      if (pdf_inforef)
	{
	  log(L_DEBUG, "Dumping info object %d:", pdf_inforef);
	  dump(pdf_inforef);
	}
      break;
    case CMD_DUMP_OBJ:
      log(L_DEBUG, "Dumping object %d:", arg);
      dump(arg);
      break;
    case CMD_DUMP_STREAM:
    case CMD_DUMP_STREAM_RAW:
      log(L_DEBUG, "Dumping stream object %d:", arg);
      stream_array[0] = arg;
      stream_array[1] = 0;
      set_input_method(pdf_stream_in);
      if (cmd == CMD_DUMP_STREAM)
	{
	  while (in_check_nschar(SP_ANYWCOMM) >= 0)
	    {
	      OBJECT o = get_obj();
	      printobj(o);
	    }
	}
      else
	{
	  struct fastbuf *out = bfdopen(1, 4096);
	  int c;
	  while ((c = bgetc(pdf_stream_in)) >= 0)
	    bputc(out, c);
	  bclose(out);
	}
      break;
    case CMD_DUMP_ENCODING:
      log(L_DEBUG, "Dumping font object %d:", arg);
      struct fencoding *enc = parse_fontenc(arg);
      dump_fontenc(enc);
      break;
    default:
      ASSERT(0);
    }

  return 0;
}
