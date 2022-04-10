/*
 *	Sherlock Shepherd Daemon -- Control Connection Tester
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "sherlock/object.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/protocol.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

static void
receive_buckets(u32 command, struct odes *attrs)
{
  struct shepp_packet_hdr rq;
  shepp_send_attrs(&rq, command, NULL, attrs);

  struct fastbuf *in = shepp_fb_open_read(&rq);
  struct shepp_bucket_header sh;
  struct mempool *mp = mp_new(65536);
  struct buck2obj_buf *b2ob = buck2obj_alloc();
  struct fastbuf *out = bfdopen_shared(1, 65536);
  while (breadb(in, &sh, sizeof(sh)))
    {
      mp_flush(mp);
      bprintf(out, "# %08x %08x (%d bytes)\n", sh.oid, sh.type, sh.length);
      struct odes *obj = obj_read_bucket(b2ob, mp, sh.type, sh.length, in, NULL, 1);
      if (obj)
	obj_write(out, obj, BUCKET_TYPE_PLAIN);
      bputc(out, '\n');
    }
  bclose(out);
  buck2obj_free(b2ob);
  mp_delete(mp);
  bclose(in);
}

static void
receive_raw(u32 command, struct odes *attrs)
{
  struct shepp_packet_hdr rq;
  shepp_send_attrs(&rq, command, NULL, attrs);

  struct fastbuf *in = shepp_fb_open_read(&rq);
  struct fastbuf *out = bfdopen_shared(1, 65536);
  bbcopy_slow(in, out, ~0U);
  bclose(out);
  bclose(in);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind >= argc)
    {
      fprintf(stderr, "\
Usage: shep-ctrltest [<config-options>] <host>[:<port>][:<extras>] [<operation> [<extra attributes>]]\n\
\n\
Operations:\n\
buckets\t\tSend all buckets (default)\n\
rawbuck\t\tSend all buckets in raw mode\n\
index\t\tSend raw state index\n\
sites\t\tSend raw site file\n\
");
      exit(1);
    }
  byte *host = argv[optind++];
  byte *op = "buckets";
  if (optind < argc)
    op = argv[optind++];

  byte **extras = shepp_connect(host);
  struct odes *attrs = shepp_send_mode(extras);
  log(L_INFO, "Locked state %s (%d objects)", obj_find_aval(attrs, 'S'), obj_find_anum(attrs, 'N', -1));

  attrs = shepp_new_attrs();
  while (optind < argc)
    {
      byte *a = argv[optind++];
      if (a[0])
	obj_add_attr(attrs, a[0], a+1);
    }

  if (!strcmp(op, "buckets"))
    receive_buckets(SHEPP_REQ_SEND_BUCKETS, attrs);
  else if (!strcmp(op, "rawbuck"))
    receive_buckets(SHEPP_REQ_SEND_RAW_BUCKETS, attrs);
  else if (!strcmp(op, "index"))
    receive_raw(SHEPP_REQ_SEND_RAW_INDEX, attrs);
  else if (!strcmp(op, "sites"))
    receive_raw(SHEPP_REQ_SEND_RAW_SITES, attrs);
  else
    die("Unknown operation %s", op);

  log(L_INFO, "Done");
  return 0;
}
