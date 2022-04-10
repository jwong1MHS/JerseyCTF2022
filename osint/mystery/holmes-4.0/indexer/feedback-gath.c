/*
 *	Feedback for the gatherer
 *
 *	(c) 2003, Robert Spalek <robert@ucw.cz>
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 *	(c) 2008 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/simple-lists.h"
#include "indexer/indexer.h"
#include "gather/shepherd/protocol.h"

#include <stdio.h>
#include <stdlib.h>

static enum { SORT_BY_FOOTPRINT, SORT_BY_WEIGHT } sort_by;

#define SORT_KEY_REGULAR struct feedback_gatherer
#define SORT_PREFIX(x) feedback_by_fp_##x
#define SORT_UNIQUE
#define SORT_HASH_BITS 32
#define SORT_INPUT_FB
#define SORT_OUTPUT_FILE
static inline int
feedback_by_fp_compare(struct feedback_gatherer *a, struct feedback_gatherer *b)
{
  u32 *ax = (void *)a->footprint;
  u32 *bx = (void *)b->footprint;
  COMPARE(ax[0], bx[0]);
  COMPARE(ax[1], bx[1]);
  COMPARE(ax[2], bx[2]);
  COMPARE(ax[3], bx[3]);

  // Old gatherer
  COMPARE(a->cardid, b->cardid);
  return 0;
}

static inline uns
feedback_by_fp_hash(struct feedback_gatherer *a)
{
  return *(u32 *)(void *)a->footprint;
}
#include "ucw/sorter/sorter.h"

#define SORT_KEY_REGULAR struct feedback_gatherer
#define SORT_PREFIX(x) feedback_by_weight_##x
#define SORT_UNIQUE
#define SORT_HASH_BITS 32
#define SORT_INPUT_FB
#define SORT_OUTPUT_FILE
static inline int
feedback_by_weight_compare(struct feedback_gatherer *a, struct feedback_gatherer *b)
{
  // Debug mode
  u32 *ax = (void *)a->footprint;
  u32 *bx = (void *)b->footprint;
  COMPARE(ax[0], bx[0]);
  COMPARE(ax[1], bx[1]);
  COMPARE(b->weight, a->weight);
  COMPARE(a->cardid, b->cardid);
  return 0;
}

static inline uns
feedback_by_weight_hash(struct feedback_gatherer *a)
{
  return *(u32 *)(void *)a->footprint;
}
#include "ucw/sorter/sorter.h"

static void
my_shepp_error(uns code, char *msg)
{
  die("Error %d while uploading: %s", code, msg);
}

static void
upload_feedback_to(char *src)
{
  if (strncmp(src, "fd:", 3))
    return;
  shepp_error_cb = my_shepp_error;
  shepp_fd = atol(src+3);

  log(L_INFO, "Uploading feedback to the gatherer on fd %d", shepp_fd);

  struct fastbuf *f = bopen(index_name(fn_feedback_gath), O_RDONLY, 1048576);
  shepp_timeout = ic_reply_timeout;
  shepp_id_counter = 0x9999;

  struct shepp_packet_hdr rq, rp;
  shepp_send_none(&rq, SHEPP_REQ_SEND_FEEDBACK, NULL);
  shepp_recv(&rp, &rq);
  if (rp.type != SHEPP_REPLY_OK)
    shepp_unex(&rp);

  byte *buf;
  uns len;
  while (len = bdirect_read_prepare(f, &buf))
    {
      shepp_send_raw(&rq, SHEPP_REQ_SEND_DATA_BLOCK, NULL, buf, len);
      bdirect_read_commit(f, buf+len);
      shepp_recv(&rp, &rq);
      if (rp.type != SHEPP_REPLY_OK)
	die("Upload failed, received reply %08x", rp.type);
    }
  bclose(f);

  shepp_send_none(&rq, SHEPP_REQ_SEND_DATA_END, NULL);
  shepp_recv(&rp, &rq);
  if (rp.type != SHEPP_REPLY_OK)
    die("Upload failed, received reply %08x", rp.type);
}

static void
upload_feedback(void)
{
  CLIST_FOR_EACH(struct simp_node *, s, indexer_sources)
    upload_feedback_to(s->s);
  log(L_INFO, "Upload completed");
}

static char *short_opts = CF_SHORT_OPTS "w";
static char *help = "\
Usage: feedback-gath [<options>]\n\
\n\
Options:\n"
CF_USAGE
"-w\t\tSort URL's inside one host descending by dynamic weight (debug)\n\
";

static void NONRET
usage(void)
{
  fputs(help, stderr);
  exit(1);
}

static inline uns
get_weight(struct card_note *n)
{
#ifdef CONFIG_WEIGHTS
  return n->weight_dynamic;
#else
  return n->weight_scanner;
#endif
}

int
main(int argc, char **argv)
{
  int opt;
  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, short_opts, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
    {
      case 'w':
	sort_by = SORT_BY_WEIGHT;
	break;
      default:
	usage();
    }
  if (optind < argc)
    usage();

  if (!fn_feedback_gath)
    {
      log(L_INFO, "Gatherer feedback not requested");
      return 0;
    }

  struct fastbuf *fb = index_bopen_tmp(1);
  struct fastbuf *note;
  struct card_note n;
  struct feedback_gatherer f;
  uns idx;

  log(L_INFO, "Reading %s", fn_notes);
  note = index_bopen(fn_notes, O_RDONLY, 1);
  idx = 0;
  while (bread(note, &n, sizeof(n)))
  {
    memcpy(f.footprint, n.footprint, sizeof(n.footprint));
    f.cardid = idx++;
    f.flags = n.flags;
    f.weight = get_weight(&n);	//FIXME: penalized from chewer?
    bwrite(fb, &f, sizeof(f));
  }
  bclose(note);

  log(L_INFO, "Reading %s", fn_notes_skel);
  note = index_bopen(fn_notes_skel, O_RDONLY, 1);
  idx = FIRST_ID_SKEL;
  while (bread(note, &n, sizeof(n)))
  {
    memcpy(f.footprint, n.footprint, sizeof(n.footprint));
    f.cardid = idx++;
    f.flags = n.flags;
    f.weight = get_weight(&n);
    bwrite(fb, &f, sizeof(f));
  }
  bclose(note);

  brewind(fb);
  log(L_INFO, "Sorting footprints");
  if (sort_by == SORT_BY_FOOTPRINT)
    feedback_by_fp_sort(fb, index_name(fn_feedback_gath));
  else if (sort_by == SORT_BY_WEIGHT)
    feedback_by_weight_sort(fb, index_name(fn_feedback_gath));
  else
    ASSERT(0);

  if (ic_send_feedback)
    upload_feedback();

  return 0;
}
