/*
 *	Sherlock Shepherd Daemon -- Export Buckets to the Indexer
 *
 *	(c) 2003--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/export.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define SORT_PREFIX(x) export_##x
#define SORT_KEY_REGULAR struct export_entry
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_UNIQUE
#define SORT_INT(x) (x).oid
#include "ucw/sorter/sorter.h"

#define SORT_PREFIX(x) weight_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
static inline uns
export_weight(struct url_state *x)
{
  uns w = x->weight;
  if (USF_IS_SACRED(x->flags))
    w += 1000000;
  return w;
}

static inline int
weight_compare(struct url_state *x, struct url_state *y)
{
  uns xw = export_weight(x);
  uns yw = export_weight(y);
  REV_COMPARE(xw, yw);
  return 0;
}
#include "gather/shepherd/index-sort.h"

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-export [<config-options>] [-w<min-weight>] [-B<best-thick>] [-b<best-thin>] <state> <export-index-file>\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  int opt;
  uns min_weight = 0;
  uns best_thick = ~0U;
  uns best_thin = ~0U;

  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS "b:w:B:", CF_NO_LONG_OPTS, NULL)) > 0)
    switch (opt)
      {
      case 'w':
	min_weight = atol(optarg);
	break;
      case 'B':
	best_thick = atol(optarg);
	break;
      case 'b':
	best_thin = atol(optarg);
	break;
      default:
	usage();
      }
  if (optind != argc-2)
    usage();

  byte *state = argv[optind];
  byte *outname = argv[optind+1];

  struct fastbuf *in = read_state_file(state, "index");
  if (best_thick != ~0U || best_thin != ~0U)
    {
      log(L_INFO, "Sorting index by weight");
      in = weight_sort(in, NULL);
    }

  log(L_INFO, "Generating export index");
  struct fastbuf *out = temp_state_file();
  struct url_state s;
  struct export_entry x;
  uns thick_cnt = 0, thin_cnt = 0;
  uns type;
  while (bread(in, &s, sizeof(s)))
    if ((type = ustate_type(&s)) != UTYPE_SKEY && type != UTYPE_ZOMBIE && s.oid != OID_UNDEFINED && !(s.flags & USF_CONTRIB) &&
	s.weight >= min_weight)
      {
	int strip = 0;
	if (type == UTYPE_NEW || type == UTYPE_SLEEPING)
	  {
	    if (thin_cnt < best_thin)
	      thin_cnt++;
	    else
	      continue;
	  }
	else
	  {
	    if (thick_cnt < best_thick)
	      thick_cnt++;
	    else if (thin_cnt < best_thin)
	      {
		thin_cnt++;
		strip = 1;
	      }
	    else
	      continue;
	  }
	x.oid = s.oid;
	x.last_checked_time = (s.last_seen & ~1U) | strip;
	x.weight = s.weight;
	bwrite(out, &x, sizeof(x));
      }
  bclose(in);
  brewind(out);

  log(L_INFO, "Sorting export index");
  sorter_bufsize /= 4;		/* FIXME: This is a hack, replace by a proper buffer sizing strategy. */
  out = export_sort(out, NULL, (u32)~0U);
  bfix_tmp_file(out, outname);

  log(L_INFO, "Exported %d thick and %d thin entries", thick_cnt, thin_cnt);
  return 0;
}
