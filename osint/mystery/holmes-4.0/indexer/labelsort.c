/*
 *	Sherlock Indexer -- Processing of Labels
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2003--2004 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "sherlock/object.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"

#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>

static byte *card_weights, *card_flags;

#define SORT_PREFIX(x) lab_##x
#define SORT_KEY_REGULAR struct lab
#define SORT_DATA_SIZE(k) ((k).count)
#define SORT_DELETE_INPUT 1
#define SORT_INPUT_FB
#define SORT_OUTPUT_FILE

static inline uns
lab_weight(struct lab *a, uns of_redir)
{
  uns w;
  if (!of_redir)
  {
    w = (a->url_id == a->merged_id) ? 0x10000000 : 0;
    w += card_weights[a->url_id];
  }
  else
  {
    w = (a->redir_id == a->url_id) ? 0x10000000 : 0;
    w += card_weights[a->redir_id];
  }
  return w;
}

static inline int
lab_compare(struct lab *a, struct lab *b)
{
  int wa, wb;

  /* Sort on primary card */
  COMPARE(a->merged_id, b->merged_id);

  /* Same primary card, sort on weight of the secondary card */
  wa = lab_weight(a, 0);
  wb = lab_weight(b, 0);
  COMPARE(wb, wa);

  /* Tie, sort on secondary card ID */
  COMPARE(a->url_id, b->url_id);

  /* Same secondary card: put URL in the front, then all redirects sorted on
   * their weight (and then on their ID).  */
  wa = lab_weight(a, 1);
  wb = lab_weight(b, 1);
  COMPARE(wb, wa);

  /* Tie, sort on redirect ID */
  COMPARE(a->redir_id, b->redir_id);

  /* Let the nature decide */
  return 0;
}

#include "ucw/sorter/sorter.h"

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  /* Collect information on cards */

  log(L_INFO, "Collecting card attributes");
  attrs_part_map(0);
  READ_ATTR(card_flags, flags);
  READ_ATTR(card_weights, weight);
  attrs_part_unmap();

  /* Generate mapping from stage 1 ID's to those used by stage 2 */
  struct fastbuf *infos = index_bopen(fn_card_info, O_RDONLY, 1);
  struct card_info info;
  u32 *id_map = big_alloc(card_count * 4);
  for (uns i=0; i<card_count; i++)
    id_map[i] = ~0U;
  uns id_count = 0;
  while (breadb(infos, &info, sizeof(info)))
    {
      ASSERT(info.orig_card < card_count);
      id_map[info.orig_card] = id_count++;
    }
  bclose(infos);

  /* Look at original by-ID labels through our merging lens */

  log(L_INFO, "Searching for labels");
  struct fastbuf *in = index_bopen(fn_labels_by_id, O_RDONLY, 0);
  struct fastbuf *out = index_bopen(fn_labels, O_RDWR | O_CREAT | O_TRUNC, 1);
  get_attr_set_type(BUCKET_TYPE_V33);
  merges_map(0);

  uns redir_id;
  uns count = 0;
  while ((int) (redir_id = bgetl(in)) >= 0)
    {
      uns flags = bgetc(in);
      uns url_id = (card_flags[redir_id] & CARD_FLAG_EMPTY) ? merges[redir_id] : redir_id;
      uns merged_id = (url_id != (u32)~0U) ? merges[url_id] : url_id;
      uns stage2_id = (merged_id != (u32)~0U) ? id_map[merged_id] : merged_id;
      if (merged_id != (u32)~0U && merged_id != url_id)
	{
	  ASSERT(card_flags[url_id] & CARD_FLAG_DUP);
	  ASSERT(card_flags[merged_id] & CARD_FLAG_MERGED);
	  ASSERT(merges[merged_id] == merged_id);
	}

      ucw_off_t start = btell(in);
      struct parsed_attr attr;
      while (bget_attr(in, &attr) > 0)
	;

      struct lab l = {
	.merged_id = stage2_id,
	.url_id = url_id,
	.redir_id = redir_id,
	.count = btell(in) - start - 1,
	.flags = flags
      };
      if (url_id == (u32)~0U || (card_flags[url_id] & CARD_FLAG_EMPTY))
	{
	  /* Empty cards should have been already skipped */
	  ASSERT(stage2_id == ~0U);
	  continue;
	}
      else if (stage2_id == (u32)~0U)
	{
	  /* Cards which don't pass to stage 2 don't need labels */
	  continue;
	}
      else if (flags & LABEL_FLAG_MERGED_ONLY)
	{
	  /* Otherwise, the label is inherited from the original object in fetch.c, saving space in the label file */
	  if (!(card_flags[redir_id] & (CARD_FLAG_DUP | CARD_FLAG_MERGED | CARD_FLAG_EMPTY)))
	    continue;
	}
      else if (!(flags & LABEL_TYPE_URL))
	{
	  /* Non-URL labels should be attached only to the primary card */
	  if (merged_id != redir_id)
	    continue;
	}
      bwrite(out, &l, sizeof(l));
      bsetpos(in, start);
      bbcopy(in, out, l.count);
      bgetc(in);
      count++;
    }
  bclose(in);
  merges_unmap();
  big_free(id_map, card_count * 4);

  log(L_INFO, "Extracted %u labels", count);

  /* Sort the resulting labels */
  brewind(out);
  lab_sort(out, index_name(fn_labels));

  FREE_ATTR(card_weights);
  FREE_ATTR(card_flags);
  return 0;
}
