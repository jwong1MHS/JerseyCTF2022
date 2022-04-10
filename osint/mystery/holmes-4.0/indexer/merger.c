/*
 *	Sherlock Indexer -- Merging of Identical Documents
 *
 *	(c) 2001--2003 Martin Mares <mj@ucw.cz>
 *	(c) 2003 Robert Spalek <robert@ucw.cz>
 *
 *	This module identifies equivalence classes defined by the Merges
 *	array and merges each class to a single primary card (the card
 *	with largest weight in the class).
 *	The primary card is marked with CARD_FLAG_MERGED and its attributes
 *	are combined with attributes of the other cards which receive
 *	CARD_FLAG_DUP. The mapping secondary -> primary is stored to the
 *	Merges array (which is also used as a work-space), cards with
 *	CARD_FLAG_EMPTY set are not touched (their positions in the array
 *	are used to denote redirects).
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/url.h"
#include "ucw/fastbuf.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

static char *fn_class_log;
static uns class_log_threshold, giant_documents, giant_redirects;

static struct cf_section merger_config = {
  CF_ITEMS {
    CF_STRING("ClassLog", &fn_class_log),
    CF_UNS("LogThreshold", &class_log_threshold),
    CF_UNS("GiantDocuments", &giant_documents),
    CF_UNS("GiantRedirects", &giant_redirects),
    CF_END
  }
};

static uns giant_cnt;

static u32 *
merge_cards(struct card_attr *attrs, uns card_count)
{
  /* For each class, find the best card there */
  u32 *best = big_alloc(4*card_count);
  for (uns i=0; i<card_count; i++)
    best[i] = ~0U;
  for (uns i=0; i<card_count; i++)
    if (!(attrs[i].flags & CARD_FLAG_EMPTY))
      {
	uns root = merges_find_root(i);
	uns j = best[root];
	if (j == (u32)~0U
	    || attrs[i].weight > attrs[j].weight
#ifdef CONFIG_SITES
	    || attrs[i].weight == attrs[j].weight && attrs[i].site_id < attrs[j].site_id
#endif
	    )
	  best[root] = i;
      }

  /* Merge each class to its best card */
  for (uns i=0; i<card_count; i++)
    if (!(attrs[i].flags & CARD_FLAG_EMPTY))
      {
	/* We assume path compression has been done by the previous pass */
	uns dest;
	if (merges[i] == (u32)~0U)
	  merges[i] = i;
	dest = best[merges[i]];
	ASSERT(dest != (u32)~0U);
	merges[i] = dest;
      }

  /* Update card attributes */
  for (uns i=0; i<card_count; i++)
    if (merges[i] != i)
      {
	if (!(attrs[i].flags & CARD_FLAG_EMPTY))
	  {
	    attrs[i].flags |= CARD_FLAG_DUP;
	    attrs[merges[i]].flags |= CARD_FLAG_MERGED;
	  }
#ifdef CUSTOM_MERGE
	if (merges[i] != (u32)~0U)
	  CUSTOM_MERGE(&attrs[merges[i]], &attrs[i]);
#endif
      }

  /* Mmap card-notes for saving giant penalties */
  notes_part_map(1);

  if (giant_redirects)
  {
    /* Re-use "best" for class sizes including redirects */
    bzero(best, 4*card_count);
    for (uns i=0; i<card_count; i++)
      if (merges[i] != (u32)~0U)
	best[merges[i]]++;

    /* Apply penalties */
    for (uns i=0; i<card_count; i++)
      if (!(attrs[i].flags & CARD_FLAG_EMPTY) && best[merges[i]] >= giant_redirects)
	bring_note(i)->flags |= CARD_NOTE_GIANT;
  }

  /* Re-use "best" for class sizes */
  bzero(best, 4*card_count);
  for (uns i=0; i<card_count; i++)
    if (!(attrs[i].flags & CARD_FLAG_EMPTY))
      best[merges[i]]++;

  /* Apply penalties */
  if (giant_documents)
    {
      for (uns i=0; i<card_count; i++)
	if (!(attrs[i].flags & CARD_FLAG_EMPTY) && best[merges[i]] >= giant_documents)
	    bring_note(i)->flags |= CARD_NOTE_GIANT;
    }

  /* Take a note on the weights */
  giant_cnt = 0;
  for (uns i=0; i<card_count; i++)
    {
      struct card_note *note = bring_note(i);
      if (note->flags & CARD_NOTE_GIANT)
	giant_cnt++;
      note->weight_merged = attrs[i].weight;
    }
  notes_part_unmap();

  return best;
}

static void
show_stats(struct card_attr *attrs, uns card_count, u32 *sizes)
{
  uns class_cnt = 0, dup_cnt = 0, max_chain = 0;

  for (uns i=0; i<card_count; i++)
    if (!(attrs[i].flags & CARD_FLAG_EMPTY))
      {
	if (attrs[i].flags & CARD_FLAG_DUP)
	  dup_cnt++;
	else if (attrs[i].flags & CARD_FLAG_MERGED)
	  class_cnt++;
	max_chain = MAX(max_chain, sizes[i]);
      }

  log(L_INFO, "Merged %d cards: %d non-trivial classes (max %d), %d duplicates, %d penalized", card_count, class_cnt,
      max_chain, dup_cnt, giant_cnt);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  cf_declare_section("Merger", &merger_config, 0);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }
  log(L_INFO, "Merging cards");

  attrs_map(1);
  merges_map(1);

  /* Select primary cards, rewrite merges[] and calculate class sizes */
  u32 *sizes = merge_cards(attrs, card_count);

  /* Show merging statistics */
  show_stats(attrs, card_count, sizes);

  attrs_unmap();
  merges_unmap();

  return 0;
}
