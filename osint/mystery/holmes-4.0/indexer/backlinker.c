/*
 *	Sherlock Indexer -- Processing of Frames, Backlinks and Image Referers
 *
 *	(c) 2002--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2003--2006 Robert Spalek <robert@ucw.cz>
 *
 *	This module performs six tasks:
 *
 *	Pass 1 (run before oook):
 *
 *	  (1) It locates redirect chains according to redirect edges in the
 *	      link graph and writes the ultimate destination of each redirect
 *	      in the Merges array.
 *
 *	  (2) It converts framesets to redirects to the largest frame if requested.
 *
 *	Pass 2 (run after weights):
 *
 *	  (3) It calculates highest superframe information for all pages
 *	      which belong to a frameset and generates frame backlink labels
 *	      from them, properly following redirects along the way. Frame edges
 *	      and the calculated redirects are used for this task.
 *
 *	  (4) For each page it calculates the largest weight page pointing to
 *	      the one in question.
 *
 *	  (5) The same for image references.
 *
 *	  (6) It generates redirect backlink labels.  It ensures that all
 *	      redirects are marked with CARD_FLAG_EMPTY.
 */

#include "sherlock/sherlock.h"
#include "ucw/url.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/bitarray.h"
#include "sherlock/math.h"
#include "sherlock/object.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"
#include "indexer/graph.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

static struct fastbuf *graph, *frame_graph, *image_graph, *label_graph;
static byte *card_weights;

static uns max_frame_depth = ~0;
static char *frame_path;
static char *image_path;

static struct cf_section back_config = {
  CF_ITEMS {
    CF_UNS("MaxFrameDepth", &max_frame_depth),
    CF_STRING("FrameGraph", &frame_path),
    CF_STRING("ImageGraph", &image_path),
    CF_END
  }
};

/*** Run before oook.  ***/

static void
flatten_merges(void)
{
  for (uns x=0; x<card_count; x++)
    if (merges[x] != (u32)~0U)
      merges[x] = merges_find_root(x);
}

static void
analyse_redirs(void)
{
  uns loops, cnt;
  u32 w, v, d, t;

#ifdef CONFIG_SITES
  u32 *sites;
  READ_ATTR(sites, site_id);
#endif

  loops = cnt = 0;
  while (bget_graph_hdr(graph, &w, &d))
    {
      while (d--)
	{
	  v = bgetl(graph);
	  t = v & ETYPE_IMAGE;
	  v &= ~ETYPE_MASK;
	  ASSERT(v < card_count && (uns)w < card_count);
	  /* Found edge v->w of type t */
	  switch (t)
	    {
	    case ETYPE_REDIRECT:
	      cnt++;
	      if (merges_union(v, w))
		loops++;
	      break;
	    case ETYPE_FRAME:
#ifdef CONFIG_SITES
	      if (sites[v] != sites[w])
		continue;
#endif
	      bputl(frame_graph, v);
	      bputl(frame_graph, w);
	      break;
	    case ETYPE_IMAGE:
	      bputl(image_graph, v);
	      bputl(image_graph, w);
	      break;
	    default: ;
	      /* ignore the edge */
	    }
	}
    }

#ifdef CONFIG_SITES
  FREE_ATTR(sites);
#endif
  flatten_merges();
  log(L_INFO, "Found %d redirects, %d loops broken", cnt, loops);

  /* Mark all redirects with direct or indirect targets */
  uns no_target = 0;
  notes_part_map(1);
  bitarray_t valid_targets = big_alloc_zero(BIT_ARRAY_BYTES(card_count));
  for (uns i=0; i<card_count; i++)
    if ((bring_note(i)->flags & (CARD_NOTE_REDIRECT | CARD_NOTE_HAS_TARGET)) != CARD_NOTE_REDIRECT)
      bit_array_set(valid_targets, i);
  for (uns i=0; i<card_count; i++)
    {
      struct card_note *note = bring_note(i);
      if ((note->flags & (CARD_NOTE_REDIRECT | CARD_NOTE_HAS_TARGET)) == CARD_NOTE_REDIRECT)
        if (merges[i] != (u32)~0U && bit_array_isset(valid_targets, merges[i]))
	  note->flags |= CARD_NOTE_HAS_TARGET;
	else
	  no_target++;
    }
  big_free(valid_targets, BIT_ARRAY_BYTES(card_count));
  notes_part_unmap();
  log(L_INFO, "Found %d redirects with no target", no_target);
}

static byte
log_size(uns size)
{
  int l = logf(size+1)*12. + 1;		// log(1G) ~ 20 ==> maximum
  return l <= 255 ? l : 255;
}

static void
analyse_subframes(void)
{
  byte *sizes = big_alloc(card_count);
  u32 *subf = big_alloc(4*card_count);
  uns chg, passes=1, cnt=0;
  uns x, v, w;

  struct fastbuf *notes = index_bopen(fn_notes, O_RDONLY, 1);
  struct card_note note;
  bitarray_t card_frameset;
  READ_ATTR_BIT(card_frameset, flags, CARD_FLAG_FRAMESET);
  for (x=0; x<card_count; x++)
    {
      breadb(notes, &note, sizeof(note));
      if (bit_array_isset(card_frameset, x))
	sizes[x] = 0;
      else
	sizes[x] = log_size(note.useful_size);
      subf[x] = x;
    }
  FREE_ATTR_BIT(card_frameset);
  bclose(notes);

  do
    {
      chg = 0;
      bsetpos(frame_graph, 0);
      while ((v = bgetl(frame_graph)) != ~0U)
	{
	  w = bgetl(frame_graph);
	  w = merges_follow(w);
	  if (sizes[v] < sizes[w])
	    {
	      sizes[v] = sizes[w];
	      subf[v] = subf[w];
	      chg++;
	    }
	}
      ITRACE("Pass %d: %d changes", passes, chg);
    }
  while (chg && ++passes < max_frame_depth);

  for (x=0; x<card_count; x++)
    if (subf[x] != x)
      {
	cnt++;
	merges_union(x, subf[x]);
      }

  flatten_merges();
  big_free(sizes, card_count);
  big_free(subf, 4*card_count);
  log(L_INFO, "Converted %d framesets to redirects in %d passes", cnt, passes);
}

/*** Run after weights.  ***/

static void
analyse_superframes(void)
{
  u32 *superframe;
  uns i, chg, cnt=0, passes=1;
  u32 v, w, vv;

  superframe = big_alloc(4*card_count);
  for (i=0; i<card_count; i++)
    superframe[i] = ~0U;

  do
    {
      chg = 0;
      bsetpos(frame_graph, 0);
      while ((v = bgetl(frame_graph)) != ~0U)
	{
	  w = bgetl(frame_graph);
	  w = merges_follow(w);
	  vv = (superframe[v] == ~0U) ? v : superframe[v];
	  if (superframe[w] == ~0U ||
	      card_weights[superframe[w]] < card_weights[vv])
	    {
	      superframe[w] = vv;
	      chg++;
	    }
	}
      ITRACE("Pass %d: %d changes", passes, chg);
    }
  while (chg && ++passes < max_frame_depth);

  for (i=0; i<card_count; i++)
    if (superframe[i] != ~0U)
      {
	bputl(label_graph, i | ETYPE_FRAME);
	bputl(label_graph, superframe[i]);
	cnt++;
      }

  big_free(superframe, 4*card_count);
  log(L_INFO, "Calculated superframes for %d cards in %d passes", cnt, passes);
}

static void
analyse_links(void)
{
  u32 *best;
  u32 v, w, d;
  uns cnt=0, lcnt=0;

  best = big_alloc(4*card_count);
  for (v=0; v<card_count; v++)
    best[v] = ~0U;

  while (bget_graph_hdr(graph, &w, &d))
    {
      while (d--)
	{
	  v = bgetl(graph);
	  if ((v & ETYPE_IMAGE) != ETYPE_NORMAL)
	    continue;
	  v &= ~ETYPE_MASK;
	  v = merges_follow(v);
	  w = merges_follow(w);
	  if (v == w)
	    continue;
	  cnt++;
	  if (best[w] == ~0U || card_weights[v] > card_weights[best[w]])
	    best[w] = v;
	}
    }

  for (v=0; v<card_count; v++)
    if (best[v] != ~0U)
      {
	bputl(label_graph, v | ETYPE_NORMAL);
	bputl(label_graph, best[v]);
	lcnt++;
      }

  big_free(best, 4*card_count);
  log(L_INFO, "Processed %u refs to %u backlinks", cnt, lcnt);
}

static void
analyse_image_links(void)
{
  uns cnt=0, lcnt=0;
  u32 v, w;
  u32 *best;

  best = big_alloc(4*card_count);
  for (v=0; v<card_count; v++)
    best[v] = ~0U;

  while ((v = bgetl(image_graph)) != ~0U)
    {
      w = bgetl(image_graph);
      v = merges_follow(v);
      w = merges_follow(w);
      if (best[w] == ~0U || card_weights[v] > card_weights[best[w]])
	best[w] = v;
      cnt++;
    }

  for (v=0; v<card_count; v++)
    if (best[v] != ~0U)
      {
	bputl(label_graph, v | ETYPE_IMAGE);
	bputl(label_graph, best[v]);
	lcnt++;
      }

  big_free(best, 4*card_count);
  log(L_INFO, "Processed %u image refs to %u backlinks", cnt, lcnt);
}

#ifdef CUSTOM_PROPAGATE_IMAGE_ATTRS
static void
propagate_image_attrs(void)
{
  uns cnt=0;
  u32 v, w, d, t;

  attrs_map(1);
  while (bget_graph_hdr(graph, &w, &d))
    {
      while (d--)
	{
	  v = bgetl(graph);
	  t = v & ETYPE_IMAGE;
	  v &= ~ETYPE_MASK;
	  ASSERT(v < card_count && w < card_count);
	  if (t == ETYPE_NORMAL || ETYPE_IMAGE)
	    {
	      v = merges_follow(v);
	      w = merges_follow(w);
	      if (FILETYPE_IS_TEXT(CA_GET_FILE_TYPE(&attrs[v])) &&
		  !FILETYPE_IS_TEXT(CA_GET_FILE_TYPE(&attrs[w])))
		{
		  CUSTOM_PROPAGATE_IMAGE_ATTRS(&attrs[v], &attrs[w]);
		  cnt++;
		}
	    }
	}
    }
  attrs_unmap();
  log(L_INFO, "Propagated image attributes over %d links", cnt);
}
#else
static inline void propagate_image_attrs(void) { }
#endif

#ifdef CUSTOM_UNLINKED_IMAGE_ATTRS
static void
set_unlinked_image_attrs(void)
{
  uns cnt = 0;
  attrs_map(1);
  notes_part_map(0);
  for (uns i=0; i<card_count; i++)
    if (!(bring_note(i)->flags & CARD_NOTE_IS_LINKED) &&
	!FILETYPE_IS_TEXT(CA_GET_FILE_TYPE(&attrs[i])))
      {
	CUSTOM_UNLINKED_IMAGE_ATTRS(&attrs[i]);
	cnt++;
      }
  notes_part_unmap();
  attrs_unmap();
  log(L_INFO, "Adjusted image attributes for %d unlinked images", cnt);
}
#else
static inline void set_unlinked_image_attrs(void) { }
#endif

static void
create_backlinks(void)
{
  uns x, y;
  uns bls=0, trees=0;

  for (x=0; x<card_count; x++)
    if (merges[x] != (u32)~0U && merges[x] != (u32)~1U)
      {
	y = merges[x];
	bputl(label_graph, y | ETYPE_REDIRECT);
	bputl(label_graph, x);
	bls++;
	if (merges[y] == (u32)~0U)
	  {
	    merges[y] = ~1U;
	    trees++;
	  }
      }
  for (x=0; x<card_count; x++)
    if (merges[x] == (u32)~1U)
      merges[x] = ~0U;

  log(L_INFO, "Created %d redirect backlinks in %d trees", bls, trees);
}

struct edge {
  u32 v, w;
};

#define SORT_PREFIX(x) edge_##x
#define SORT_KEY_REGULAR struct edge
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

static int
edge_compare(struct edge *e, struct edge *f)
{
  return (e->w > f->w) - (e->w < f->w);
}

#include "ucw/sorter/sorter.h"

static void
write_backlinks(void)
{
  u32 v, w, urli;
  uns type, cnt=0;
  byte buf[MAX_URL_SIZE];
  struct fastbuf *labels, *url_list;

  label_graph = edge_sort(label_graph, NULL);
  labels = index_bopen(fn_labels_by_id, O_WRONLY | O_APPEND, 0);
  put_attr_set_type(BUCKET_TYPE_V33);
  url_list = index_bopen(fn_urls, O_RDONLY, 1);

  urli = 0;
  bgets(url_list, buf, sizeof(buf));
  while ((v = bgetl(label_graph)) != ~0U)
    {
      switch (v & ETYPE_IMAGE)
	{
	case ETYPE_NORMAL:
	  type = 'z'; break;
	case ETYPE_REDIRECT:
	  type = 'y'; break;
	case ETYPE_IMAGE:
	  type = 'i'; break;
	case ETYPE_FRAME:
	  type = 'b'; break;
	default:
	  ASSERT(0);
	}
      v &= ~ETYPE_MASK;
      w = bgetl(label_graph);
      /* The edge means that back_$type[$v] == $w. */
      while (urli < w)
	{
	  if (!bgets(url_list, buf, sizeof(buf)))
	    die("write_backlinks: object %d not found", w);
	  urli++;
	}
      bputl(labels, type == 'y' ? w : v);	/* Redirects are the only one grouped inside the target URL.  */
      bputc(labels, LABEL_TYPE_URL);
      bput_attr_str(labels, type, buf);
      if (type == 'y')
	bput_attr_format(labels, 'W', "y%d", card_weights[w]);
      bput_attr_separator(labels);
      cnt++;
    }

  for (uns i=0; i<card_count; i++)
    if (merges_follow(i) == i)
      {
	bputl(labels, i);
	bputc(labels, LABEL_TYPE_URL);
	bput_attr_format(labels, 'W', "u%d", card_weights[i]);
	bput_attr_separator(labels);
      }

  bclose(url_list);
  bclose(labels);
  log(L_INFO, "Created %d backlinks", cnt);
}

static char *short_opts = CF_SHORT_OPTS "12";
static char *help = "\
Usage: backlinker [<options>]\n\
\n\
Options:\n"
CF_USAGE
"-1\tRun the 1st phase (before oook)\n\
-2\tRun the 2nd phase (after weights)\n\
";

static void NONRET
usage(void)
{
  fputs(help, stderr);
  exit(1);
}

int
main(int argc, char **argv)
{
  uns phases = 0;
  int opt;
  uns i;

  log_init(argv[0]);
  cf_declare_section("Backlinker", &back_config, 0);
  while ((opt = cf_getopt(argc, argv, short_opts, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
    {
      case '1':
        phases |= 1;
	break;
      case '2':
        phases |= 2;
	break;
      default:
        usage();
    }
  if (optind < argc || !phases)
    usage();

  attrs_part_map(1);
  merges_map(1);
  READ_ATTR(card_weights, weight);

  if (phases & 1)
  {
    /* Scan the graph, analyse redirects and extract frame and image edges */
    graph = index_bopen(fn_graph_obj, O_RDONLY, 1);
    frame_graph = index_bopen(frame_path, O_RDWR | O_CREAT | O_TRUNC, 1);
    image_graph = index_bopen(image_path, O_WRONLY | O_CREAT | O_TRUNC, 1);
    log(L_INFO, "Analysing redirects");
    analyse_redirs();
    brewind(frame_graph);
    bclose(image_graph);
    bclose(graph);

    /* Analyse subframes */
    if (frameset_to_redir)
      analyse_subframes();
    bclose(frame_graph);
  }

  if (phases & 2)
  {
    /* Analyse superframes */
    graph = index_bopen(fn_graph_obj, O_RDONLY, 1);
    label_graph = index_bopen_tmp(1);
    frame_graph = index_bopen(frame_path, O_RDONLY, 1);
    analyse_superframes();
    bclose(frame_graph);

    /* Analyse normal links */
    analyse_links();
    bclose(graph);

    /* Analyse image links */
    image_graph = index_bopen(image_path, O_RDONLY, 1);
    analyse_image_links();
    bclose(image_graph);

    /* Create backlink labels */
    create_backlinks();
    brewind(label_graph);
    write_backlinks();
    bclose(label_graph);

    /* Fix attributes */
    log(L_INFO, "Fixing attributes");
    bitarray_t card_empty;
    READ_ATTR_BIT(card_empty, flags, CARD_FLAG_EMPTY);
    for (i=0; i<card_count; i++)
      if (merges[i] != (u32)~0U)
      {
	uns j = merges[i];
	if (!bit_array_isset(card_empty, i))
	  {
	    if (bit_array_isset(card_empty, j))
	      {
		/*
		 *  If we redirect a non-empty page to an empty page (which can happen
		 *  e.g. in case of redirects to redirects to unknown pages), ignore
		 *  the first redirect.
		 */
		merges[i] = ~0U;
	      }
	    else
	      {
		/* Force redirect sources to be ignored */
		bit_array_set(card_empty, i);
	      }
	  }
	if (card_weights[i] > card_weights[j])
	  card_weights[j] = card_weights[i];
      }
    for (uns i=0; i<card_count; i++)
      if (bit_array_isset(card_empty, i))
	bring_attr(i)->flags |= CARD_FLAG_EMPTY;
    FREE_ATTR_BIT(card_empty);
  }

  WRITE_ATTR(card_weights, weight);
  attrs_part_unmap();

#if defined(CUSTOM_PROPAGATE_IMAGE_ATTRS) || defined(CUSTOM_UNLINKED_IMAGE_ATTRS)
  if (phases & 2)
    {
      /* Propagate image attributes */
      graph = index_bopen(fn_graph_obj, O_RDONLY, 0);
      propagate_image_attrs();
      set_unlinked_image_attrs();
      bclose(graph);
    }
#endif

  merges_unmap();
  return 0;
}
