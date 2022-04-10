/*
 *	Sherlock Indexer -- Graph Builder
 *
 *	(c) 2001 Martin Mares <mj@ucw.cz>
 *	(c) 2003--2007 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"
#include "ucw/bitarray.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/stkstring.h"
#include "indexer/indexer.h"
#include "indexer/graph.h"

#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

#define	TRACE(x...)	if (0) printf(x)

/* Rearrange vertices by their site_id's */

static uns objects, skel_objects, all_objects;

#ifdef CONFIG_SITES
static u32 *sites;

#define ASORT_PREFIX(x) real_vertex_##x
#define ASORT_KEY_TYPE u32
#define ASORT_LT(x,y) (sites[x] < sites[y])
#include "ucw/sorter/array.h"
#endif

static void
compute_translation(void)
{
  uns i;
#ifdef CONFIG_SITES
  attrs_part_map(0);
  READ_ATTR(sites, site_id);
  attrs_part_unmap();

  u32 *real_vertex = big_alloc(objects * sizeof(u32));
  for (i=0; i<objects; i++)
    real_vertex[i] = i;
  real_vertex_sort(real_vertex, objects);

  FREE_ATTR(sites);
#else
  u32 *real_vertex = big_alloc(objects * sizeof(u32));
  for (i=0; i<objects; i++)
    real_vertex[i] = i;
#endif

  u32 *goes_to = big_alloc(objects * sizeof(u32));
  for (i=0; i<objects; i++)
    goes_to[real_vertex[i]] = i;
  write_free_ary(fn_graph_obj, FN_GRAPH_REAL, &real_vertex, objects, sizeof(u32));
  write_free_ary(fn_graph_obj, FN_GRAPH_GOES, &goes_to, objects, sizeof(u32));

  log(L_INFO, "Sorted objects by site_id");
}

/* Reading data for constructing the graph */

static bitarray_t is_redirect;

static void
read_redirects(void)
{
  is_redirect = big_alloc(BIT_ARRAY_BYTES(objects));
  bit_array_zero(is_redirect, objects);
  uns redirects = 0;
  notes_part_map(0);
  for (uns i=0; i<objects; i++)
    if (bring_note(i)->flags & CARD_NOTE_REDIRECT)
    {
      bit_array_set(is_redirect, i);
      redirects++;
    }
  notes_part_unmap();
  log(L_INFO, "Found %u redirects", redirects);
}

static void
renumber_chain(uns *merge, uns start)
{
  uns min = ~0U;
  uns cur = start;
  /* Renumber to the minimal vertex number from the equivalence class that is not a redirect.  */
  while (1)
  {
    if (!bit_array_isset(is_redirect, cur) && cur < min)
      min = cur;
    if (merge[cur] == cur)
      break;
    cur = merge[cur];
  }
  /* If there are only redirects, then we do not care, which one is chosen.
   * We only want that normal vertices are not merged with redirects.  */
  if (min == ~0U)
    min = start;
  uns next = start;
  do
  {
    cur = next;
    next = merge[cur];
    TRACE("%x -> %x (was %x)%s\n", cur, min, merge[cur],
	bit_array_isset(is_redirect, cur) ? " REDIRECT" : "");
    merge[cur] = min;
  }
  while (next != cur);
  TRACE("\n");
}

static uns *
read_merges(void)
{
  uns *merge = big_alloc(objects * sizeof(uns));
  for (uns i=0; i<objects; i++)
    merge[i] = i;
  // here you can exit if you want to skip the merging phase

  struct fastbuf *fprints = index_bopen(fn_fingerprints, O_RDONLY, 1);
  struct card_print p, last_p;
  last_p.cardid = 0xffffffff;
  uns classes = 0;
  while (breadb(fprints, &p, sizeof(p)))
  {
    if (p.cardid >= FIRST_ID_SKEL)
      continue;
    if (last_p.cardid == 0xffffffff)
    {
      last_p = p;
      continue;
    }
    if (!memcmp(&last_p.fp, &p.fp, sizeof(p.fp)))
      merge[p.cardid] = last_p.cardid;
    else
      classes++, renumber_chain(merge, last_p.cardid);
    last_p = p;
  }
  if (last_p.cardid != 0xffffffff)
    classes++, renumber_chain(merge, last_p.cardid);
  bclose(fprints);
  for (uns i=0; i<objects; i++)
    TRACE("%x: %x%s\n", i, merge[i], bit_array_isset(is_redirect, i) ? " REDIRECT" : "");
  log(L_INFO, "Found %u equivalence classes", classes);
  return merge;
}

#ifdef CONFIG_AREAS
static u32 *
read_areas(void)
{
  u32 *areas = big_alloc(all_objects * 4);
  attrs_part_map(0);
  for (uns i=0; i<objects; i++)
    areas[i] = bring_attr(i)->area;
  attrs_part_unmap();

  notes_skel_part_map(0);
  for (uns i=0; i<skel_objects; i++)
    areas[objects+i] = bring_skel_note(i)->area;
  notes_skel_part_unmap();

  return areas;
}
#endif

/* Merging vertices */

struct resolve_output {
  u32 dest, src;
};

static void
record_is_linked_and_has_target(bitarray_t is_linked, bitarray_t has_target)
{
  notes_part_map(1);
  for (uns i=0; i<objects; i++)				// mark which vertices have incoming links
  {
    if (bit_array_isset(is_linked, i))
      bring_note(i)->flags |= CARD_NOTE_IS_LINKED;
    if (bit_array_isset(has_target, i))
      bring_note(i)->flags |= CARD_NOTE_HAS_TARGET;
  }
  notes_part_unmap();
  notes_skel_part_map(1);
  for (uns i=objects; i<all_objects; i++)
    if (bit_array_isset(is_linked, i))
      bring_skel_note(i-objects)->flags |= CARD_NOTE_IS_LINKED;
  notes_skel_part_unmap();
}

static void
merge_vertices(struct fastbuf *in, struct fastbuf **out_obj, struct fastbuf **out_skel)
{
  read_redirects();
  uns *merge = read_merges();
#ifdef CONFIG_AREAS
  u32 *areas = read_areas();
  uns inter_area_cnt = 0;
#endif
#ifdef CONFIG_SITES
  u32 *sites;
  attrs_part_map(0);
  READ_ATTR(sites, site_id);
  attrs_part_unmap();
#endif
  bitarray_t is_linked = big_alloc_zero(BIT_ARRAY_BYTES(all_objects));
  bitarray_t has_target = big_alloc_zero(BIT_ARRAY_BYTES(objects));

  *out_obj = index_bopen_tmp(1);
  *out_skel = index_bopen_tmp(1);
  struct resolve_output link;
  while (bread(in, &link, sizeof(link)))
    {
      uns src = link.src & ~ETYPE_MASK;
      uns dest = link.dest;
      struct fastbuf *out;

      if (dest < FIRST_ID_SKEL)
      {
	out = *out_obj;
#ifdef CONFIG_AREAS
	if (areas[src] != areas[dest]) {
	  inter_area_cnt++;
	  continue;
	}
#endif
	if (!bit_array_isset(is_redirect, dest)		// let links point to the redirects
	    || bit_array_isset(is_redirect, src))	// unless the source vertex is a redirect
	{
	  dest = merge[dest];
	  link.dest = dest;
	}
	bit_array_set(is_linked, dest);
	if (!bit_array_isset(is_redirect, src))		// never merge redirects
	  src = merge[src];
#ifdef CONFIG_SITES
	if (sites[src] != sites[dest])
	  link.src |= ETYPE_INTERSITE;
#endif
      }
      else						// dest >= FIRST_ID_SKEL
      {
	out = *out_skel;
	ASSERT(dest != 0xffffffff);
	link.dest = dest & ~FIRST_ID_SKEL;
	dest += objects - FIRST_ID_SKEL;
#ifdef CONFIG_AREAS
	if (areas[src] != areas[dest]) {
	  inter_area_cnt++;
	  continue;
	}
#endif
	bit_array_set(is_linked, dest);
	if (!bit_array_isset(is_redirect, src))		// never merge redirects
	  src = merge[src];
	else
	  bit_array_set(has_target, src);		// mark redirects with a skeleton destination
							// (the others are handled later in backlinker)
      }
      link.src = (link.src & ETYPE_MASK) | src;
      bwrite(out, &link, sizeof(link));
    }
  bclose(in);
  big_free(is_redirect, BIT_ARRAY_BYTES(objects));
  big_free(merge, objects * sizeof(uns));
#ifdef CONFIG_AREAS
  log(L_INFO, "Dropped %u inter-area links", inter_area_cnt);
  big_free(areas, all_objects * 4);
#else
  log(L_INFO, "Merged vertices");
#endif
#ifdef CONFIG_SITES
  FREE_ATTR(sites);
#endif
  record_is_linked_and_has_target(is_linked, has_target);
  big_free(is_linked, BIT_ARRAY_BYTES(all_objects));
  big_free(has_target, BIT_ARRAY_BYTES(objects));
  brewind(*out_obj);
  brewind(*out_skel);
}

/* Presorting */

#define ASORT_PREFIX(x) dest_##x
#define ASORT_KEY_TYPE struct resolve_output
#define ASORT_LT(x,y) (x.dest < y.dest || x.dest == y.dest && x.src < y.src)
#include "ucw/sorter/array.h"

static u32 *goes_to;

#define ASORT_PREFIX(x) goes_dest_##x
#define ASORT_KEY_TYPE struct resolve_output
#define ASORT_LT(x,y) (goes_to[x.dest] < goes_to[y.dest] || x.dest == y.dest && x.src < y.src)
#include "ucw/sorter/array.h"

static struct fastbuf *presort_in;

static int
link_presort(struct fastbuf *dest, void *buf_ptr, size_t buf_size)
{
  uns record = sizeof(struct resolve_output);
  struct resolve_output *buf = buf_ptr;
  ASSERT(buf_size >= record);
  uns len = bread(presort_in, buf, MIN(~0U, buf_size) / record * record);
  if (!len)
    return 0;
  ASSERT(!(len % record));
  buf_size -= len;
  uns nr = len / record;
  if (goes_to)
    goes_dest_sort(buf, nr);
  else
    dest_sort(buf, nr);
  for (uns i=0; i<nr; )
  {
    uns start = i++, count = 1;
    for (; i<nr && buf[i].dest == buf[start].dest; i++)	// find and count the neighbors
      if (buf[i].src != buf[i-1].src)
	count++;
    bput_graph_hdr(dest, buf[start].dest, count);
    bputl(dest, buf[start++].src);
    for (; start < i; start++)				// and prune them
      if (buf[start].src != buf[start-1].src)
	bputl(dest, buf[start].src);
  }
  return 1;
}

/* Sorting neighbors with unifying */

#define ASORT_PREFIX(x) neighbors_##x
#define ASORT_KEY_TYPE uns
#define ASORT_ELT(i) (array[i] /*& ~ETYPE_MASK*/)		// we don't want to unify [redir], [frame], and [img]
#define ASORT_SWAP(i,j) do { uns e=array[j]; array[j]=array[i]; array[i]=e; } while(0)
#define ASORT_EXTRA_ARGS , uns *array
#include "ucw/sorter/array-simple.h"

static inline uns
sort_neighbors(u32 *buf, uns len)
{
  if (len <= 1)
    return len;
  neighbors_sort(len, buf);
  uns write = 1;
  for (uns i=1; i<len; i++)
    if (buf[i] != buf[i-1])
      buf[write++] = buf[i];
  return write;
}

/* Sorting by the source vertex after resolving */

struct link_merge {
  u32 dest, deg;
};

#define SORT_PREFIX(x) link_##x
#define SORT_KEY struct link_merge
#define SORT_DATA_SIZE(k) ((k).deg * sizeof(u32))
#define SORT_UNIFY
#define SORT_INPUT_PRESORT
#define SORT_OUTPUT_FILE

static inline int
link_compare(struct link_merge *a, struct link_merge *b)
{
  uns ai = a->dest;
  uns bi = b->dest;
  if (goes_to)				// FIXME: speedup since the test is constant in each of the two instances?
  {
    ai = goes_to[ai];
    bi = goes_to[bi];
  }
  COMPARE(ai, bi);
  return 0;
}

static int
link_read_key(struct fastbuf *fb, struct link_merge *k)
{
  return bget_graph_hdr(fb, &k->dest, &k->deg);
}

static void
link_write_key(struct fastbuf *fb, struct link_merge *k)
{
  bput_graph_hdr(fb, k->dest, k->deg);
}

#define	GBUF_TYPE	u32
#define	GBUF_PREFIX(x)	u32b_##x
#include "ucw/gbuf.h"

static void
link_write_merged(struct fastbuf *dest, struct link_merge **keys, void **data, uns n, void *buf)
{
// FIXME: what about direct merging with a heap?
  uns total = 0;
  for (uns i = 0; i < n; i++)
  {
    memcpy(buf + total * sizeof(u32), data[i], keys[i]->deg * sizeof(u32));
    total += keys[i]->deg;
  }
  total = sort_neighbors(buf, total);
  bput_graph_hdr(dest, keys[0]->dest, total);
  bwrite(dest, buf, total * sizeof(u32));
}

static void
link_copy_merged(struct link_merge **keys, struct fastbuf **data, uns n, struct fastbuf *dest)
{
  static u32b_t bb;
  uns total = 0;
  for (uns i = 0; i < n; i++)
  {
    u32b_grow(&bb, total + keys[i]->deg);
    breadb(data[i], bb.ptr + total, keys[i]->deg * sizeof(u32));
    total += keys[i]->deg;
  }
  total = sort_neighbors(bb.ptr, total);
  bput_graph_hdr(dest, keys[0]->dest, total);
  bwrite(dest, bb.ptr, total * sizeof(u32));
}

#include "ucw/sorter/sorter.h"

/* Construction of the graph index */

static uns last_index;

static void
append_graph_idx(struct fastbuf *graph_idx, uns node, ucw_off_t pos)
{
  if (goes_to)
    node = goes_to[node];
  while (last_index++ < node)
    bputo(graph_idx, -1);
  bputo(graph_idx, pos);
}

static uns total_v, max_id;
static u64 total_e;

static void
construct_index(byte *file, uns total_num, u32 *out_degree)
{
  struct fastbuf *graph = index_bopen(file, O_RDONLY, 1);
  struct fastbuf *graph_idx = index_bopen(stk_strcat(file, FN_GRAPH_INDEX), O_WRONLY | O_CREAT | O_TRUNC, 1);
  last_index = 0;
  total_v = max_id = 0;
  total_e = 0;
  //don't clear out_degree
  u32 dest, deg;
  ucw_off_t pos = 0;
  while (bget_graph_hdr(graph, &dest, &deg))
  {
    append_graph_idx(graph_idx, dest, pos);
    max_id = MAX(max_id, deg);
    total_v++;
    total_e += deg;
    while (deg--)
    {
      u32 src = bgetl(graph);
      if (out_degree)
	out_degree[src & ~ETYPE_MASK]++;
    }
    pos = btell(graph);
  }
  if (goes_to)						// for the sake of append_graph_idx()
  {
    big_free(goes_to, objects * sizeof(u32));
    goes_to = NULL;
  }
  append_graph_idx(graph_idx, total_num, pos);
  bclose(graph);
  bclose(graph_idx);

  uns max_od = 0;
  if (out_degree)
    for (uns i=0; i<objects; i++)
      max_od = MAX(max_od, out_degree[i]);
  log(L_INFO, "Built index of %s, %u vertices, %llu edges, in-deg %u, out-deg %d",
      file, total_v, (long long) total_e, max_id, max_od);
}

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

  byte *fp_path = index_name(fn_fingerprints);
  all_objects = ucw_file_size(fp_path) / sizeof(struct card_print);
  objects = ucw_file_size(index_name(fn_attributes)) / sizeof(struct card_attr);
  skel_objects = all_objects - objects;
  set_card_count(objects);
  log(L_INFO, "Processing %u objects + %u skeletons", objects, skel_objects);

  compute_translation();
  struct fastbuf *orig_links = index_bopen(fn_links, O_RDONLY, 1);
  struct fastbuf *resolved_links = resolve_fastbuf(orig_links, RESOLVE_SKIP_UNKNOWN, sizeof(struct link) - sizeof(struct fingerprint));
  struct fastbuf *links_obj, *links_skel;
  merge_vertices(resolved_links, &links_obj, &links_skel);

  presort_in = links_skel;
  link_sort(NULL, index_name(fn_graph_skel));
  bclose(presort_in);
  log(L_INFO, "Sorted %s", fn_graph_skel);
  alloc_read_ary(fn_graph_obj, FN_GRAPH_GOES, &goes_to, objects, sizeof(u32));
  presort_in = links_obj;
  link_sort(NULL, index_name(fn_graph_obj));
  bclose(presort_in);
  log(L_INFO, "Sorted %s", fn_graph_obj);

  u32 *out_degree = big_alloc_zero(sizeof(u32) * objects);
  construct_index(fn_graph_obj, objects, out_degree);		// frees goes_to
  struct fastbuf *fb_outdeg = index_bopen(stk_strcat(fn_graph_obj, FN_GRAPH_DEG), O_WRONLY | O_CREAT | O_TRUNC, 1);
  bwrite(fb_outdeg, out_degree, objects * sizeof(u32));
  bclose(fb_outdeg);
  construct_index(fn_graph_skel, skel_objects, out_degree);
  big_free(out_degree, sizeof(u32) * objects);

  return 0;
}
