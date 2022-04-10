/* 
 *   Finder of unreachable documents from a given init set
 *   
 *   (c) 2007, Pavel Charvat <pchar@ucw.cz>
 */

/* FIXME:
 * -- I have some ideas how to detect and filter out some surely unreachable vertices in DFS
 * -- decide what to do with merges
 * -- fix tracing levels
 * -- maybe replace radix_buf with a growing buffer + bitarray
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/getopt.h"
#include "ucw/lfs.h"
#include "ucw/bbuf.h"
#include "ucw/url.h"
#include "ucw/conf.h"
#include "ucw/stkstring.h"
#include "indexer/indexer.h"
#include "indexer/graph.h"
#include "indexer/merges.h"

#include <stdio.h>
#include <stdlib.h>

/*** Options and configuration ***/

static void NONRET
usage(void)
{
  fprintf(stderr, "\
Usage: find-unreachable [<options>] <init-set\n\
\n\
Options:\n\
-a  Add implicit auto-go-root edges\n\
-m  Merge equivalent vertices\n\
");
  exit(1);
}

static const char *short_opts = "am" CF_SHORT_OPTS;

static uns want_merges;
static uns want_auto_go_root;
static int trace_level = 1;
static u64 big_buf_size = 1024 * 1024;
static uns max_passes = 5;
static double refilter_threshold = 0.05;

#define XTRACE(l, m...) do { if ((l) <= trace_level) msg(L_INFO, m); } while(0)

static struct cf_section unreachable_config = {
  CF_ITEMS {
    CF_INT("Trace", &trace_level),
    CF_U64("BufSize", &big_buf_size),
    CF_UNS("Merges", &want_merges),
    CF_UNS("AutoGoRoot", &want_auto_go_root),
    CF_UNS("MaxPasses", &max_passes),
    CF_DOUBLE("Threshold", &refilter_threshold),
    CF_END
  }
};

static void CONSTRUCTOR
unreachable_config_init(void)
{
  cf_declare_section("Unreachable", &unreachable_config, 0);
}

/*** Graph sizes ***/

static uns num_obj;			/* Number of non-skeleton objects */
static uns num_skel;			/* Number of skeletons */
static uns num_all;			/* num_obj + num_skel */

static void
get_graph_sizes(void)
{
  num_all = ucw_file_size(index_name(fn_fingerprints)) / sizeof(struct card_print);
  num_obj = ucw_file_size(index_name(fn_attributes)) / sizeof(struct card_attr);
  num_skel = num_all - num_obj;
  XTRACE(1, "Processing %u objects and %u skeletons", num_obj, num_skel);
}

/*** Equivalence classes ***/

static s32 *classes;
static uns num_classes;			/* Number CLS_ROOT + CLS_VERTEX */

#define CLS_ROOT		-1	/* Class representant */
#define CLS_REACHABLE		-2	/* Reachable vertex */
#define CLS_UNREACHABLE		-3	/* Unreachable vertex */
#define CLS_VERTEX		-4	/* <= CLS_VERTEX ... class representant with pointer to vertices[-classes[i]] */

static void
classes_init(void)
{
  classes = big_alloc(4 * num_all);
  for (uns i = 0; i < num_all; i++)
    classes[i] = CLS_ROOT;
  num_classes = num_all;
}

static void
classes_cleanup(void)
{
  big_free(classes, 4 * num_all);
}

static void
load_merges(void)
{
  attrs_part_map(0);
  merges_map(0);
  ASSERT(card_count == num_obj);
  for (uns i = 0; i < num_obj; i++)
    /* FIXME: I really found `!(flags & CARD_FLAG_EMPTY) && (s32)merges[i] < 0' in final merges, is it correct? */
    if (!(bring_attr(i)->flags & CARD_FLAG_EMPTY) && (s32)merges[i] >= 0 && merges[i] != i)
      {
	ASSERT(merges[i] < num_obj);
	ASSERT(classes[merges[i]] == CLS_ROOT);
        classes[i] = merges[i];
	num_classes--;
      }
  XTRACE(1, "Loaded %u equivalence classes (including skeletons)", num_classes);
  merges_unmap();
  attrs_part_unmap();
}

static void
flatten_classes(void)
{
  DBG("Flattening classes");
  for (uns i = 0; i < num_all; i++)
    if (classes[i] >= 0 && classes[classes[i]] != CLS_ROOT && classes[classes[i]] > CLS_VERTEX)
      classes[i] = classes[classes[i]];
}

/*** Init set ***/

static void
load_init_set(void)
{
  XTRACE(1, "Loading the init set");
  uns num_init_obj = 0, num_init_skel = 0;
  struct fastbuf *urls = bfdopen_shared(0, indexer_fb_size);
  struct fastbuf *fps = index_bopen_tmp(1);
  struct fingerprint fp;
  bb_t bb;
  bb_init(&bb);
  while (bgets_bb(urls, &bb, ~0U))
    {
      byte buf[URL_KEY_BUF_SIZE];
      byte *key = url_key(bb.ptr, buf);
      fingerprint(key, &fp);
      bwrite(fps, &fp, sizeof(fp));
    }
  bb_done(&bb);
  bclose(urls);
  if (!btell(fps))
    bclose(fps);
  else
    {
      brewind(fps);
      struct fastbuf *ids = resolve_fastbuf(fps, RESOLVE_SKIP_UNKNOWN, 0);
      u32 id;
      while (breadb(ids, &id, 4))
        {
	  uns orig_id = id;
	  if (id >= FIRST_ID_SKEL)
	    id = id - FIRST_ID_SKEL + num_obj;
	  else if (classes[id] >= 0)
	    id = classes[id];
	  if (classes[id] != CLS_REACHABLE)
	    {
	      ASSERT(classes[id] == CLS_ROOT);
	      classes[id] = CLS_REACHABLE;
	      num_classes--;
	      if (orig_id >= FIRST_ID_SKEL)
		num_init_skel++;
	      else
		num_init_obj++;
	    }
	}
      bclose(ids);
      flatten_classes();
    }
  XTRACE(1, "Found %u initial classes and %u skeletons", num_init_obj, num_init_skel);
}

/*** Emulation of Shepherd's auto-go-root edges ***/

struct pair {
  u32 dest, src;
};

static s32 *auto_go_root;		/* Link lists of edges */

static void
gen_auto_go_root(void)
{
  log(L_INFO, "Generating implicit links to root pages");

  bb_t bb;
  bb_init(&bb);
  struct fastbuf *root_links = index_bopen_tmp(1);
  struct link link;
  link.src = 0;
  for (uns i = 0; i < 2; i++)
    {
      char *fn = i ? fn_skel_urls : fn_urls;
      struct fastbuf *urls = index_maybe_bopen(fn, O_RDONLY, 1);
      if (!urls)
        {
	  log(L_WARN, "Cannot open %s, auto-go-root edges from these URLs will be ignored", fn);
	  continue;
	}
      while (bgets_bb(urls, &bb, ~0U))
        {
	  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], buf3[MAX_URL_SIZE];
	  struct url u;
	  if (url_split(bb.ptr, &u, buf1))
	    continue;
	  if (!strcmp(u.rest, "/"))
	    continue;
	  u.rest = "/";
	  if (url_pack(&u, buf2) || url_enescape(buf2, buf3))
	    continue;
	  byte kbuf[URL_KEY_BUF_SIZE];
	  byte *key = url_key(buf3, kbuf);
	  fingerprint(key, &link.fp);
	  bwrite(root_links, &link, sizeof(link));
	  link.src++;
	}
      bclose(urls);
    }
  brewind(root_links);
  bb_done(&bb);

  struct fastbuf *resolved_links = resolve_fastbuf(root_links, RESOLVE_SKIP_UNKNOWN, sizeof(struct link) - sizeof(struct fingerprint));
  uns resolved_count = bfilesize(resolved_links) / sizeof(struct pair);
  if (!resolved_count)
    return;

  auto_go_root = big_alloc(num_all * 4);
  uns num_src = 0, num_dest = 0;
  for (uns i = 0; i < num_all; i++)
    auto_go_root[i] = i;
  struct pair rlink;
  while (breadb(resolved_links, &rlink, sizeof(rlink)))
    {
      int src = rlink.src;
      int dest = (rlink.dest < FIRST_ID_SKEL) ? rlink.dest : rlink.dest - FIRST_ID_SKEL + num_obj;
      if (auto_go_root[src] == src && (auto_go_root[dest] < 0 || auto_go_root[dest] == dest))
        {
	  num_src++;
	  if (auto_go_root[dest] == dest)
	    {
	      num_dest++;
	      auto_go_root[src] = dest;
	    }
	  else
	    auto_go_root[src] = ~auto_go_root[dest];
	  auto_go_root[dest] = ~src;
	}
    }
  bclose(resolved_links);

  log(L_INFO, "Generated %u auto-go-root edges to %u root pages", num_src, num_dest);
}

static void
cleanup_auto_go_root(void)
{
  if (auto_go_root)
    big_free(auto_go_root, num_all * 4);
}

/*** Bucket buffers ***/

struct vertex {
  u32 id;
  u32 val;
  u32 low;
  u32 next;
  u32 pos;
  u32 edges;
};

static s32 *big_buf, *big_buf_end;
static struct vertex *vertices;
static struct vertex *vertex_ptr;
static s32 *edges;
static s32 *edge_ptr;
static s32 *radix_buf;

static void
buf_reset(void)
{
  vertex_ptr = vertices - CLS_VERTEX;
  edge_ptr = big_buf_end;
#ifdef LOCAL_DEBUG
  for (uns i = 0; i < num_all; i++)
    ASSERT(!radix_buf[i]);
  for (uns i = 0; i < num_all; i++)
    ASSERT(classes[i] > CLS_VERTEX && classes[i] < 0 || classes[classes[i]] == CLS_ROOT);
#endif
}

static void
buf_init(void)
{
  u64 size = MAX(big_buf_size, 4 * num_all + 1024) / 4;
  size = MIN(size, 0x7fffffff);
  XTRACE(3, "Buffer size: %s", stk_fsize((u64)size * 4));
  big_buf = big_alloc(size * 4);
  big_buf_end = big_buf + size;
  vertices = (void *)big_buf;
  edges = big_buf;
  radix_buf = big_alloc_zero(4 * num_all);
  buf_reset();
}

static void
buf_cleanup(void)
{
  big_free(radix_buf, 4 * num_all);
  big_free(big_buf, 4 * (big_buf_end - big_buf));
}

/*** Statisticts ***/

static struct fastbuf *graph;

/* Pass ID */
static uns pass_no;

/* Per-pass stats */
static uns read_blocks;
static uns read_vertices;
static uns written_vertices;
static long long read_edges;
static long long written_edges;
static long long read_size;

/* Per-block stats */
static uns read_block_vertices;
static uns written_block_vertices;
static long long read_block_edges;
static long long written_block_edges;

static void
read_block_stats(void)
{
  XTRACE(2, "  << %u vertices and %llu edges", read_block_vertices, read_block_edges);
  read_vertices += read_block_vertices;
  read_edges += read_block_edges;
  read_block_vertices = read_block_edges = 0;
  read_blocks++;
}

static void
written_block_stats(void)
{
  XTRACE(2, "  >> %u vertices and %llu edges", written_block_vertices, written_block_edges);
  written_vertices += written_block_vertices;
  written_edges += written_block_edges;
  written_block_vertices = written_block_edges = 0;
}

static int
pass_stats(void)
{
  if (!pass_no++)
    XTRACE(1, "Initial: vertices=%u edges=%llu size=%s", read_vertices, read_edges, stk_fsize(read_size));
  XTRACE(1, "Pass %u: vertices=%u edges=%llu size=%s blocks=%u classes=%u", pass_no, written_vertices, written_edges, stk_fsize(btell(graph)), read_blocks, num_classes);

  int plan;
  if (!num_classes || read_vertices == written_vertices || read_blocks < 2)
    plan = -1;
  else if (pass_no < max_passes &&
      ((read_vertices > written_vertices && (double)(read_vertices - written_vertices) / read_vertices >= refilter_threshold) ||
      (read_edges > written_edges && (double)(read_edges - written_edges) / read_edges >= refilter_threshold)))
    plan = 1;
  else
    plan = 0;

  read_vertices = read_edges = read_size = read_blocks = 0;
  written_vertices = written_edges = 0;
  brewind(graph);

  return plan;
}

/*** Depth first search algorithm ***/

static u32 topol_list;	/* Link list of topologically sorted classes (the 'next' pointer is in v->low) */

static void
depth_first_search(void)
{
  /*
   * Algorithm overview with recursion (modified Tarjan's algorithm):
   *
   *  foreach v
   *    val = 1
   *    if v has not yet been visited
   *      DFS(v)				// found a new component
   *      while w = pop(stack)
   *        set_reachable(w)
   *
   *  DFS(v)
   *
   *    v.val = vlow = ++val
   *    push(stack, v)				
   *
   *    foreach e=(v,w) is an edge
   *      
   *      if w is reachable
   *        v.low = 0
   *
   *      else if w has not yet been visited	// if e is a tree edge
   *        DFS(w)				// recursive call
   *        v.low = MIN(v.low, w.low)
   *
   *      else if w in stack
   *        v.low = MIN(v.low, w.num)
   *
   *    if (v.low = v.num)			// the root of a strongly connected component
   *      repeat
   *        w = pop(stack)
   *        merge(v, w)				// merge 2 classes of vertices
   *      until v = w
   *
   *      add v to the list of topologically sorted vertices
   */

  DBG("Starting DFS");

  uns val = 1, depth = 0;
  u32 *topol_end = &topol_list;
  struct vertex *v, *w, *stack;

  for (uns i = 0; i < num_all; i++)

    /* Start a new tree */
    if (classes[i] <= CLS_VERTEX && !(v = vertices - classes[i])->val)
      {
	DBG("  Starting tree in vertex %x", v->id);

	v->low = v->val = ++val;
	v->pos = v->edges;

	v->next = -1;
	stack = v;

	/* The recursion */
	while (1)
	  {
	    s32 e = edges[v->pos];
	    if (e < 0)
	      if (e == -1)
	        {
		  /* There are no more edges from v */
		  if (v->low == v->val)
		    {
		      /* Found a strongly connected component, lets merge all vertices */
		      do
		        {
			  w = stack;
			  if (v != w)
			    {
			      /* Merge v and w */
			      DBG("    Merging %x and %x", v->id, w->id);
			      edges[w->pos] = -v->edges;
			      v->edges = w->edges;
			      classes[w->id] = v->id;
			      num_classes--;
			    }
			  stack = vertices + w->next;
			  w->next = 0;
		        }
		      while (v != w);

		      /* Topological sort */
		      *topol_end = v - vertices;
		      topol_end = &v->low;
		    }

		  /* Return from recursion */
		  if (!depth)
		    {
		      if (!v->low)
			do
			  {
			    w = stack;
			    DBG("    Marking %x as reachable", w->id);
			    classes[w->id] = CLS_REACHABLE;
			    num_classes--;
			    stack = vertices + w->next;
			    w->next = 0;
			  }
			while (v != w);
		      break;
		    }
		  w = v;
		  v = vertices + radix_buf[--depth];
		  radix_buf[depth] = 0;
		  if (v->low > w->low)
		    v->low = w->low;
		}
	      else
		/* Jump to next array of edges */
		v->pos = -e;
	    else
	      {
		/* Process edge */
		v->pos++;
		while (classes[e] >= 0)
		  e = classes[e];
		if (classes[e] <= CLS_VERTEX)
		  {
		    w = vertices - classes[e];
		    if (!w->val)
		      {
			/* Follow a tree edge */
			DBG("    Following tree edge %x <-- %x", v->id, w->id);
			w->val = w->low = ++val;
			radix_buf[depth++] = v - vertices;
			w->pos = w->edges;
			w->next = stack - vertices;
			v = stack = w;
		      }
		    else if (w->next)
		      {
			DBG("    Found type1 edge %x <-- %x", v->id, w->id);
		        if (v->low > w->val)
		          v->low = w->val;
		      }
		    else
		      DBG("    Found type2 edge %x <-- %x", v->id, w->id);
		  }
		else if (classes[e] == CLS_REACHABLE)
		  v->low = 0;
	      }
	  }
      }

  *topol_end = 0;
  DBG("Ending DFS");
}

/*** Bucket I/O ***/

static void
write_block(void)
{
  DBG("Writing block");

  /* Process all read classes in topological order */
  struct vertex *v;
  for (uns i = topol_list; i; i = v->low)
    {
      v = vertices + i;
      uns id = v->id;

      /* Gather all known edges to the current class */
      uns last = 0, deg = 0;
      u32 *e = edges + v->edges;
      while (1)
        {
	  s32 x = *e;
	  if (x >= 0)
	    {
	      e++;
	      if (classes[x] >= 0)
		x = classes[x];
	      if ((uns)x != id && !radix_buf[x])
	        {
		  radix_buf[x] = last + 1;
		  last = x;
		  deg++;
		}
	    }
	  else if (x == -1)
	    break;
	  else
	    e = edges - x;
	}

      /* Flush and reset the buffer */
      if (deg)
        {
	  written_block_vertices++;
	  written_block_edges += deg;
	  bput_graph_hdr(graph, id, deg);
	  while (deg--)
	    {
	      ASSERT((s32)last >= 0);
	      bputl(graph, last);
	      uns x = radix_buf[last];
	      radix_buf[last] = 0;
	      last = x - 1;
	    }
	}

      /* Reset classes for a next pass */
      classes[id] = CLS_ROOT;
    }
}

static void
flush_block(void)
{
  if (read_block_edges)
    {
      read_block_stats();
      depth_first_search();
      flatten_classes();
      write_block();
      buf_reset();
      written_block_stats();
    }
}

static inline uns
alloc_edges(uns dest, uns deg)
{
  if (unlikely(edge_ptr - (s32*)(vertex_ptr + 2) <= (int)deg))
    {
      flush_block();
      if (classes[dest] != CLS_ROOT && classes[dest] > CLS_VERTEX)
        return 1;
    }
  return 0;
}

static inline uns
add_edge(uns dest, uns src, uns *deg, uns *list)
{
  while (classes[src] >= 0)
    src = classes[src];
  if (src != dest && !radix_buf[src])
    {
      if (classes[src] == CLS_REACHABLE)
        {
	  ASSERT(classes[dest] == CLS_ROOT || classes[dest] <= CLS_VERTEX);
	  classes[dest] = CLS_REACHABLE;
	  num_classes--;
          while ((*deg)--)
	    {
	      uns x = radix_buf[*list];
	      radix_buf[*list] = 0;
	      *list = x - 1;
	    }
	  return 1;
	}
      radix_buf[src] = *list + 1;
      *list = src;
      (*deg)++;
    }
  return 0;
}

static inline void
flush_edges(uns dest, uns deg, uns list)
{
  if (!deg)
    return;
  *--edge_ptr = -1;
  uns pos = edge_ptr - edges;
  while (deg--)
    {
      *--edge_ptr = list;
      uns x = radix_buf[list];
      radix_buf[list] = 0;
      list = x - 1;
    }
  if (classes[dest] == CLS_ROOT)
    {
      /* Read a new class of vertices */
      struct vertex *v = ++vertex_ptr;
      classes[dest] = vertices - v;
      bzero(v, sizeof(*v));
      v->id = dest;
      v->edges = edge_ptr - edges;
      v->pos = pos;
      v++;
    }
  else
    {
      /* Append more edges to already read class */
      struct vertex *v = vertices - classes[dest];
      edges[v->pos] = edges - edge_ptr;
      v->pos = pos;
    }
}

/*** Filtering passes ***/

static inline void
filter_fastbuf(struct fastbuf *in, s32 *auto_go_root, uns dest_ofs)
{
  u32 dest, deg, olist, odeg;
  while (bget_graph_hdr(in, &dest, &deg))
    {
      read_block_vertices++;
      read_block_edges += deg;
      dest += dest_ofs;
      while (classes[dest] >= 0)
	dest = classes[dest];
      if (classes[dest] != CLS_ROOT && classes[dest] > CLS_VERTEX)
	goto skip;
      odeg = olist = 0;
      if (auto_go_root && auto_go_root[dest] < 0)
        {
	  uns adeg = 0;
	  for (int i = (s32)~auto_go_root[dest]; auto_go_root[i] >= 0; i = auto_go_root[i])
	    adeg++;
	  if (alloc_edges(dest, adeg + deg))
	    goto skip;
	  for (int i = (s32)~auto_go_root[dest]; auto_go_root[i] >= 0; i = auto_go_root[i])
	    {
	      read_block_edges++;
	      if (add_edge(dest, i, &odeg, &olist))
	        goto skip;
	    }
	}
      else if (alloc_edges(dest, deg))
	goto skip;
      while (deg--)
	if (add_edge(dest, bgetl(in) & ~ETYPE_MASK, &odeg, &olist))
	  goto skip;
      flush_edges(dest, odeg, olist);
      continue;
skip:
      bskip(in, deg * 4);
    }
  read_size += btell(in);
  bclose(in);
}

static void
filter_split_graph(void)
{
  /* Read object graph, add auto-go-root edges */
  XTRACE(2, "Filtering objects");
  graph = index_bopen_tmp(1);
  filter_fastbuf(index_bopen(fn_graph_obj, O_RDONLY, 1), auto_go_root, 0);
  cleanup_auto_go_root();

  /* Read skeleton graph */
  XTRACE(2, "Filtering skeletons");
  filter_fastbuf(index_bopen(fn_graph_skel, O_RDONLY, 1), NULL, num_obj);
  flush_block();
}

static void
refilter_graph(void)
{
  XTRACE(2, "Refiltering graph");
  struct fastbuf *in = graph;
  graph = index_bopen_tmp(1);
  filter_fastbuf(in, NULL, 0);
  flush_block();
}

/*** Final pass ***/

#define SORT_PREFIX(x) pairs_##x
#define SORT_KEY_REGULAR struct pair
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_UNIFY

static inline int
pairs_compare(struct pair *a, struct pair *b)
{
  COMPARE(a->src, b->src);
  COMPARE(a->dest, b->dest);
  return 0;
}

static inline void
pairs_write_merged(struct fastbuf *f, struct pair **keys, void **data UNUSED, uns n UNUSED, void *buf UNUSED)
{
  bwrite(f, *keys, sizeof(**keys));
}

#include "ucw/sorter/sorter.h"

static void
seek_graph(void)
{
  XTRACE(1, "Inverting graph");
  struct fastbuf *in = graph;
  struct pair pair;
  struct fastbuf *pairs = index_bopen_tmp(1);
  u32 deg;
  while (bget_graph_hdr(in, &pair.dest, &deg))
    {
      if (classes[pair.dest] >= 0)
	pair.dest = classes[pair.dest];
      while (deg--)
        {
	  pair.src = bgetl(in);
	  if (classes[pair.src] >= 0)
	    pair.src = classes[pair.src];
	  if (pair.src != pair.dest)
	    bwrite(pairs, &pair, sizeof(pair));
	}
    }
  bclose(in);
  brewind(pairs);

  XTRACE(2, "Sorting edge pairs");
  pairs = pairs_sort(pairs, NULL);

  XTRACE(2, "Reading edges");
  byte *idx = big_alloc((u64)(num_all + 1) * BYTES_PER_O);
  struct fastbuf *edges = bopen_tmp(8192);
  uns next = 0;
  while (breadb(pairs, &pair, sizeof(pair)))
    {
      while (next <= pair.src)
	PUT_O(idx + next++ * BYTES_PER_O, btell(edges));
      bputl(edges, pair.dest);
    }
  while (next <= num_all)
    PUT_O(idx + next++ * BYTES_PER_O, btell(edges));
  bclose(pairs);

  XTRACE(1, "Seeking edges of size %s", stk_fsize(btell(edges)));
  brewind(edges);
  u32 *stack = big_alloc(num_all * 4);
  uns num_seeks = 0;
  for (uns i = 0; i < num_all; i++)
    if (classes[i] == CLS_REACHABLE)
      {
	uns seek = 0, depth = 0;
	stack[depth++] = i;
	while (depth)
	  {
	    uns src = stack[--depth];
	    byte *p = idx + (u64)src * BYTES_PER_O;
	    ucw_off_t ofs = GET_O(p);
	    ucw_off_t end = GET_O(p + BYTES_PER_O);
	    uns deg = (end - ofs) / 4;
	    if (!deg)
	      continue;
	    if (!seek)
	      bskip(edges, ofs - btell(edges));
	    else
	      {
	        bsetpos(edges, ofs);
		num_seeks++;
	      }
	    seek = 1;
	    while (deg--)
	      {
		uns dest = bgetl(edges);
		if (classes[dest] == CLS_ROOT)
		  {
		    classes[dest] = CLS_REACHABLE;
		    if (dest < i)
		      stack[depth++] = dest;
		  }
	      }
	    ASSERT(btell(edges) == end);
	  }
	if (seek)
	  {
	    bsetpos(edges, GET_O(idx + (u64)(i + 1) * BYTES_PER_O));
	    num_seeks++;
	  }
      }
  big_free(idx, (u64)(num_all + 1) * BYTES_PER_O);
  big_free(stack, num_all * 4);
  bclose(edges);
  XTRACE(2, "Made %u seeks", num_seeks);

  XTRACE(2, "Finishing classes");
  for (uns i = 0; i < num_all; i++)
    {
      if (classes[i] >= 0)
	classes[i] = classes[classes[i]];
      if (classes[i] == CLS_ROOT)
	classes[i] = CLS_UNREACHABLE;
    }
  num_classes = 0;
}

/*** Results ***/

static void
dump_results(void)
{
  XTRACE(2, "Writing results");
  bb_t bb;
  bb_init(&bb);
  struct fastbuf *fb, *urls;
  uns reachable_obj = 0, unreachable_obj = 0;
  fb = index_bopen("unreachable-obj", O_CREAT | O_TRUNC | O_WRONLY, 1);
  urls = index_bopen(fn_urls, O_RDONLY, 1);
  for (uns i = 0; i < num_obj; i++)
    {
      bgets_bb(urls, &bb, ~0U);
      if (classes[i] == CLS_UNREACHABLE)
        {
	  bprintf(fb, "%08x %s\n", i, bb.ptr);
	  unreachable_obj++;
        }
      else if (classes[i] == CLS_REACHABLE)
	reachable_obj++;
    }
  bclose(fb);
  bclose(urls);

  uns reachable_skel = 0, unreachable_skel = 0;
  fb = index_bopen("unreachable-skel", O_CREAT | O_TRUNC | O_WRONLY, 1);
  urls = index_maybe_bopen(fn_skel_urls, O_RDONLY, 1);
  for (uns i = 0; i < num_skel; i++)
    {
      if (urls)
        bgets_bb(urls, &bb, ~0U);
      if (classes[i + num_obj] == CLS_UNREACHABLE)
        {
	  bprintf(fb, "%08x", i);
	  if (urls)
	    bprintf(fb, " %s", bb.ptr);
	  bputc(fb, '\n');
	  unreachable_skel++;
        }
      else if (classes[i + num_obj] == CLS_REACHABLE)
        reachable_skel++;
    }
  bclose(fb);
  bclose(urls);
  bb_done(&bb);

  uns reachable_all = reachable_obj + reachable_skel;
  uns unreachable_all = unreachable_obj + unreachable_skel;
  log(L_INFO, "Reachable: obj=%u (%.1f%%), skel=%u (%.1f%%), all=%u (%.1f%%)",
      reachable_obj, num_obj ? 100.0 * reachable_obj / num_obj : 100.0,
      reachable_skel, num_skel ? 100.0 * reachable_skel / num_skel : 100.0,
      reachable_all, num_all ? 100.0 * reachable_all / num_all : 100.0);
  log(L_INFO, "Unreachable: obj=%u (%.1f%%), skel=%u (%.1f%%), all=%u (%.1f%%)",
      unreachable_obj, num_obj ? 100.0 * unreachable_obj / num_obj : 0.0,
      unreachable_skel, num_skel ? 100.0 * unreachable_skel / num_skel : 0.0,
      unreachable_all, num_all ? 100.0 * unreachable_all / num_all : 0.0);

#if 0
  if (!num_classes)
    return;
  uns unknown_obj = num_obj - reachable_obj - unreachable_obj;
  uns unknown_skel = num_skel - reachable_skel - unreachable_skel;
  uns unknown_all = unknown_obj + unknown_skel;
  log(L_INFO, "Unknown: obj=%u (%.1f%%), skel=%u (%.1f%%), all=%u (%.1f%%), classes=%u",
      unknown_obj, num_obj ? 100.0 * unknown_obj / num_obj : 0.0,
      unknown_skel, num_skel ? 100.0 * unknown_skel / num_skel : 0.0,
      unknown_all, num_all ? 100.0 * unknown_all / num_all : 0.0,
      num_classes);
#else
  ASSERT(!num_classes);
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
	case 'a':
	  want_auto_go_root = 1;
	  break;
	case 'm':
	  want_merges = 1;
	  break;
	default:
	  usage();
      }
  if (argc != optind)
    usage();

  url_key_init();
  get_graph_sizes();
  classes_init();
  if (want_merges)
    load_merges();
  load_init_set();
  if (want_auto_go_root)
    gen_auto_go_root();
  buf_init();
  filter_split_graph();
  int plan;
  while ((plan = pass_stats()) > 0)
    refilter_graph();
  buf_cleanup();
  if (plan < 0)
    {
      for (uns i = 0; i < num_all; i++)
	if (classes[i] != CLS_REACHABLE)
	  classes[i] = CLS_UNREACHABLE;
      num_classes = 0;
      bclose(graph);
    }
  else
    seek_graph();
  dump_results();
  classes_cleanup();

  return 0;
}
