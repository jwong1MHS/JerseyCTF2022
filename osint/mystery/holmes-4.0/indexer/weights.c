/*
 *	Sherlock Indexer -- Dynamic Document Weights
 *
 *	(c) 2003--2007 Robert Spalek <robert@ucw.cz>
 *
 *	Based on the following articles:
 *
 *	Lawrence Page, Sergey Brin, Rajeev Motwani, and Terry Winograd (1998):
 *		The PageRank Citation Ranking: Bringing Order to the Web
 *		http://citeseer.nj.nec.com/page98pagerank.html
 *
 *	Christian Kohlschütter, Paul-Alexandru Chirita, and Wolfgang Nejdl (2006):
 *		Efficient Parallel Computation of PageRank
 *		http://www.l3s.de/~kohlschuetter/publications/kohlschuetter06efficient.pdf
 *
 *	Original version of this module was written by:
 *
 *	(c) 2001 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/math.h"
#include "ucw/lfs.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/url.h"
#include "ucw/stkstring.h"
#include "ucw/bitarray.h"
#include "ucw/threads.h"
#include "indexer/indexer.h"
#include "indexer/graph.h"

#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#define	PROFILE_TOD
#include "ucw/profile.h"

#define NO_THREADS					// compute everything in the main thread

/* Temporary functions */

static void *
my_mmap(byte *name, uns expected_size, uns write)
{
  uns size;
  void *ptr = mmap_file(index_name(name), &size, write);
  if (size != expected_size)
    die("Size of %s is %d, expected %d", name, size, expected_size);
  madvise(ptr, size, MADV_RANDOM);
  return ptr;
}
#define	MY_MMAP(var,name,cnt,write) var = my_mmap(name, cnt * sizeof(*var), write)
#define	MY_MUNMAP(var,cnt) munmap_file(var, cnt * sizeof(*var))

static inline void
my_partmap_restart(struct partmap *map)
{
  if (map->file_size)
    partmap_map(map, 0, 1);
}

static struct partmap *
my_partmap_open(byte *path, uns rw)
{
  struct partmap *map = partmap_open(index_name(path), rw);
  my_partmap_restart(map);
  return map;
}

#define PBRING(map,index,type)	((type *) partmap_map_forward(map, sizeof(type) * (ucw_off_t) index, sizeof(type)) )
#define BRING(map,index,type)	(* PBRING(map,index,type) )
#define PBRING_SLOW(map,index,type)	((type *) partmap_map(map, sizeof(type) * (ucw_off_t) index, sizeof(type)) )
#define BRING_SLOW(map,index,type)	(* PBRING_SLOW(map,index,type) )

/* Configuration */

static uns trace;
static uns threads;
static uns max_eigen_passes = 100;
static uns check_passes = 10;
static uns check_threshold = 1100;
static double min_change = 0;
static uns max_weight = 255;
static uns prob_random = 15;
static uns prob_weight = 0;
static uns prob_follow = 85;
static uns link_weight[2] = { 1, 1};
static double overrelax = 1.;
#define FN_GRAPH_NUMBER "-number"
static char *fn_intra_graph;
static char *fn_leaf_graph;
static char *fn_leaf_source;
static char *fn_obj_rank;
static char *fn_skel_rank;
static char *log_name;

static struct cf_section weights_config = {
  CF_ITEMS {
    CF_UNS("Trace", &trace),
    CF_UNS("Threads", &threads),
    CF_UNS("MaxEigenPasses", &max_eigen_passes),
    CF_UNS("CheckPasses", &check_passes),
    CF_UNS("CheckThreshold", &check_threshold),
    CF_DOUBLE("MinChange", &min_change),
    CF_UNS("MaxWeight", &max_weight),
    CF_UNS("ProbRandom", &prob_random),
    CF_UNS("ProbWeight", &prob_weight),
    CF_UNS("ProbFollow", &prob_follow),
    CF_UNS_ARY("LinkWeight", link_weight, 2),
    CF_DOUBLE("Overrelax", &overrelax),
    CF_STRING("IntraGraph", &fn_intra_graph),
    CF_STRING("LeafGraph", &fn_leaf_graph),
    CF_STRING("LeafSourceRank", &fn_leaf_source),
    CF_STRING("ObjRank", &fn_obj_rank),
    CF_STRING("SkelRank", &fn_skel_rank),
    CF_STRING("WeightLog", &log_name),
    CF_END
  }
};

/* Global variables */

static uns objects, skeletons;
static uns intras, leaves;
static rank_t source_mult, follow_mult;

static void
init_constants(void)
  /* Computes objects, skeletons, source_mult, and follow_mult.  */
{
  objects = card_count;
  if (!objects)
    {
      log(L_INFO, "Graph is empty, giving up.");
      exit(0);
    }
  skeletons = ucw_file_size(index_name(fn_notes_skel)) / sizeof(struct card_note);
  set_skel_count(skeletons);
  log(L_INFO, "Working with %d vertices and %d skeletons", objects, skeletons);
  uns prob_total = prob_random + prob_weight + prob_follow;
  ASSERT(prob_total > 0);
  source_mult = (prob_random + prob_weight + 0.) / prob_total;
  follow_mult = (prob_follow + 0.) / prob_total;
  if (trace >= 2)
    log(L_DEBUG, "Multiplicators: source %.4f, follow %.4f", source_mult, follow_mult);
}

/* Pre-processing of the link graphs */

struct leaf_node {				// plucking leaves
  uns v;					// virtual ID in the reordered graph
};

#define TREE_NODE	struct leaf_node
#define TREE_PREFIX(x)	leaf_##x
#define TREE_KEY_ATOMIC	v
#define TREE_ATOMIC_TYPE	uns
#define TREE_WANT_CLEANUP
#define TREE_WANT_NEW
#define TREE_WANT_ADJACENT
#define TREE_WANT_BOUNDARY
#define TREE_WANT_REMOVE
#include "ucw/redblack.h"

struct vertex_hdr {
  u32 dest, deg;
  u32 neigh[0];
} PACKED;

static s32 *leaves_plucked;			// sorting the leaf graph into a topological order

#define SORT_PREFIX(x) lg_leaves_##x
#define SORT_KEY struct vertex_hdr
#define SORT_DATA_SIZE(k) ((k).deg * sizeof(u32))
#define SORT_PRESORT
#define SORT_INPUT_FB
#define SORT_OUTPUT_FILE

static int
lg_leaves_compare(struct vertex_hdr *a, struct vertex_hdr *b)
{
  int pass_a = leaves_plucked[a->dest - intras];
  int pass_b = leaves_plucked[b->dest - intras];
  COMPARE(pass_a, pass_b);
  return 0;
}

static int
lg_leaves_read_key(struct fastbuf *f, struct vertex_hdr *k)
{
  return bget_graph_hdr(f, &k->dest, &k->deg);
}

static void
lg_leaves_write_key(struct fastbuf *f, struct vertex_hdr *k)
{
  bput_graph_hdr(f, k->dest, k->deg);
}

#include "ucw/sorter/sorter.h"

static int min_index = -1;
static uns seeks, memory, max_memory;
static struct leaf_tree leaf_tree;

static struct leaf_node *
remove_leaf(struct leaf_node *node, struct fastbuf *graph, struct fastbuf *graph_idx, s32 *outdeg, u32 *goes)
{
  bsetpos(graph_idx, node->v * BYTES_PER_O);
  ucw_off_t pos = bgeto(graph_idx);		// read position

  if (pos != (1LL << (BYTES_PER_O*8)) - 1)
  {
    bsetpos(graph, pos);			// read incoming links
    u32 dest, deg;
    bget_graph_hdr(graph, &dest, &deg);
    ASSERT(goes[dest] == node->v);
    while (deg--)				// decrease their out-degree
    {
      uns n = bgetl(graph) & ~ETYPE_MASK;
      ASSERT(outdeg[n] > 0);
      if (!--outdeg[n])
      {
	leaf_new(&leaf_tree, goes[n]);
	memory++;
	outdeg[n] = outdeg[dest]-1;
	min_index = MIN(min_index, outdeg[n]);
      }
    }
  }
  max_memory = MAX(memory, max_memory);

  struct leaf_node *next = leaf_adjacent(node, 1);
  leaf_remove(&leaf_tree, node);
  memory--;
  if (!next)					// try to seek as little as possible
  {
    next = leaf_boundary(&leaf_tree, 0);
    seeks++;
  }
  leaves++;
  return next;
}

static void
remove_leaves(void)
  /* Split the link graph into two and renumber vertices.  */
{
  s32 *outdeg;
  u32 *goes;
  alloc_read_ary(fn_graph_obj, FN_GRAPH_DEG, &outdeg, sizeof(s32), objects);
  alloc_read_ary(fn_graph_obj, FN_GRAPH_GOES, &goes, sizeof(u32), objects);

  struct fastbuf *graph = index_bopen(fn_graph_obj, O_RDONLY, 0);
  struct fastbuf *graph_idx = index_bopen(stk_strcat(fn_graph_obj, FN_GRAPH_INDEX), O_RDONLY, 0);
  struct fastbuf *fb_realv = index_bopen(stk_strcat(fn_graph_obj, FN_GRAPH_REAL), O_RDONLY, 1);

  leaf_init(&leaf_tree);
  uns i;
  leaves = 0;
  for (i=0; i<objects; i++)			// initial leaves
  {
    uns v = bgetl(fb_realv);
    if (!outdeg[v])
    {
      outdeg[v] = -1;
      struct leaf_node *node = leaf_new(&leaf_tree, i);
      memory++;
      remove_leaf(node, graph, graph_idx, outdeg, goes);
    }
  }
  bclose(fb_realv);
  log(L_INFO, "Found %d initial leaves", leaves);

  uns init = leaves;
  seeks = 1;
  struct leaf_node *curr = leaf_boundary(&leaf_tree, 0);
  while (curr)					// process all leaves
    curr = remove_leaf(curr, graph, graph_idx, outdeg, goes);
  leaf_cleanup(&leaf_tree);
  bclose(graph_idx);
  big_free(goes, objects * sizeof(u32));
  log(L_INFO, "Plucked %d new leaves in %d seeks, maximal chain %d, memory consumption %d", leaves - init, seeks, -min_index, max_memory);
  ASSERT(!memory);
  intras = objects - leaves;

  u32 *realv;					// compute new realv
  alloc_read_ary(fn_graph_obj, FN_GRAPH_REAL, &realv, objects, sizeof(u32));
  struct fastbuf *fb_realv2 = index_bopen(stk_strcat(fn_intra_graph, FN_GRAPH_REAL), O_WRONLY | O_CREAT | O_TRUNC, 1);
  for (i=0; i<objects; i++)			// renumber vertices to (1) intra
  {
    uns v = realv[i];
    if (outdeg[v] > 0)
      bputl(fb_realv2, v);
  }
  ASSERT(btell(fb_realv2) == (ucw_off_t) (intras * sizeof(u32)));
  for (i=0; i<objects; i++)			// and (2) leaves
  {
    uns v = realv[i];
    if (outdeg[v] <= 0)
    {
      ASSERT(outdeg[v] < 0);
      bputl(fb_realv2, v);
    }
  }
  ASSERT(btell(fb_realv2) == (ucw_off_t) (objects * sizeof(u32)));
  bclose(fb_realv2);
  big_free(realv, objects * sizeof(u32));

  u32 *realv2;
  alloc_read_ary(fn_intra_graph, FN_GRAPH_REAL, &realv2, objects, sizeof(u32));
  leaves_plucked = big_alloc(leaves * sizeof(u32));
  for (i=0; i<leaves; i++)			// remember for a moment the order of plucking leaves
    leaves_plucked[i] = outdeg[realv2[i + intras]];
  bzero(outdeg, objects * sizeof(u32));		// start counting from 0 again

  goes = big_alloc(objects * sizeof(u32));	// compute goes
  for (i=0; i<objects; i++)
    goes[realv2[i]] = i;
  big_free(realv2, objects * sizeof(u32));

  struct fastbuf *fb_num = index_bopen(stk_strcat(fn_intra_graph, FN_GRAPH_NUMBER), O_WRONLY | O_CREAT | O_TRUNC, 1);
  bputl(fb_num, intras);			// record #intras and #threads
  bputl(fb_num, threads);
  bputl(fb_num, 0);

  log(L_INFO, "Generated translation tables");

  bsetpos(graph, 0);				// split the graph into two and renumber
  struct fastbuf *graph_intra = index_bopen(stk_printf("%s-%d", fn_intra_graph, 0), O_WRONLY | O_CREAT | O_TRUNC, 1);
  struct fastbuf *graph_leaves = index_bopen_tmp(1);
  u32 dest, deg;
  uns curr_thread = 0;
  while (bget_graph_hdr(graph, &dest, &deg))
  {
    dest = goes[dest];
    struct fastbuf *out;
    if (dest < intras)				// split the intra graph for the parallel threads
    {
      uns thread = dest * threads / intras;
      if (thread > curr_thread)
      {
	bclose(graph_intra);
	graph_intra = index_bopen(stk_printf("%s-%d", fn_intra_graph, thread), O_WRONLY | O_CREAT | O_TRUNC, 1);
	curr_thread = thread;
	bputl(fb_num, dest);			// record the first vertex of the new thread
      }
      out = graph_intra;
    }
    else
      out = graph_leaves;
    bput_graph_hdr(out, dest, deg);		// works for both graphs
    while (deg--)
    {
      uns ss = bgetl(graph);
      uns s = /* (s & ETYPE_MASK) | */
	goes[ss & ~ETYPE_MASK];			// skip the type of the link; the code is here for debugging purposes only
      if (dest < intras || s >= intras)		// ignore links from intras to leafs
	outdeg[s] += link_weight[!!(ss & ETYPE_INTERSITE)];
      bputl(out, s | (ss & ETYPE_INTERSITE));
    }
  }
  write_free_ary(fn_intra_graph, FN_GRAPH_GOES, &goes, objects, sizeof(u32));	// only needed in debug/pagerank.c
  write_free_ary(fn_intra_graph, FN_GRAPH_DEG, &outdeg, objects, sizeof(s32));
  bclose(fb_num);
  bclose(graph);
  bclose(graph_intra);
  brewind(graph_leaves);
  log(L_INFO, "Link graph split into %d intras in %d threads, and %d leaves", intras, threads, leaves);

  lg_leaves_sort(graph_leaves, index_name(fn_leaf_graph));
  big_free(leaves_plucked, leaves * sizeof(u32));		// sort the leaf graph into a topological order
}

static u32 *thread_starts;

static void
prepare_vectors(void)
{
  /* Fill in intras and leaves again.  */
  struct fastbuf *fb_num = index_bopen(stk_strcat(fn_intra_graph, FN_GRAPH_NUMBER), O_RDONLY, 1);
  intras = bgetl(fb_num);
  leaves = objects - intras;
  threads = bgetl(fb_num);
  thread_starts = xmalloc((threads+1) * sizeof(u32));
  bread(fb_num, thread_starts, threads * sizeof(u32));
  thread_starts[threads] = intras;
  bclose(fb_num);

  /* Compute the normalisation coefficients for the initial rank vector s.t.
   * |v|=1 and set the probability multiplicators.  */
  byte *wt;
  READ_ATTR(wt, weight);
  struct partmap *realv = my_partmap_open(stk_strcat(fn_intra_graph, FN_GRAPH_REAL), 1);
  rank_t total_weight = 0.;
  my_partmap_restart(realv);
  for (uns i=0; i<intras; i++)
    total_weight += wt[BRING(realv,i,u32)];
  if (!total_weight)
  {
    total_weight = 1.;
    prob_random += prob_weight;
    prob_weight = 0;
  }
  rank_t rank_r = 1./intras * prob_random / (prob_random + prob_weight);
  rank_t rank_w = 1./total_weight * prob_weight / (prob_random + prob_weight);
  if (trace >= 2)
  {
    log(L_DEBUG, "Total weight of %d intras is %e", intras, total_weight);
    log(L_DEBUG, "Initial rank is %e + %e*weight", rank_r, rank_w);
  }

  /* The rank source needs to be smaller (typically 15%) than the initial rank
   * vector.  */
  struct fastbuf *b_source, *b_rank;
  b_source = index_bopen(fn_leaf_source, O_RDWR | O_CREAT | O_TRUNC, 1);
  b_rank = index_bopen(fn_obj_rank, O_RDWR | O_CREAT | O_TRUNC, 1);
  struct partmap *outdeg = my_partmap_open(stk_strcat(fn_intra_graph, FN_GRAPH_DEG), 0);
  my_partmap_restart(realv);
  /* Set the probability distribution on all intras.  */
  ASSERT(link_weight[0]);
  for (uns i=0; i<objects; i++)
  {
    u32 v = BRING(realv, i, u32);
    rank_t r = rank_r + rank_w * wt[v];
    if (trace >= 5)
      log(L_DEBUG, "%d. vertex %d: filter weight %d, rank %e", i, v, wt[v], r);
    uns odeg = BRING(outdeg, i, u32) ? : 1;
    if (i < intras) {					// used in the 1st pass
      rank_t r_start = r / odeg * link_weight[0];
      bwrite(b_rank, &r_start, sizeof(rank_t));
      rank_t r_source = r * source_mult * overrelax;
      bwrite(b_source, &r_source, sizeof(rank_t));
    } else {						// basis for the 2nd pass
      rank_t r_source = r / odeg * link_weight[0] * source_mult;
      bwrite(b_rank, &r_source, sizeof(rank_t));
    }
  }
  bclose(b_source);
  bclose(b_rank);
  partmap_close(outdeg);
  partmap_close(realv);
  FREE_ATTR(wt);

  b_rank = index_bopen(fn_skel_rank, O_WRONLY | O_CREAT | O_TRUNC, 1);
  notes_skel_part_map(0);
  for (uns i=0; i<skeletons; i++)			// skeletons
  {
    struct card_note *note = bring_skel_note(i);
    rank_t r = (rank_r + rank_w * note->weight_scanner) * source_mult;
    bwrite(b_rank, &r, sizeof(rank_t));
  }
  notes_skel_part_unmap();
  bclose(b_rank);

  log(L_INFO, "Set up starting vectors");
}

static void
dump(rank_t *w, uns n, uns exp_form)
{
  rank_t total = 0;
  for (uns i=0; i<n;)
    {
      printf("%6d:", i);
      for (uns j=0; j<5 && i<n; j++, i++)
	{
	  printf(exp_form ? " %12e" : " %12.6f", w[i]);
	  total += w[i];
	}
      putchar('\n');
    }
  printf("Total rank: %e\n", total);
  fflush(stdout);
}

/* The rank of an object (both internal and a leaf) is stored divided by its
 * out-degree (so that other vertices can easily sum up the contribution of
 * their ancestors).  Internal objects with zero in-degree converge to their
 * catalog weight * source_mult / outdeg.
 *
 * Leaves are not processed in the iterative process and their rank stays on
 * their initial value catalog_weight / outdeg.  Finally, skeletons are never
 * divided by their out-degree, and thus are initialized to their catalog
 * weight.  */

static rank_t
iteration(rank_t *rank, uns first, uns last, struct fastbuf *graph, struct partmap *rank_source, struct partmap *outdeg)
{
  my_partmap_restart(rank_source);
  my_partmap_restart(outdeg);
  bsetpos(graph, 0);
  rank_t delta = 0.;
  rank_t mult1 = follow_mult * overrelax, mult2 = 1 - overrelax;
  rank_t inter_site = (link_weight[1] + 0.) / link_weight[0];
  u32 next_dest, ideg;
  bget_graph_hdr(graph, &next_dest, &ideg);
  for (uns dest=first; dest<last; dest++)		// one step of the Gauss-Seidel method
  {
    rank_t new = 0.;
    if (dest == next_dest)
    {
      while (ideg--)					// sum all incoming links
      {
	uns ss = bgetl(graph), src = ss & ~ETYPE_MASK;
	if (ss & ETYPE_INTERSITE)
	  new += rank[src] * inter_site;
	else
	  new += rank[src];
      }
      new *= mult1;
      bget_graph_hdr(graph, &next_dest, &ideg);
    }
    new += BRING(rank_source, dest, rank_t);		// add source rank

    uns odeg = BRING(outdeg, dest, u32);
    new *= (link_weight[0] + 0.) / odeg;		// divide by outdeg for other vertices
    new += rank[dest] * mult2;				// successive overrelaxation

    delta += fabs(rank[dest] - new);
    rank[dest] = new;
  }
  return delta;
}

struct thread_data {
  rank_t *rank;
  struct fastbuf *graph;
  struct partmap *source, *outdeg;
  uns first, last;
  rank_t delta;
};

static void *
iteration_thread(void *arg)
{
  struct thread_data *thrd = arg;
  thrd->delta = iteration(thrd->rank, thrd->first, thrd->last, thrd->graph, thrd->source, thrd->outdeg);
  return thrd;
}

static void
find_eigen_vector(rank_t *rank)
  /* Finds the principal eigen-vector of the intra graph.  */
{
  struct thread_data thrd[threads];
  for (uns i=0; i<threads; i++)
  {
    thrd[i].rank = rank;
    thrd[i].graph = index_bopen(stk_printf("%s-%d", fn_intra_graph, i), O_RDONLY, 1);
    thrd[i].source = my_partmap_open(fn_leaf_source, 0);
    thrd[i].outdeg = my_partmap_open(stk_strcat(fn_intra_graph, FN_GRAPH_DEG), 0);
    thrd[i].first = thread_starts[i];
    thrd[i].last = thread_starts[i+1];
  }
#ifndef NO_THREADS
  pthread_attr_t attr;
  if (pthread_attr_init(&attr) < 0 ||
      pthread_attr_setstacksize(&attr, ucwlib_thread_stack_size) < 0)
    ASSERT(0);
#endif
  rank_t total = 0.;					// prefetch
  for (uns i=0; i<intras; i += 4096 / sizeof(rank_t))
    total += rank[i];

  rank_t delta = 0. * total, printed_delta = -1., check_delta = -1.;
  uns pass, delta_passes = 0;
  for (pass=0; pass<max_eigen_passes; pass++)
  {
    delta = 0.;
#ifdef NO_THREADS
    for (uns i=0; i<threads; i++)
    {
      iteration_thread(thrd+i);
      delta += thrd[i].delta;
    }
#else
    pthread_t thr[threads];
    for (uns i=0; i<threads; i++)
      if (pthread_create(thr+i, &attr, iteration_thread, thrd+i) < 0)
	die("pthread_create(%d): %m", i);
    for (uns i=0; i<threads; i++)
    {
      void *ret;
      if (pthread_join(thr[i], &ret) < 0)
	die("pthread_join(%d): %m", i);
      ASSERT(ret == thrd+i);
      delta += thrd[i].delta;
    }
#endif
    if (trace >= 1)
      log(L_INFO, "Pass %d: delta=%e", pass+1, delta);
    if (trace >= 10)
      dump(rank, intras, 1);
    if (delta < min_change)
    {
      pass++;
      break;
    }
    delta_passes++;
    if (printed_delta == -1)
    {
      printed_delta = check_delta = delta;
      log(L_INFO, "Initial delta %e", delta);
    }
    else if (delta < printed_delta/10)
    {
      log(L_INFO, "Delta improved %.4f times in %d passes",
	  printed_delta/delta, delta_passes);
      printed_delta = check_delta = delta;
      delta_passes = 0;
    }
    else if (!(delta_passes % check_passes))
    {
      if (delta > check_delta / (check_threshold/1000.))
      {
	log(L_INFO, "Delta has improved only %.4f<%.4f times in last %d passes, cancelling",
	  check_delta/delta, check_threshold/1000., check_passes);
	pass++;
	break;
      }
      else
	check_delta = delta;
    }
  }
  if (trace >= 5)
    dump(rank, intras, 1);
  for (uns i=0; i<threads; i++)
  {
    bclose(thrd[i].graph);
    partmap_close(thrd[i].source);
    partmap_close(thrd[i].outdeg);
  }
  log(L_INFO, "Converged in %d passes, delta is %e < %e)", pass, delta, min_change);
}

static void
suck_pagerank(rank_t *from, rank_t *to, struct fastbuf *graph, struct partmap *outdeg)
{
  if (outdeg)
    my_partmap_restart(outdeg);
  rank_t inter_site = (link_weight[1] + 0.) / link_weight[0];
  u32 dest, ideg;
  while (bget_graph_hdr(graph, &dest, &ideg))
  {
    rank_t new = 0.;
    while (ideg--)					// sum all incoming links
    {
      uns ss = bgetl(graph), src = ss & ~ETYPE_MASK;
      if (ss & ETYPE_INTERSITE)
	new += from[src] * inter_site;
      else
	new += from[src];
    }

    if (outdeg)
    {
      uns odeg = BRING_SLOW(outdeg, dest, u32) ? : 1;		// rank is stored divided by the out-degree
      to[dest] += new * follow_mult * link_weight[0] / odeg;	// leaves are stored divided by odeg, and skeletons are never divided
    }
    else							// skeletons are never didided
      to[dest] += new * follow_mult;
  }
}

static void
distribute_rank(rank_t *rank, rank_t *rank_skel)
{
  struct partmap *outdeg = my_partmap_open(stk_strcat(fn_intra_graph, FN_GRAPH_DEG), 0);
  struct fastbuf *graph = index_bopen(fn_leaf_graph, O_RDONLY, 1);
  suck_pagerank(rank, rank, graph, outdeg);		// into leaves
  bclose(graph);

  struct fastbuf *tmp_rank = index_bopen_tmp(1);
  bwrite(tmp_rank, rank, objects * sizeof(rank_t));
  brewind(tmp_rank);
  struct fastbuf *realv = index_bopen(stk_strcat(fn_intra_graph, FN_GRAPH_REAL), O_RDONLY, 1);
  for (uns i=0; i<objects; i++)				// shuffle objects back
  {
    uns real = bgetl(realv);
    rank_t r;
    bread(tmp_rank, &r, sizeof(rank_t));
    rank[real] = r;
  }
  bclose(tmp_rank);
  log(L_INFO, "Rank distributed into leaves");

  graph = index_bopen(fn_graph_skel, O_RDONLY, 1);
  suck_pagerank(rank, rank_skel, graph, NULL);		// into skeletons
  bclose(graph);
  log(L_INFO, "Rank distributed into skeletons");

  brewind(realv);
  my_partmap_restart(outdeg);
  for (uns i=0; i<objects; i++)				// multiply pagerank by the out-degree
  {
    uns real = bgetl(realv);
    uns odeg = BRING(outdeg, i, u32) ? : 1;
    rank[real] *= odeg / (link_weight[0] + 0.);
  }
  bclose(realv);
  partmap_close(outdeg);
}

struct statistics {
  uns count, count0;
  rank_t min, max, avg;
  float width;
};
static struct statistics s_doc, s_img;
#define HIST_BUCKETS 10
static uns hist_count[2][2*HIST_BUCKETS+1];

static inline void
stat_update(struct statistics *stat, rank_t r)
{
  if (!r)
  {
    stat->count0++;
    return;
  }
  if (!stat->count++)
    stat->min = stat->max = r;
  else
  {
    if (r < stat->min)
      stat->min = r;
    if (r > stat->max)
      stat->max = r;
  }
  stat->avg += r;
}
static inline float
rescale(float x)
  /* Maps [0,1] to [0,1].  */
{
  return sqrt(x);
  //return x;
}
static inline float
log_rank(struct statistics *stat, rank_t r)
{
  if (!r)				// possible if rank_r == 0
    return 0;
  return rescale(log10(r/stat->min) / stat->width) * max_weight;
}
static inline void
stat_finish(struct statistics *stat, byte *label)
{
  if (stat->count)
  {
    stat->avg /= stat->count;
    stat->width = log10(stat->max / stat->min);
    if (trace >= 1)
      log(L_INFO, "%s Ranks: count=%d+%d, min=%e (%.2f), max=%e (%.2f), avg=%e (%.2f)",
	  label, stat->count, stat->count0, stat->min, log_rank(stat, stat->min),
	  stat->max, log_rank(stat, stat->max),
	  stat->avg, log_rank(stat, stat->avg));
  }
  else
    stat->width = 1.;
}

static void
fill_image_out(struct fastbuf *graph, bitarray_t is_image, bitarray_t has_outedge)
{
  u32 dest, ideg;
  while (bget_graph_hdr(graph, &dest, &ideg))
    while (ideg--)
    {
      uns src = bgetl(graph);
      bit_array_set(has_outedge, src & ~ETYPE_MASK);
      if ((src & ETYPE_IMAGE) == ETYPE_IMAGE)
	bit_array_set(is_image, dest);
    }
}

static void
compute_renormalization(struct partmap *rank_obj, struct partmap *rank_skel, u32 *img_obj, u32 *img_skel)
{
  /* Browse graph-obj and graph-skel, and decide which objects are images.  */
  u32 *out_obj = big_alloc_zero(BIT_ARRAY_BYTES(objects));
  struct fastbuf *graph = index_bopen(fn_graph_obj, O_RDONLY, 0);
  fill_image_out(graph, img_obj, out_obj);
  bclose(graph);
  graph = index_bopen(fn_graph_skel, O_RDONLY, 1);
  fill_image_out(graph, img_skel, out_obj);
  bclose(graph);
  uns img = 0, false_img = 0;
  for (uns i=0; i<objects; i++)
    if (bit_array_isset(img_obj, i))
      if (bit_array_isset(out_obj, i))
      {
	bit_array_clear(img_obj, i);
	false_img++;
      }
      else
	img++;
  log(L_INFO, "Found %d images and %d false images", img, false_img);
  big_free(out_obj, BIT_ARRAY_BYTES(objects));

  /* Compute basic statistics.  */
  s_doc.count = s_img.count = 0;
  s_doc.count0 = s_img.count0 = 0;
  for (uns i=0; i<objects; i++)
    stat_update(bit_array_isset(img_obj, i) ? &s_img : &s_doc, BRING(rank_obj, i, rank_t));
  for (uns i=0; i<skeletons; i++)
    stat_update(bit_array_isset(img_skel, i) ? &s_img : &s_doc, BRING(rank_skel, i, rank_t));
  stat_finish(&s_doc, "Document");
  stat_finish(&s_img, "Image");
  ASSERT(s_doc.count + s_doc.count0 + s_img.count + s_img.count0 == objects + skeletons);
}

static void
dump_pagerank(int dump_level, uns count, struct partmap *rank, u32 *images, byte *nname)
{
  struct fastbuf *logf = NULL, *urlf = NULL;
  if (log_name && dump_level > 3)
  {
    logf = index_bopen(log_name, O_WRONLY | O_CREAT | O_TRUNC, 1);
    urlf = index_bopen(fn_urls, O_RDONLY, 1);
  }
  struct partmap *notes = my_partmap_open(nname, 1);
  ASSERT(partmap_size(notes) == (ucw_off_t)sizeof(struct card_note) * (ucw_off_t)count);
  my_partmap_restart(rank);
  for (uns i=0; i<count; i++)
  {
    uns is_image = bit_array_isset(images, i);
    rank_t origrank = BRING(rank, i, rank_t);
    rank_t logrank = log_rank(is_image ? &s_img : &s_doc, origrank);
    if (logf)
    {
      byte buf[MAX_URL_SIZE];
      sprintf(buf, "%08x %10.4e\t%7.3f\t%c", i, origrank, logrank, is_image ? 'i' : 'd');
      bputs(logf, buf);
      if (trace >= 2)
      {
	byte *s = bgets(urlf, buf, MAX_URL_SIZE);
	ASSERT(s);
	bputc(logf, ' ');
	bputs(logf, buf);
      }
      bputc(logf, '\n');
    }

    logrank += 0.5;
    byte wt = CLAMP(logrank, 0, 255);
    if (dump_level > 2)
      bring_attr(i)->weight = wt;
    PBRING(notes, i, struct card_note)->weight_dynamic = wt;
    hist_count[is_image ? 1 : 0] [ (int) ((wt + 0.) / max_weight * HIST_BUCKETS + HIST_BUCKETS) ]++;
  }
  partmap_close(notes);
  log(L_INFO, "Dumped %d ranks into %s%s", count, nname, dump_level > 2 ? " and attributes" : "");
  if (logf)
  {
    log(L_INFO, "Dumped pagerank into log-file");
    bclose(logf);
    bclose(urlf);
  }
}

static void
restore_weights(void)
{
  notes_part_map(0);
  for (uns i=0; i<objects; i++)
    {
      struct card_attr *attr = bring_attr(i);
      struct card_note *note = bring_note(i);
      attr->weight = note->weight_scanner;
    }
  notes_part_unmap();
  log(L_INFO, "Restored %d ranks from card notes", objects);
}

static void
soft_unlink(byte *path)
{
  path = index_name(path);
  if (unlink(path) && errno != ENOENT)
    die("Cannot unlink %s: %m", path);
}

static void
remove_temp_files(int level)
{
  if (level <= 1)
    return;
  if (level > 2)
  {
    soft_unlink(fn_leaf_graph);
    soft_unlink(fn_leaf_source);
    soft_unlink(stk_strcat(fn_intra_graph, FN_GRAPH_DEG));
    soft_unlink(stk_strcat(fn_intra_graph, FN_GRAPH_REAL));
    soft_unlink(stk_strcat(fn_intra_graph, FN_GRAPH_GOES));
    soft_unlink(stk_strcat(fn_intra_graph, FN_GRAPH_NUMBER));
    for (uns i=0; i<=threads; i++)
      soft_unlink(stk_printf("%s-%d", fn_intra_graph, i));
  }
  if (level > 4)
  {
    soft_unlink(fn_obj_rank);
    soft_unlink(fn_skel_rank);
  }
  log(L_INFO, "Removed temporary files at level %d", level);
}

static char *short_opts = CF_SHORT_OPTS "nGgPD:dR:";
static char *help = "\
Usage: weights [<options>]\n\
\n\
Options:\n"
CF_USAGE
"-n\tRestore initial weights from card-notes\n\
-G\tSkip the pruning of graph\n\
-g\tOnly prune graph and exit\n\
-P\tSkip the computation of PageRank\n\
-d\tOnly dump the PageRank and exit\n\
-D NUM\tDump PageRank into files with level < NUM (default=3)\n\
\tlog-file=3, attributes=2, notes=1\n\
-R NUM\tRemove files with level < NUM (default=4)\n\
\tweights=7, pagerank=4, pruned graph=2\n\
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
#define	RESTORE_WEIGHTS		1
#define	PRUNE_GRAPH		2
#define	COMPUTE_PAGERANK	4
  uns options = PRUNE_GRAPH | COMPUTE_PAGERANK;
  int opt, remove_level = 4, dump_level = 3;

  log_init(argv[0]);
  cf_declare_section("Weights", &weights_config, 0);
  while ((opt = cf_getopt(argc, argv, short_opts, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
    {
      case 'n':
	options |= RESTORE_WEIGHTS;
	break;
      case 'G':
	options &= ~PRUNE_GRAPH;
	remove_level = MIN(remove_level, 3);
	break;
      case 'g':
	options = PRUNE_GRAPH;
	remove_level = MIN(remove_level, 3);
	dump_level = 0;
	break;
      case 'P':
	options &= ~COMPUTE_PAGERANK;
	break;
      case 'D':
	dump_level = atoi(optarg);
	remove_level = MIN(remove_level, 4);
	break;
      case 'd':
	options = 0;
	remove_level = 0;
	break;
      case 'R':
	remove_level = atoi(optarg);
	break;
      default:
	usage();
    }
  if (optind < argc)
    usage();

  attrs_part_map(1);
  init_constants();

  if (options & RESTORE_WEIGHTS)
    restore_weights();
  if (options & PRUNE_GRAPH)
    remove_leaves();

  if (options & COMPUTE_PAGERANK)
  {
    prepare_vectors();

    rank_t *rank, *rank_skel;
    MY_MMAP(rank, fn_obj_rank, objects, 1);

    find_eigen_vector(rank);
    MY_MMAP(rank_skel, fn_skel_rank, skeletons, 1);
    distribute_rank(rank, rank_skel);

    MY_MUNMAP(rank, objects);
    MY_MUNMAP(rank_skel, skeletons);
  }

  if (dump_level > 1)
  {
    struct partmap *rank_obj = my_partmap_open(fn_obj_rank, 1);
    struct partmap *rank_skel = my_partmap_open(fn_skel_rank, 1);
    u32 *img_obj = big_alloc_zero(BIT_ARRAY_BYTES(objects));
    u32 *img_skel = big_alloc_zero(BIT_ARRAY_BYTES(skeletons));
    compute_renormalization(rank_obj, rank_skel, img_obj, img_skel);
    dump_pagerank(dump_level, objects, rank_obj, img_obj, fn_notes);
    if (skeletons > 0)
      dump_pagerank(2, skeletons, rank_skel, img_skel, fn_notes_skel);
    partmap_close(rank_obj);
    partmap_close(rank_skel);
    big_free(img_obj, BIT_ARRAY_BYTES(objects));
    big_free(img_skel, BIT_ARRAY_BYTES(skeletons));
    /* Dump a histogram.  */
    if (trace >= 1)
    {
      for (int b=-HIST_BUCKETS; b<=HIST_BUCKETS; b++)
	if (hist_count[0][b+HIST_BUCKETS] > 0 || hist_count[1][b+HIST_BUCKETS] > 0)
	  log(L_INFO, "%6.2f..%6.2f: %d documents, %d images",
	      (b+0.)/HIST_BUCKETS * max_weight, (b+1.)/HIST_BUCKETS * max_weight,
	      hist_count[0][b+HIST_BUCKETS], hist_count[1][b+HIST_BUCKETS]);
    }
  }
  remove_temp_files(remove_level);
  attrs_part_unmap();

  return 0;
}
