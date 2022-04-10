/*
 *	Sherlock Indexer -- Merging of Duplicate Images
 *
 *	(c) 2006--2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/math.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/mempool.h"
#include "ucw/bitarray.h"
#include "ucw/workqueue.h"
#include "ucw/stkstring.h"
#include "ucw/conf.h"
#include "ucw/clists.h"
#include "ucw/sorter/common.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"
#include "indexer/images.h"
#include "images/object.h"
#include "images/duplicates.h"
#include "images/math.h"

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>

/*** Configuration ***/

#define NUM_FEATURES 7 /* aspect ratio, L, u, v, LH, HL, HH */
#define CONTEXT_MEMORY_RESERVE (CPU_STRUCT_ALIGN * 4)

static uns image_dup_trace = 1;
static uns image_dup_trace_sorter = ~0U;
static uns image_dup_threads;
static uns image_dup_thread_stack_size;
static u64 image_dup_buf_size = 32U << 20;
static uns image_dup_max_cluster_count = ~0U;
static u64 image_dup_max_cluster_size = ~(u64)0;
static uns image_dup_kdtree_min_count = 100;
static uns image_dup_kdtree_max_depth = 20;
static uns image_dup_max_image_size = 4U << 20;
static uns image_dup_max_passes = 2;
static double image_dup_vector_weights[NUM_FEATURES] = { 1, 1, 1, 1, 0.2, 0.2, 0.2 };
static double image_dup_vector_threshold = 5;
static double image_dup_pixel_threshold = 10;
static double image_dup_aspect_ratio_threshold = 0.05;
static uns image_dup_res_support = 1;
static uns image_dup_res_min_cols = 16;
static uns image_dup_res_min_rows = 16;
static uns image_dup_res_min_thumb_cols = 4;
static uns image_dup_res_min_thumb_rows = 4;
static uns image_dup_same_size_compare = 2;
static uns image_dup_transformations;
static uns image_dup_qtree_limit = 8;

static char *
merge_images_commit(void *p UNUSED)
{
  for (uns i = 0; i < NUM_FEATURES; i++)
    if (!(image_dup_vector_weights[i] >= 0))
      return "MergeImages.VectorWeights is out of range";
  if (image_dup_transformations > 2)
    return "Invalid value of MergeImages.Transformations";
  return NULL;
}

static uns num_contexts;
static u64 context_buf_size;
static uns image_dup_res_flags;
static uns image_dup_nres_flags;
static uns vector_threshold;
static uns vector_threshold_sqr;

static void
finish_config(void)
{
  /* Separated from merge_images_commit() to avoid journaling */

  if (~image_dup_trace_sorter)
    sorter_trace = image_dup_trace_sorter;

  num_contexts = (image_dup_threads > 1) ? image_dup_threads : 1;
  context_buf_size = image_dup_buf_size / num_contexts;
  context_buf_size = MAX(65536, context_buf_size);
  context_buf_size = ALIGN_TO(context_buf_size, CPU_PAGE_SIZE);
  image_dup_max_image_size = (context_buf_size - CONTEXT_MEMORY_RESERVE) / 2;

  switch (image_dup_transformations)
    {
      case 0:
	image_dup_nres_flags = IMAGE_DUP_TRANS_ID;
	break;
      case 1:
	image_dup_nres_flags = IMAGE_DUP_TRANS_ID | IMAGE_DUP_ROT_CW | IMAGE_DUP_ROT_CCW | IMAGE_DUP_ROT_180;
	break;
      case 2:
	image_dup_nres_flags = IMAGE_DUP_TRANS_ALL;
	break;
      default:
	ASSERT(0);
    }
  image_dup_res_flags = image_dup_nres_flags | IMAGE_DUP_SCALE;

  vector_threshold = image_dup_vector_threshold + 0.5;
  vector_threshold_sqr = isqr(vector_threshold);
}

static struct cf_section merge_images_config = {
  CF_COMMIT(merge_images_commit),
  CF_ITEMS {
    CF_UNS("Trace", &image_dup_trace),
    CF_UNS("TraceSorter", &image_dup_trace_sorter),
    CF_UNS("Threads", &image_dup_threads),
    CF_UNS("ThreadStackSize", &image_dup_thread_stack_size),
    CF_U64("BufSize", &image_dup_buf_size),
    CF_UNS("MaxClusterCount", &image_dup_max_cluster_count),
    CF_U64("MaxClusterSize", &image_dup_max_cluster_size),
    CF_UNS("KdTreeMinCount", &image_dup_kdtree_min_count),
    CF_UNS("KdTreeMaxDepth", &image_dup_kdtree_max_depth),
    CF_UNS("MaxPasses", &image_dup_max_passes),
    CF_UNS("MaxImageSize", &image_dup_max_image_size),
    CF_DOUBLE_ARY("VectorWeights", image_dup_vector_weights, NUM_FEATURES),
    CF_DOUBLE("VectorThreshold", &image_dup_vector_threshold),
    CF_DOUBLE("PixelThreshold", &image_dup_pixel_threshold),
    CF_DOUBLE("AspectRatioThreshold", &image_dup_aspect_ratio_threshold),
    CF_UNS("SupportResize", &image_dup_res_support),
    CF_UNS("ResizeableMinWidth", &image_dup_res_min_cols),
    CF_UNS("ResizeableMinHeight", &image_dup_res_min_rows),
    CF_UNS("ResizeableMinThumbWidth", &image_dup_res_min_thumb_cols),
    CF_UNS("ResizeableMinThumbHeight", &image_dup_res_min_thumb_rows),
    CF_UNS("SameSizeCompare", &image_dup_same_size_compare),
    CF_UNS("Transformations", &image_dup_transformations),
    CF_UNS("QuadTreeLimit", &image_dup_qtree_limit),
    CF_END
  }
};

/*** BASIC DEFINITIONS ***/

#define XTRACE(level, m...) do { if ((level) <= image_dup_trace) msg(((level) <= 1) ? L_INFO : L_DEBUG, m); } while (0)
#define CTRACE(ctx, level, m, params...) XTRACE(level, "%s" m, (ctx)->msg_prefix, ##params)

#define FLAG_CLUSTER_LAST	0x80000000

struct stats {
  clist contexts;			/* List of unfinished tasks */
  uns dups;				/* Number of found new duplicates */
  unsigned long long cmp_pairs;		/* Number of compared vectors */
  unsigned long long cmp_pixmaps;	/* Number of compared pixmaps */
  unsigned long long cmp_levels;	/* Number of compared pixmap levels */
  unsigned long long cmp_pixels;	/* Number of compared pixels */
  unsigned long long kdtree_down;	/* Number of calls to kdtree_down() */
  unsigned long long kdtree_up;		/* Number of calls to kdtree_up() */
};

struct context {
  struct work w;
  cnode n;				/* Node in merge_stats->contexts or ready_contexts */
  uns id;				/* Context ID */
  char *msg_prefix;			/* A prefix with context ID added in front of each message */
  void *buf;				/* A buffer of context_buf_size bytes */
  void *buf_end;			/* End of the buffer */
  void *buf_ptr1;			/* First unallocated byte */
  void *buf_ptr2;			/* Last unallocated byte */
  struct mempool *pool;			/* A memory pool for some relatively small allocations */
  struct image_context ic;		/* ImageLib context */
  struct image_io io;			/* Image I/O context */
  struct image_obj_info ioi;		/* Context for decoding of thumbnails */
  struct image_dup_context idc;		/* Context for comparing of Quad-Trees */
  struct node *nodes;			/* List of nodes ("vectors") */
  u32 *list;				/* Pointers to the nodes array */
  struct image_dup **dups;		/* Quad-Trees and releted data */
  struct kdnode *kdtree;		/* Kd-Tree root */
  struct kdnode *leaves;		/* Link list of Kd-Tree leaves (the right comes first) */
  uns count;				/* Original number of images (before merges) */
  struct stats stats;			/* Context stats */
  struct stats *merge_stats;		/* Where to merge context stats after the thread finished its work */
  char group_name[64];			/* Used by flush_memory() to save the current group's name */
};

struct node {
  u32 card;				/* Real card ID */
  u32 size;				/* Expected memory size */
  s32 val;				/* Various meanings */
  byte f[NUM_FEATURES];			/* See NUM_FEATURES and init_node() for description */
};

struct kdnode {
  byte bbox[NUM_FEATURES][2];		/* Subtree's bounding box */
  uns split;				/* Splitting dimension or ~0U if the node contains leaves */
  struct kdnode *parent;		/* Parent node */
  union {
    struct kdnode *son[2];		/* Internal node's sons */
    struct {
      struct kdnode *next;		/* Next leaf in the link list of all leaves */
      uns index, end;			/* Interval in context.{list,dups} arrays */
    };
  };
};

static struct worker_pool context_pool;
static struct work_queue context_queue;
static struct context *contexts;
static clist ready_contexts;

static inline uns
same_size_compare(uns resizeable)
{
  return !resizeable < image_dup_same_size_compare;
}

static void
init_stats(struct stats *s)
{
  bzero(s, sizeof(*s));
  clist_init(&s->contexts);
}

static void
merge_stats(struct stats *dest, struct stats *src)
{
  dest->dups += src->dups;
  dest->cmp_pairs += src->cmp_pairs;
  dest->cmp_pixmaps += src->cmp_pixmaps;
  dest->cmp_levels += src->cmp_levels;
  dest->cmp_pixels += src->cmp_pixels;
  dest->kdtree_down += src->kdtree_down;
  dest->kdtree_up += src->kdtree_up;
}

static void
context_init(struct context *c, uns id)
{
  bzero(c, sizeof(*c));
  c->id = id;
  c->msg_prefix = ~id ? xstrdup(stk_printf("C%u: ", id)) : "";
  CTRACE(c, 3, "Initializing context");
  ASSERT(!(context_buf_size & (CPU_PAGE_SIZE - 1)));
  c->buf = c->buf_ptr1 = big_alloc(context_buf_size);
  c->buf_end = c->buf_ptr2 = c->buf + context_buf_size;
  c->pool = mp_new(65536);
  image_context_init(&c->ic);
  if (!image_io_init(&c->ic, &c->io))
    die("%sCannot initialize image I/O", c->msg_prefix);
  image_dup_context_init(&c->ic, &c->idc);
  c->idc.error_threshold = image_dup_pixel_threshold * image_dup_pixel_threshold * 3 + 0.5;
  c->idc.ratio_threshold = 128.5 + image_dup_aspect_ratio_threshold * 128;
  c->idc.qtree_limit = image_dup_qtree_limit;
}

static void
context_cleanup(struct context *c)
{
  CTRACE(c, 3, "Cleaning up context");
  image_dup_context_cleanup(&c->idc);
  image_io_cleanup(&c->io);
  image_context_cleanup(&c->ic);
  big_free(c->buf, c->buf_end - c->buf);
  mp_delete(c->pool);
}

static void
init_contexts(void)
{
  contexts = xmalloc(num_contexts * sizeof(*contexts));
  clist_init(&ready_contexts);
  for (uns i = 0; i < num_contexts; i++)
    {
      context_init(&contexts[i], (image_dup_threads > 1) ? i : ~0U);
      clist_add_tail(&ready_contexts, &contexts[i].n);
    }
  if (image_dup_threads > 1)
    {
      context_pool.num_threads = image_dup_threads;
      context_pool.stack_size = image_dup_thread_stack_size;
      worker_pool_init(&context_pool);
      work_queue_init(&context_pool, &context_queue);
    }
}

static void
cleanup_contexts(void)
{
  if (image_dup_threads > 1)
    {
      work_queue_cleanup(&context_queue);
      worker_pool_cleanup(&context_pool);
    }
  for (uns i = 0; i < num_contexts; i++)
    context_cleanup(&contexts[i]);
}

static inline void
context_flush_idc_stats(struct context *c)
{
  c->stats.cmp_levels += c->idc.sum_depth;
  c->stats.cmp_pixels += c->idc.sum_pixels;
  c->idc.sum_depth = 0;
  c->idc.sum_pixels = 0;
}

static void
context_return(struct context *c)
{
  ASSERT(c);
  context_flush_idc_stats(c);
  merge_stats(c->merge_stats, &c->stats);
  clist_remove(&c->n);
  clist_add_head(&ready_contexts, &c->n);
}

static struct context *
context_wait(struct stats *s)
{
  struct context *c;
  if (clist_empty(&ready_contexts))
    {
      ASSERT(image_dup_threads > 1);
      c = (void *)work_wait(&context_queue);
      context_return(c);
    }
  c = SKIP_BACK(struct context, n, clist_head(&ready_contexts));
  clist_remove(&c->n);
  clist_add_tail(&s->contexts, &c->n);
  bzero(&c->stats, sizeof(c->stats));
  c->merge_stats = s;
  c->buf_ptr1 = c->buf;
  c->buf_ptr2 = c->buf_end;
  return c;
}

static void
context_submit(struct context *c, void (*go)(struct worker_thread *t, struct work *w))
{
  if (image_dup_threads > 1)
    {
      c->w.go = go;
      c->w.priority = 0;
      work_submit(&context_queue, &c->w);
    }
  else
    {
      go(NULL, &c->w);
      context_return(c);
    }
}

static void
sync_contexts(struct stats *s)
{
  if (image_dup_threads > 1)
    while (!clist_empty(&s->contexts))
      context_return((void *)work_wait(&context_queue));
}

static inline size_t
context_avail(struct context *c)
{
  return c->buf_ptr2 - c->buf_ptr1;
}

static inline void *
context_alloc_start(struct context *c, size_t size)
{
  size = ALIGN_TO(size, CPU_STRUCT_ALIGN);
  ASSERT(context_avail(c) >= size);
  void *p = c->buf_ptr1;
  c->buf_ptr1 += size;
  return p;
}

static inline void *
context_alloc_end(struct context *c, size_t size)
{
  ASSERT(context_avail(c) >= size);
  return c->buf_ptr2 -= size;
}

static inline void *
context_free_end(struct context *c, size_t size)
{
  ASSERT((size_t)(c->buf_end - c->buf_ptr2) >= size);
  void *p = c->buf_ptr2;
  c->buf_ptr2 += size;
  return p;
}

static void
cluster_read(struct context *c, struct fastbuf *fb_thumbs)
{
  u64 sum_size = 0;
  uns max_size = 0;
  for (uns i = 0; i < c->count; i++)
    {
      struct image_thumb hdr;
      if (!breadb(fb_thumbs, &hdr, sizeof(hdr)))
	ASSERT(0);
      uns size = sizeof(hdr) + hdr.thumb_size;
      void *p = context_alloc_end(c, size);
      memcpy(p, &hdr, sizeof(hdr));
      breadb(fb_thumbs, p + sizeof(hdr), hdr.thumb_size);
      sum_size += size;
      max_size = MAX(max_size, size);
    }
  CTRACE(c, 3, "Read %s of thumbnails (max=%s, avg=%s)", stk_fsize(sum_size), stk_fsize(max_size), stk_fsize(sum_size / c->count));
}

static void
cluster_decode(struct context *c)
{
  c->dups = context_alloc_start(c, c->count * sizeof(*c->dups));
  u64 sum_size = 0;
  uns max_size = 0;
  for (uns i = c->count; i--; )
    {
      struct image_thumb hdr;
      memcpy(&hdr, context_free_end(c, sizeof(hdr)), sizeof(hdr));
      ASSERT(hdr.id == c->list[i]);
      c->ioi.thumb_size = hdr.thumb_size;
      c->ioi.thumb_format = hdr.thumb_format;
      c->ioi.thumb_data = context_free_end(c, hdr.thumb_size);
      struct fastbuf cfb;
      fbbuf_init_read(&cfb, c->ioi.thumb_data, c->ioi.thumb_size, 0);
      image_io_reset(&c->io);
      struct image *img = read_image_obj_thumb(&c->ioi, &cfb, &c->io, c->pool);
      if (!img)
	die("%sCannot read thumbnail %08x", c->msg_prefix, c->nodes[hdr.id].card);
      uns same_size = same_size_compare(c->idc.flags & IMAGE_DUP_SCALE);
      struct image_dup *dup = context_alloc_start(c, image_dup_estimate_size(img->cols, img->rows, same_size, image_dup_qtree_limit));
      if (!image_dup_new(&c->idc, img, dup, same_size))
	die("%sCannot create comparison structure for thumbnail %08x", c->msg_prefix, c->nodes[hdr.id].card);
      c->dups[i] = dup;
      mp_flush(c->pool);
      sum_size += c->nodes[c->list[i]].size;
      max_size = MAX(max_size, c->nodes[c->list[i]].size);
    }
  CTRACE(c, 3, "Decoded %s of %u images (max=%s, avg=%s)", stk_fsize(sum_size), c->count, stk_fsize(max_size), stk_fsize(sum_size / c->count));
}

static inline uns
compare(struct context *c, uns i, uns j)
{
  c->stats.cmp_pairs++;
  struct node *n_i = c->nodes + c->list[i];
  struct node *n_j = c->nodes + c->list[j];
  uns dist = 0;
  for (uns k = 0; k < NUM_FEATURES; k++)
    dist += isqr((int)n_i->f[k] - (int)n_j->f[k]);
  if (dist > vector_threshold_sqr)
    return 0;
  c->stats.cmp_pixmaps++;
  return image_dup_compare(&c->idc, c->dups[i], c->dups[j]);
}

static void
merge(struct context *c, uns i, uns j)
{
  c->stats.dups++;
  struct node *n_i = c->nodes + c->list[i];
  struct node *n_j = c->nodes + c->list[j];
  if (image_dup_trace >= 3)
    {
      char buf[NUM_FEATURES * 8 + 2], *buf_ptr = buf;
      uns dist = 0;
      for (uns i = 0; i < NUM_FEATURES; i++)
        {
	  int d = (int)n_i->f[i] - (int)n_j->f[i];
	  dist += isqr(d);
	  buf_ptr += sprintf(buf_ptr, "%u,", ABS(d));
	}
      buf_ptr[-1] = 0;
      uns size_i = c->dups[i]->image.cols * c->dups[i]->image.rows;
      uns size_j = c->dups[j]->image.cols * c->dups[j]->image.rows;
      CTRACE(c, 3, "Merging %08x and %08x (dist=[%s]=%.1f, err=%.1f, scale=%.1f%%)",
	  n_i->card, n_j->card, buf, sqrt(dist), sqrt(c->idc.error / 3.0),
	  100 * sqrt((size_i <= size_j) ? (double)size_i / size_j : (double)size_j / size_i));
    }

  /* Yes, this is really thread-safe, because we always work with at most one image from each class
   * and this image is hold by at most one context at a time */
  if (merges_union(n_i->card, n_j->card))
    ASSERT(0);
  n_j->card = ~0U;
  c->list[j] = ~0U;
  c->dups[j] = NULL;
}

static void
search_trivial(struct context *c)
{
  uns last = c->count - 1;
  for (uns i = 0; i < last; i++)
    for (uns j = i + 1; j <= last; j++)
      if (compare(c, i, j))
        {
	  merge(c, i, j);
	  c->list[j] = c->list[last];
	  c->dups[j] = c->dups[last];
	  last--;
	  j--;
        }
  context_flush_idc_stats(c);
  CTRACE(c, 3, "Avg comparisons: vec=%.1f, pm=%.1f, lv=%.1f, px=%.1f",
      (double)c->stats.cmp_pairs / c->count, (double)c->stats.cmp_pixmaps / c->count,
      (double)c->stats.cmp_levels / c->count, (double)c->stats.cmp_pixels / c->count);
}

static void
build_kdtree(struct context *c)
{
  uns count = c->count;
  ASSERT(count > 1);
  u32 *list = c->list;
  struct node *nodes = c->nodes;
  struct image_dup **dups = c->dups;
  struct kdnode *kdnodes = context_alloc_start(c, 2 * count * sizeof(*kdnodes));
  uns max_depth = 1, num_leaves = 0, max_leaf_diag = 0, max_leaf_count = 0;
  u64 sum_leaves_diag = 0, sum_leaves_count = 0;

  uns depth_limit = 1;
  while ((1U << depth_limit) < count)
    depth_limit++;
  depth_limit *= 2;
  depth_limit = MIN(depth_limit, image_dup_kdtree_max_depth);

  /* Initialize recursion */
  struct stk {
    uns depth;
    struct kdnode *node;
  } stk_top[depth_limit + 1], *stk = stk_top + 1;
  struct kdnode *kdn = c->kdtree = kdnodes++;
  stk->node = kdn;
  stk->depth = 1;
  kdn->index = 0;
  kdn->end = count;
  kdn->parent = NULL;
  c->leaves = NULL;

  /* Main loop */
  while (stk != stk_top)
    {
      /* Compute bbox */
      struct kdnode *kdn = stk->node;
      uns count = kdn->end - kdn->index;
      for (uns j = 0; j < NUM_FEATURES; j++)
	kdn->bbox[j][0] = kdn->bbox[j][1] = nodes[list[kdn->index]].f[j];
      for (uns i = kdn->index + 1; i < kdn->end; i++)
	for (uns j = 0; j < NUM_FEATURES; j++)
	  {
	    kdn->bbox[j][0] = MIN(kdn->bbox[j][0], nodes[list[i]].f[j]);
	    kdn->bbox[j][1] = MAX(kdn->bbox[j][1], nodes[list[i]].f[j]);
	  }
      uns diag = 0;
      for (uns j = 0; j < NUM_FEATURES; j++)
	diag += isqr(kdn->bbox[j][1] - kdn->bbox[j][0]);

      /* Split conditions */
      if (count < 4 || stk->depth >= depth_limit || diag * 4 <= vector_threshold_sqr)
	kdn->split = ~0U;
      else
        {
	  /* Split the axis with maximum bbox size */
	  kdn->split = 0;
	  uns dif = kdn->bbox[0][1] - kdn->bbox[0][0];
	  for (uns i = 1; i < NUM_FEATURES; i++)
	    if ((uns)(kdn->bbox[i][1] - kdn->bbox[i][0]) > dif)
	      {
		kdn->split = i;
		dif = kdn->bbox[i][1] - kdn->bbox[i][0];
	      }
	}

      /* Leaf node */
      if (!~kdn->split)
        {
	  kdn->next = c->leaves;
	  c->leaves = kdn;
          stk--;
	  num_leaves++;
	  diag = fast_sqrt_u32(diag);
	  max_leaf_diag = MAX(max_leaf_diag, diag);
	  sum_leaves_diag += diag;
	  max_leaf_count = MAX(max_leaf_count, count);
	  sum_leaves_count += count;
	}

      /* Internal node */
      else
        {
	  /* Choose a random pivot */
	  uns min = kdn->bbox[kdn->split][0];
	  uns max = kdn->bbox[kdn->split][1];
	  ASSERT(min < max);
	  uns pivot = nodes[list[kdn->index + random_max(count)]].f[kdn->split];
	  if (pivot == min)
	    pivot++;

	  /* Split */
	  uns l = kdn->index;
	  uns r = l + count - 1;
	  while (1)
	    {
	      while (nodes[list[l]].f[kdn->split] < pivot)
		l++;
	      while (nodes[list[r]].f[kdn->split] >= pivot)
		r--;
	      if (l <= r)
	        {
		  u32 x = list[l];
		  list[l] = list[r];
		  list[r] = x;
		  void *y = dups[l];
		  dups[l] = dups[r];
		  dups[r] = y;
		  l++;
		  r--;
		}
	      else
		break;
	    }
	  ASSERT(l == r + 1);

	  /* Create sons */
	  stk[1].depth = ++(stk[0].depth);
	  max_depth = MAX(max_depth, stk[1].depth);
	  stk[0].node = kdnodes++;
	  stk[1].node = kdnodes++;
	  stk[1].node->index = kdn->index;
	  stk[0].node->index = stk[1].node->end = l;
	  stk[0].node->end = kdn->index + count;
	  stk[1].node->parent = stk[0].node->parent = kdn;
	  kdn->son[0] = stk[1].node;
	  kdn->son[1] = stk[0].node;
	  stk++;
	}
    }

  kdn = c->kdtree;
  uns root_diag = 0;
  for (uns i = 0; i < NUM_FEATURES; i++)
    root_diag += isqr(kdn->bbox[i][1] - kdn->bbox[i][0]);
  root_diag = fast_sqrt_u32(root_diag);

  CTRACE(c, 3, "Built Kd-Tree of %u depth and %u leaves (root diag=%u, max leaf diag=%u, avg=%u; max leaf size=%u, avg=%.1f)",
    max_depth, num_leaves, root_diag, max_leaf_diag, (uns)(sum_leaves_diag / num_leaves), max_leaf_count, (double)sum_leaves_count / num_leaves);
}

static inline uns
kdtree_up(struct context *c, struct kdnode *k, struct node *n)
{
  c->stats.kdtree_up++;
  for (uns i = 0; i < NUM_FEATURES; i++)
    if (n->f[i] >= k->bbox[i][0] - vector_threshold || n->f[i] <= k->bbox[i][1] + vector_threshold)
      return 1;
  return 0;
}

static inline uns
kdtree_down(struct context *c, struct kdnode *k, struct node *n)
{
  c->stats.kdtree_down++;
  uns dist = 0;
  for (uns i = 0; i < NUM_FEATURES; i++)
    {
      if (n->f[i] < k->bbox[i][0])
	dist += isqr(k->bbox[i][0] - n->f[i]);
      else if (n->f[i] > k->bbox[i][1])
	dist += isqr(n->f[i] - k->bbox[i][1]);
    }
  return dist <= vector_threshold_sqr;
}

static void
search_kdtree(struct context *c)
{
  struct node *nodes = c->nodes;
  u32 *list = c->list;

  /* For each Kd-Tree leaf */
  for (struct kdnode *kdleaf = c->leaves; kdleaf; kdleaf = kdleaf->next)
    {
      /* For each picture in this leaf */
      for (uns i = kdleaf->index; i < kdleaf->end; i++)
        {
	  struct node *node = &nodes[list[i]];
          struct kdnode *top = kdleaf;

	  /* Inspect the current leaf */
	  for (uns j = i + 1; j < kdleaf->end; j++)
	    {
	      if (compare(c, i, j))
	        {
		  merge(c, i, j);
		  kdleaf->end--;
		  c->list[j] = c->list[kdleaf->end];
		  c->dups[j] = c->dups[kdleaf->end];
		  j--;
		}
	    }

	  /* Find all near leaves */
	  struct kdnode *x = kdleaf, *y;
	  while (1)
	    {
	      while (y = x->parent)
	        {
		  if (x == top)
		    if (kdtree_up(c, x, node))
		      top = y;
		    else
		      {
			y = NULL;
			break;
		      }
		  if (x == y->son[1])
		    break;
		  x = y;
		}
	      if (!y)
		break;
	      x = y->son[0];
	      while (kdtree_down(c, x, node))
	        {
		  if (!~x->split)
		    {
		      for (uns j = x->index; j < x->end; j++)
			if (compare(c, i, j))
			  {
			    merge(c, i, j);
			    x->end--;
			    c->list[j] = c->list[x->end];
			    c->dups[j] = c->dups[x->end];
			    j--;
			  }
		      break;
		    }
		  x = x->son[1];
		}
	    }
	}
    }
  context_flush_idc_stats(c);
  CTRACE(c, 3, "Avg comparisons: vec=%.1f, pm=%.1f, lv=%.1f, px=%.1f; Kd-Tree tests: down=%.1f, up=%.1f",
      (double)c->stats.cmp_pairs / c->count, (double)c->stats.cmp_pixmaps / c->count,
      (double)c->stats.cmp_levels / c->count, (double)c->stats.cmp_pixels / c->count,
      (double)c->stats.kdtree_down / c->count, (double)c->stats.kdtree_up / c->count);
}

static void
cluster_search(struct context *c)
{
  if (c->count < image_dup_kdtree_min_count)
    search_trivial(c);
  else
    {
      build_kdtree(c);
      search_kdtree(c);
    }
}

static uns
aspect_ratio(uns cols, uns rows)
{
  double x = (!image_dup_transformations || cols >= rows) ? x = cols / (double)rows : rows / (double)cols;
  if (x < 1)
    x = 1 / x;
  x = log2(x) * 32 * image_dup_vector_weights[0];
  if (image_dup_transformations)
    x += 128;
  if (!(x > 0))
    return 0;
  if (x >= 255)
    return 255;
  return x;
}

static inline uns
estimate_size(struct image_thumb *t, uns same_size_compare)
{
  uns size = image_dup_estimate_size(t->thumb_cols, t->thumb_rows, same_size_compare, image_dup_qtree_limit);
  size = ALIGN_TO(size, CPU_STRUCT_ALIGN);
  size = MAX(size, sizeof(*t) + t->thumb_size);
  size += sizeof(struct image_dup *) + 2 * sizeof(struct kdnode);
  return size;
}

static void
init_node(struct node *n, struct image_thumb *t, uns same_size_compare)
{
  ASSERT(IMAGE_VEC_F < NUM_FEATURES);
  n->f[0] = aspect_ratio(t->cols, t->rows);
  for (uns i = 1; i < NUM_FEATURES; i++)
    n->f[i] = CLAMP(t->vector.f[i - 1] * image_dup_vector_weights[i], 0, 255);
  n->card = t->id;
  n->size = estimate_size(t, same_size_compare);
}

#define ASORT_PREFIX(x) random_splits_##x
#define ASORT_ELT(i) (list[i])
#define ASORT_KEY_TYPE u32
#define ASORT_LT(x,y) (nodes[x].val < nodes[y].val)
#define ASORT_EXTRA_ARGS , u32 *list, struct node *nodes
#include "ucw/sorter/array-simple.h"

static void
split_randomly(u32 *list, uns count, struct node *nodes, size_t avail)
{
  /* Initialize recursion */
  struct stk {
    uns count;
    u32 *start;
  } stk_top[64], *stk = stk_top + 1;
  stk->start = list;
  stk->count = count;
  avail = MIN(avail, image_dup_max_cluster_size);

  uns num_clusters = 0;
  uns max_count = 0;

  /* Main loop */
  while (stk != stk_top)
    {
      /* Split conditions */
      uns split;
      if (stk->count < 2)
	split = 0;
      else if (stk->count > image_dup_max_cluster_count)
	split = 1;
      else
        {
	  s64 size = avail;
	  for (uns i = 0; i < stk->count && size >= 0; i++)
	    size -= nodes[stk->start[i]].size;
	  split = size < 0;
	}

      /* BSP leaf node */
      if (!split)
        {
	  num_clusters++;
	  max_count = MAX(max_count, stk->count);
	  stk->start[stk->count - 1] |= FLAG_CLUSTER_LAST;
          stk--;
        }

      /* BSP internal node */
      else
        {
          /* Generate normal vector of a random splitting plane */
          int normal[NUM_FEATURES];
	  uns zero;
	  do
	    {
	      zero = 0;
              for (uns i = 0; i < NUM_FEATURES; i++)
                zero |= (normal[i] = random_max(0x20001) - 0x10000);
	    }
	  while (!zero);

	  /* Compute dot produts */
	  for (uns i = 0; i < stk->count; i++)
	    {
	      int dot_prod = 0;
	      for (uns j = 0; j < NUM_FEATURES; j++)
                dot_prod += normal[j] * nodes[stk->start[i]].f[j];
	      nodes[stk->start[i]].val = dot_prod;
            }

	  /* Sort... could be faster, because we only need the median */
	  random_splits_sort(stk->count, stk->start, nodes);

	  /* Split in the middle */
	  stk[1].count = stk[0].count >> 1;
	  stk[0].count -= stk[1].count;
	  stk[1].start = stk[0].start;
	  stk[0].start += stk[1].count;
	  stk++;
	}
    }
  for (uns i = 0; i < count; i++)
    nodes[list[i] & ~FLAG_CLUSTER_LAST].val = i;

  XTRACE(3, "Randomly split %u vectors to %u clusters (avg=%u, max=%u)", count, num_clusters, count / num_clusters, max_count);
}

static struct node *random_nodes;

#define SORT_PREFIX(x) random_##x
#define SORT_KEY struct image_thumb
#define SORT_DATA_SIZE(k) ((k).thumb_size)
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

static int
random_read_key(struct fastbuf *f, struct image_thumb *k)
{
  while (breadb(f, k, sizeof(*k)))
    if (random_nodes[k->id].card != (u32)~0U)
      return 1;
    else
      bskip(f, k->thumb_size);
  return 0;
}

static void
random_write_key(struct fastbuf *f, struct image_thumb *k)
{
  bwrite(f, k, sizeof(*k));
}

static inline int
random_compare(struct image_thumb *a, struct image_thumb *b)
{
  COMPARE(random_nodes[a->id].val, random_nodes[b->id].val);
  return 0;
}

#include "ucw/sorter/sorter.h"

/*** Processing of independent groups of images ***/

struct group {
  char *name;
  struct stats *stats;
  struct context *ctx;
  void *thumbs;
  struct fastbuf *fb_thumbs;
  struct fastbuf *fb_nodes;
  uns count;
  u64 estimated_size;
  uns flags;
};

static void
group_start(struct group *g, char *name, uns flags, uns memory, struct stats *stats)
{
  bzero(g, sizeof(*g));
  g->name = name;
  g->flags = flags;
  g->stats = stats;
  if (memory)
    g->ctx = context_wait(g->stats);
  else
    {
      g->fb_thumbs = index_bopen_tmp(1);
      g->fb_nodes = index_bopen_tmp(1);
    }
}

static void
group_add(struct group *g, struct image_thumb *hdr, struct fastbuf *data)
{
  struct node node;
  init_node(&node, hdr, same_size_compare(g->flags & IMAGE_DUP_SCALE));
  hdr->id = g->count;
  g->estimated_size += node.size;
  if (!g->fb_thumbs)
    {
      struct node *nodes = g->ctx->buf_ptr1;
      if (context_avail(g->ctx) >= (g->count + 1) * (sizeof(node) + sizeof(u32)) + g->estimated_size + CONTEXT_MEMORY_RESERVE ||
	  g->count >= image_dup_max_cluster_count || g->estimated_size >= image_dup_max_cluster_size)
        {
	  breadb(data, context_alloc_end(g->ctx, hdr->thumb_size), hdr->thumb_size);
	  memcpy(context_alloc_end(g->ctx, sizeof(*hdr)), hdr, sizeof(*hdr));
	  node.val = hdr->thumb_size;
	  memcpy(&nodes[g->count], &node, sizeof(node));
	}
      else
        {
	  g->fb_thumbs = index_bopen_tmp(1);
	  g->fb_nodes = index_bopen_tmp(1);
	  bwrite(g->fb_nodes, nodes, g->count * sizeof(node));
	  void *p = g->ctx->buf_end;
	  for (uns i = 0; i < g->count; i++)
	    {
	      p -= sizeof(*hdr) + nodes[i].val;
	      bwrite(g->fb_thumbs, p, sizeof(*hdr) + nodes[i].val);
	    }
	  context_return(g->ctx);
	}
    }
  if (g->fb_thumbs)
    {
      bwrite(g->fb_thumbs, hdr, sizeof(*hdr));
      bbcopy(data, g->fb_thumbs, hdr->thumb_size);
      bwrite(g->fb_nodes, &node, sizeof(node));
    }
  g->count++;
}

static void
go_memory(struct worker_thread *t UNUSED, struct work *w)
{
  struct context *c = (void *)w;
  c->nodes = context_alloc_start(c, c->count * sizeof(struct node)); // Filled in group_add()
  c->list = context_alloc_start(c, c->count * sizeof(u32));
  for (uns i = 0; i < c->count; i++)
    c->list[i] = i;
  cluster_decode(c);
  cluster_search(c);
  CTRACE(c, 2, "Processed group `%s' of %u images in memory, found %u dups (%.1f%%)",
      c->group_name, c->count, c->stats.dups, 100.0 * c->stats.dups / c->count);
}

static void
flush_memory(struct group *g)
{
  CTRACE(g->ctx, 3, "Flushing group `%s' of %u images", g->name, g->count);
  g->ctx->count = g->count;
  g->ctx->idc.flags = g->flags;
  strcpy(g->ctx->group_name, g->name);
  context_submit(g->ctx, go_memory);
}

static void
go_fastbufs(struct worker_thread *t UNUSED, struct work *w)
{
  struct context *c = (void *)w;
  cluster_decode(c);
  cluster_search(c);
}

static void
flush_fastbufs(struct group *g)
{
  XTRACE(2, "Flushing group `%s' of %u images", g->name, g->count);

  u32 *list = big_alloc(g->count * sizeof(*list));
  struct node *nodes = big_alloc(g->count * sizeof(*nodes));
  brewind(g->fb_nodes);
  for (uns i = 0; i < g->count; i++)
    if (!breadb(g->fb_nodes, &nodes[i], sizeof(*nodes)))
      ASSERT(0);
  bclose(g->fb_nodes);

  struct stats stats, pass_stats;
  init_stats(&stats);
  for (uns pass = 1; pass <= image_dup_max_passes; pass++)
    {
      init_stats(&pass_stats);
      brewind(g->fb_thumbs);
      uns n = 0;
      for (uns i = 0; i < g->count; i++)
	if (nodes[i].card != (u32)~0U)
	  list[n++] = i;
      if (n < 2)
        {
	  XTRACE(2, "Pass %u: Skipped, the group is trivial", pass);
	  break;
	}
      split_randomly(list, n, nodes, context_buf_size - CONTEXT_MEMORY_RESERVE);
      random_nodes = nodes;
      g->fb_thumbs = random_sort(g->fb_thumbs, NULL);
      uns j = 0, num_clusters = 0;
      for (uns i = 0; i < n; i++)
	if (list[i] & FLAG_CLUSTER_LAST)
	  {
	    list[i] &= ~FLAG_CLUSTER_LAST;
	    struct context *c = context_wait(&pass_stats);
	    c->idc.flags = g->flags;
	    c->count = i - j + 1;
	    c->list = &list[j];
	    c->nodes = nodes;
	    cluster_read(c, g->fb_thumbs);
	    context_submit(c, go_fastbufs);
	    num_clusters++;
	    j = i + 1;
	  }
      sync_contexts(&pass_stats);
      XTRACE(3, "Pass %u: Avg comparisons: vec=%.1f, pm=%.1f, lv=%.1f, px=%.1f; Kd-Tree tests: down: %.1f, up: %.1f",
          pass, (double)pass_stats.cmp_pairs / n, (double)pass_stats.cmp_pixmaps / n,
	  (double)pass_stats.cmp_levels / n, (double)pass_stats.cmp_pixels / n,
	  (double)pass_stats.kdtree_down / n, (double)pass_stats.kdtree_up / n);
      merge_stats(&stats, &pass_stats);
      double dups_percent = 100.0 * pass_stats.dups / n;
      if (num_clusters > 1)
	XTRACE(2, "Pass %u: Split to %u random clusters, found %u/%u dups (%.1f%%)", pass, num_clusters, pass_stats.dups, n, dups_percent);
      else
        {
          XTRACE(2, "Pass %u: Processed in memory, found %u dups (%.1f%%)", pass, pass_stats.dups, dups_percent);
	  break;
	}
    }
  bclose(g->fb_thumbs);
  XTRACE(2, "Processed group `%s' of of %u images, found %u dups (%.1f%%)",
      g->name, g->count, stats.dups, 100.0 * stats.dups / g->count);
  merge_stats(g->stats, &stats);

  big_free(nodes, g->count * sizeof(*nodes));
  big_free(list, g->count * sizeof(*list));
}

static void
group_flush(struct group *g)
{
  if (g->count < 2)
    if (g->fb_thumbs)
      {
	bclose(g->fb_thumbs);
	bclose(g->fb_nodes);
      }
    else
      context_return(g->ctx);
  else
    if (g->fb_thumbs)
      flush_fastbufs(g);
    else
      flush_memory(g);
}

/*** Main splits ***/

#define SORT_PREFIX(x) size_##x
#define SORT_KEY_REGULAR struct image_thumb
#define SORT_DATA_SIZE(k) ((k).thumb_size)
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

static inline void
normalize_image_size(uns *cols, uns *rows)
{
  if (image_dup_transformations && *cols < *rows)
    {
      uns x = *cols;
      *cols = *rows;
      *rows = x;
    }
}

static inline int
size_compare(struct image_thumb *a, struct image_thumb *b)
{
  uns a_cols = a->thumb_cols, a_rows = a->thumb_rows;
  uns b_cols = b->thumb_cols, b_rows = b->thumb_rows;
  normalize_image_size(&a_cols, &a_rows);
  normalize_image_size(&b_cols, &b_rows);
  COMPARE(a_cols, b_cols);
  COMPARE(a_rows, b_rows);
  a_cols = a->cols, a_rows = a->rows;
  b_cols = b->cols, b_rows = b->rows;
  normalize_image_size(&a_cols, &a_rows);
  normalize_image_size(&b_cols, &b_rows);
  COMPARE(a->cols, b->cols);
  COMPARE(a->rows, b->rows);
  return 0;
}

#include "ucw/sorter/sorter.h"

static void
split_by_size(struct fastbuf *in_thumbs, struct stats *stats)
{
  XTRACE(1, "Splitting images by size");
  brewind(in_thumbs);
  in_thumbs = size_sort(in_thumbs, NULL);
  struct image_thumb hdr;
  uns last_cols = ~0U, last_rows = ~0U, last_thumb_cols = ~0U, last_thumb_rows = ~0U, group_cnt = 0, trivial_cnt = 0, group_max = 0;
  char name_buf[64];
  struct group g;
  while (breadb(in_thumbs, &hdr, sizeof(hdr)))
    {
      if (last_thumb_cols != hdr.thumb_cols || last_thumb_rows != hdr.thumb_rows || last_cols != hdr.cols || last_rows != hdr.rows)
        {
	  if (~last_cols)
	    {
	      if (g.count > 1)
		group_cnt++;
	      else if (g.count == 1)
		trivial_cnt++;
	      group_max = MAX(g.count, group_max);
	      group_flush(&g);
	    }
	  last_thumb_cols = hdr.thumb_cols;
	  last_thumb_rows = hdr.thumb_rows;
	  sprintf(name_buf, "%ux%u", last_cols = hdr.cols, last_rows = hdr.rows);
	  group_start(&g, name_buf, image_dup_nres_flags, 1, stats);
	}
      group_add(&g, &hdr, in_thumbs);
    }
  if (~last_cols)
    {
      if (g.count > 1)
	group_cnt++;
      else if (g.count == 1)
	trivial_cnt++;
      group_max = MAX(g.count, group_max);
      group_flush(&g);
    }
  bclose(in_thumbs);
  sync_contexts(stats);
  XTRACE(1, "Processed %u trivial and %u non-trivial classes of non-resizeable images (maximum size %u)", trivial_cnt, group_cnt, group_max);
}

static inline uns
is_resizeable(struct image_thumb *t)
{
  return
    image_dup_res_support &&
    t->cols >= image_dup_res_min_cols &&
    t->rows >= image_dup_res_min_rows &&
    t->thumb_cols >= image_dup_res_min_thumb_cols &&
    t->thumb_rows >= image_dup_res_min_thumb_rows;
}

static void
split(void)
{
  struct fastbuf *thumbs = index_bopen(fn_image_thumbnails, O_RDONLY, 1);
  XTRACE(1, "Preprocessing %s of thumbnails", stk_fsize(bfilesize(thumbs)));

  uns large_cnt = 0, dup_cnt = 0;
  uns res_cnt = 0, nres_cnt = 0, res_compr_max = 0, nres_compr_max = 0;
  uns res_size_max = 0, nres_size_max = 0, all_cnt, cls;
  u64 res_compr = 0, nres_compr = 0, res_size = 0, nres_size = 0;
  struct image_thumb hdr;

  struct stats stats, res_stats, nres_stats;
  init_stats(&stats);
  init_stats(&res_stats);
  init_stats(&nres_stats);

  struct group g;
  group_start(&g, "resizeable", image_dup_res_flags, 0, &res_stats);
  struct fastbuf *nres_thumbs = index_bopen_tmp(1);

  bitarray_t cls_found = big_alloc_zero(BIT_ARRAY_BYTES(card_count));
  while (breadb(thumbs, &hdr, sizeof(hdr)))
    {
      uns resizeable = is_resizeable(&hdr);
      uns size = estimate_size(&hdr, same_size_compare(resizeable));
      if (size > image_dup_max_image_size)
        {
	  large_cnt++;
	  bskip(thumbs, hdr.thumb_size);
	}
      else if (bit_array_test_and_set(cls_found, cls = merges_find_root(hdr.id)))
        {
	  dup_cnt++;
	  bskip(thumbs, hdr.thumb_size);
	}
      else if (resizeable)
        {
	  group_add(&g, &hdr, thumbs);
	  res_cnt++;
	  res_size += size;
	  res_size_max = MAX(res_size_max, size);
	  size = sizeof(hdr) + hdr.thumb_size;
	  res_compr += size;
	  res_compr_max = MAX(res_compr_max, size);
	}
      else
        {
	  nres_cnt++;
	  bwrite(nres_thumbs, &hdr, sizeof(hdr));
	  bbcopy(thumbs, nres_thumbs, hdr.thumb_size);
	  nres_size += size;
	  nres_size_max = MAX(nres_size_max, size);
	  size = sizeof(hdr) + hdr.thumb_size;
	  nres_compr += size;
	  nres_compr_max = MAX(nres_compr_max, size);
	}
    }
  big_free(cls_found, BIT_ARRAY_BYTES(card_count));
  all_cnt = res_cnt + nres_cnt;
  bclose(thumbs);

  XTRACE(1, "Skipped %u already known duplicates and %u too large images", dup_cnt, large_cnt);
  XTRACE(1, "Found %u resizeable and %u non-resizeable images", res_cnt, nres_cnt);

  if (!nres_cnt)
    bclose(nres_thumbs);
  if (!res_cnt)
    group_flush(&g);
  if (!all_cnt)
    return;

  init_contexts();
  if (res_cnt)
    {
      XTRACE(1, "Processing resizeable thumbnails (compressed %s, avg %s, max %s; memory %s, avg %s, max %s)",
	stk_fsize(res_compr), stk_fsize(res_compr / res_cnt), stk_fsize(res_compr_max),
	stk_fsize(res_size), stk_fsize(res_size / res_cnt), stk_fsize(res_size_max));
      group_flush(&g);
      sync_contexts(&res_stats);
      merge_stats(&stats, &res_stats);
    }
  if (nres_cnt)
    {
      XTRACE(1, "Processing non-resizeable thumbnails (compressed %s, avg %s, max %s; memory %s, avg %s, max %s)",
	stk_fsize(nres_compr), stk_fsize(nres_compr / nres_cnt), stk_fsize(nres_compr_max),
	stk_fsize(nres_size), stk_fsize(nres_size / nres_cnt), stk_fsize(nres_size_max));
      split_by_size(nres_thumbs, &nres_stats);
      merge_stats(&stats, &nres_stats);
    }
  cleanup_contexts();

  XTRACE(1, "Found %u new duplicates (%.1f%%)", stats.dups, 100.0 * stats.dups / all_cnt);
  XTRACE(1, "Avg per image comparisons: vectors=%.1f, pixmaps=%.1f, levels=%.1f, pixels=%.1f",
      (double)stats.cmp_pairs / all_cnt, (double)stats.cmp_pixmaps / all_cnt,
      (double)stats.cmp_levels / all_cnt, (double)stats.cmp_pixels / all_cnt);
  XTRACE(1, "Avg Kd-Tree tests: down=%.1f, up=%.1f", (double)stats.kdtree_down / all_cnt, (double)stats.kdtree_up / all_cnt);

}

/*** The entry point ***/

int
main(int argc, char **argv)
{
  cf_declare_section("MergeImages", &merge_images_config, 0);
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
    {
      fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
      exit(1);
    }

  finish_config();
  merges_map(1);
  split();
  merges_unmap();

  return 0;
}
