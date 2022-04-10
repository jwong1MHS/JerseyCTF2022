/*
 *	Sherlock Indexer -- Clusterization of images by signatures
 *
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/mempool.h"
#include "indexer/indexer.h"
#include "images/images.h"
#include "images/signature.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

struct node {
  u32 oid;
  union {
    s32 dot_prod;
    u32 index;
  };
  struct image_vector vector;
  byte len;
};

static uns count, depth, max_oid;
static struct node *nodes, *nodes_end;
static struct node **pnodes, **pnodes_end;
static u32 *oid_cmp;

static uns
vectors_read(void)
{
  struct fastbuf *fb = index_bopen(fn_image_signatures_unsorted, O_RDONLY, 1);
  struct image_signature sig;
  count = bgetl(fb);
  if (!count)
    {
      /* No signatures, create empty files */
      log(L_INFO, "There is no image signature in the index");
      bclose(fb);
      fb = index_bopen(fn_image_signatures, O_WRONLY | O_CREAT | O_TRUNC, 1);
      bclose(fb);
      fb = index_bopen(fn_image_clusters, O_WRONLY | O_CREAT | O_TRUNC, 1);
      bputl(fb, 0);
      bclose(fb);
      return 0;
    }
  log(L_INFO, "Reading %u image signatures", count);
  nodes_end = nodes = big_alloc(count * sizeof(*nodes));
  pnodes_end = pnodes = big_alloc(count * sizeof(*pnodes));
  for (uns i = 0; i < count; i++, nodes_end++, pnodes_end++)
    {
      nodes_end->oid = bgetl(fb);
      max_oid = MAX(max_oid, nodes_end->oid);
      nodes_end->len = bpeekc(fb);
      breadb(fb, &sig, image_signature_size(nodes_end->len));
      nodes_end->vector = sig.vec;
      *pnodes_end = nodes_end;
    }
  bclose(fb);
  return 1;
}

#define ASORT_PREFIX(x) vectors_clusterize_##x
#define ASORT_KEY_TYPE struct node *
#define ASORT_LT(x,y) ((x)->dot_prod < (y)->dot_prod)
#include "ucw/sorter/array.h"

static void
vectors_clusterize(void)
{
  while ((image_sig_max_cluster_count << depth) < count)
    depth++;
  depth++;
  log(L_INFO, "Creating clusters tree of depth %u, average number of signatures per cluster is %u", depth, count >> (depth - 1));
  struct image_cluster *clusters = big_alloc_zero(sizeof(*clusters) << depth), *clus;

  /* Initialize recursion */
  u64 clus_pos = 0;
  uns clus_index = 0;
  struct stk {
    uns count;
    uns index;
    struct node **start;
  } stk_top[64], *stk = stk_top + 1;
  stk->start = pnodes;
  stk->count = count;
  stk->index = 1;

  /* Main loop */
  while (stk != stk_top)
    {
      DBG("recursion step... start=%u count=%u index=%u", stk->start - pnodes, stk->count, stk->index);
      clus = clusters + stk->index - 1;

      /* BSP leaf node */
      if (stk->index >= (uns)(1 << (depth - 1)))
        {
	  clus->pos = clus_pos;
	  for (uns i = 0; i < stk->count; i++)
	    {
	      clus_pos += 4 + image_signature_size(stk->start[i]->len);
	      stk->start[i]->index = clus_index;
	    }
	  clus_index++;
	  stk--;
        }

      /* BSP internal node */
      else
        {
          /* Generate normal vector of a random splitting plane */
          int normal[IMAGE_VEC_F];
	  uns zero;
	  do
	    {
	      zero = 0;
              for (uns i = 0; i < IMAGE_VEC_F; i++)
                zero |= (clus->vec[i] = normal[i] = random_max(255) - 127);
	    }
	  while (!zero);

	  /* Compute dot produts */
	  for (uns i = 0; i < stk->count; i++)
	    {
	      stk->start[i]->dot_prod = 0;
	      for (uns j = 0; j < IMAGE_VEC_F; j++)
                stk->start[i]->dot_prod += normal[j] * stk->start[i]->vector.f[j];
            }

	  /* Sort */
	  vectors_clusterize_sort(stk->start, stk->count);

	  /* Split in the middle */
	  stk[1].index = stk[0].index << 1;
	  stk[0].index = stk[1].index + 1;
	  stk[1].count = stk[0].count >> 1;
	  stk[0].count -= stk[1].count;
	  stk[1].start = stk[0].start;
	  stk[0].start += stk[1].count;

	  /* Choose split value */
	  clus->dot = (stk->start[-1]->dot_prod + stk->start[0]->dot_prod) / 2;

	  /* Continue with recursion */
	  stk++;
	}
    }

  clusters[(1 << depth) - 1].pos = clus_pos;

  /* Write clusters */
  struct fastbuf *fb = index_bopen(fn_image_clusters, O_WRONLY | O_CREAT | O_TRUNC, 1);
  bputl(fb, depth);
  bwrite(fb, clusters, sizeof(*clusters) << depth);
  bclose(fb);
  big_free(clusters, sizeof(*clusters) << depth);
  big_free(pnodes, count * sizeof(*pnodes));

  /* Prepare comparision array for external sort */
  oid_cmp = big_alloc(sizeof(*oid_cmp) * (max_oid + 1));
  for (struct node *n = nodes; n != nodes_end; n++)
    oid_cmp[n->oid] = n->index;

  big_free(nodes, count * sizeof(*nodes));
}

struct key {
  byte oid[4];
  byte len;
} PACKED;

#define SORT_PREFIX(x) clusters_sort_##x
#define SORT_KEY_REGULAR struct key
#define SORT_DATA_SIZE(k) (image_signature_size((k).len) - 1)
#define SORT_PRESORT
#define SORT_UNIQUE
#define SORT_INPUT_FB
#define SORT_OUTPUT_FILE

static int
clusters_sort_compare(struct key *x, struct key *y)
{
  uns xi = get_u32(x->oid);
  uns yi = get_u32(y->oid);
  uns xc = oid_cmp[xi];
  uns yc = oid_cmp[yi];
  COMPARE(xc, yc);
  COMPARE(xi, yi);
  return 0;
}

#include "ucw/sorter/sorter.h"

static void
build_clusters(void)
{
  if (vectors_read())
    {
      vectors_clusterize();
      log(L_INFO, "Sorting signatures");
      struct fastbuf *fb = index_bopen(fn_image_signatures_unsorted, O_RDONLY, 1);
      bskip(fb, 4);
      clusters_sort_sort(fb, index_name(fn_image_signatures));
      big_free(oid_cmp, sizeof(*oid_cmp) * (max_oid + 1));
      log(L_INFO, "Clusters built");
    }
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

  /*in_fb = index_bopen(fn_image_signatures, O_RDONLY);
  out_fb = index_bopen("image-clusters", O_WRONLY | O_CREAT | O_TRUNC);*/
  build_clusters();
  //bclose(out_fb);

  return 0;
}

