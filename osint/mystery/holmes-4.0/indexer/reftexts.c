/*
 *	Sherlock Indexer -- Processing of Reference Texts
 *
 *	(c) 2002--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2004--2007 Robert Spalek <robert@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/heap.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "ucw/url.h"
#include "ucw/bbuf.h"
#include "ucw/sorter/common.h"
#include "sherlock/object.h"
#include "sherlock/math.h"
#include "charset/unicat.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"
#include "indexer/graph.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

/*
 * XXX: due to gradual merging with cropping in many passes it may happen that
 * one doesn't get the same results with different buffer sizes, as different
 * records are dropped in the meantime.  however, most likely only small
 * fluctuations occur between reftexts of very similar total weight, and the
 * most relevant ones are selected with high probability.  the impact of this
 * can be minimized by defining STABLE_SELECTION that doubles the merging limit
 * and sorts on other criteria after weight.
 *
 * please note that much more significant changes in the index are caused by
 * the randomization in merging similar documents.
 */
#undef STABLE_SELECTION

/* Reading the original reftexts file */

struct rt_source {
  u32 src;
  u32 hash;
  ucw_off_t pos;
};

struct rt_fp {			// input to the resolver
  struct fingerprint fp;
  struct rt_source rt;
} PACKED;

static uns
rt_hashf(byte *x)
{
  uns h = 0;
  uns u;
  uns lastsp = 1;

  for(;;)
    {
      x = utf8_get(x, &u);
      if (!u)
	break;
      if (Ualnum(u))
	{
	  if (lastsp)
	    h = 67*h + ' ';
	  h = 67*h + Utoupper(u);
	  lastsp = 0;
	}
      else
	lastsp = 1;
    }
  return h;
}

static void
analyse_reftexts(struct fastbuf ***Fb)
{
  uns bits = resolve_start(RESOLVE_SKIP_UNKNOWN | RESOLVE_SKIP_SKEL, sizeof(struct rt_fp) - sizeof(struct fingerprint));
  uns shift = 32 - bits, mask = (1 << bits) - 1;
  struct fastbuf **fb = xmalloc((mask + 1) * sizeof(struct fastbuf *));
  struct fastbuf *ref = index_bopen(fn_ref_texts, O_RDONLY, 1);
  u64 count = 0;
  for (uns i=0; i<=mask; i++)
    fb[i] = bopen_tmp_file(&sorter_fb_params);
  struct rt_fp node1;
  bb_t bb;
  bb_init(&bb);
  while (node1.rt.pos = btell(ref), (node1.rt.src = bgetl(ref)) != (u32)~0U)
  {
    ASSERT(!(node1.rt.src & ETYPE_IMAGE));
    breadb(ref, &node1.fp, sizeof(struct fingerprint));
    uns l = bgetw(ref);
    bb_grow(&bb, l+1);
    breadb(ref, bb.ptr, l);
    bb.ptr[l] = 0;
    node1.rt.hash = rt_hashf(bb.ptr);
    uns out_stream = (fp_hash(&node1.fp) >> shift) & mask;
    bwrite(fb[out_stream], &node1, sizeof(struct rt_fp));
    count++;
  }
  for (uns i=0; i<=mask; i++)
    brewind(fb[i]);
  bclose(ref);
  bb_done(&bb);
  log(L_INFO, "Analyzed %llu reftexts, exported to %d streams", (long long) count, mask + 1);
  *Fb = fb;
}

/* Pruning unwanted links */

struct rt_id {			// output from the resolver
  u32 dest;
  struct rt_source rt;
} PACKED;

struct rt_link {		// records enriched by weights
  u32 dest;
  struct rt_source src;
  float wt;
  u16 cnt;
  byte src_wt;
} PACKED;

static struct fastbuf *
prune_references(struct fastbuf *ids2)
{
  attrs_part_map(0);
  byte *card_weights;
  READ_ATTR(card_weights, weight);
#ifdef CONFIG_SITES
  u32 *card_sites;
  READ_ATTR(card_sites, site_id);
#endif
#ifdef CONFIG_AREAS
  u32 *card_areas;
  READ_ATTR(card_areas, area);
#endif
  attrs_part_unmap();

  struct fastbuf *ids3 = index_bopen_tmp(1);
  struct rt_id node2;
  while (breadb(ids2, &node2, sizeof(struct rt_id)))
  {
    uns src = node2.rt.src, dest = node2.dest;
    ASSERT(dest < FIRST_ID_SKEL);
    if (merges[src] != (u32)~0U && merges[dest] != (u32)~0U			/* src/dest card not dropped */
	&& merges[merges[src]] != (u32)~0U && merges[merges[dest]] != (u32)~0U	/* final src/dest card is nonempty */
	&& merges[merges[src]] != merges[merges[dest]]				/* not a self-link */
#ifdef CONFIG_AREAS
	&& card_areas[src] == card_areas[dest]					/* in the same area */
#endif
       )
    {
      struct rt_link link;
      link.dest = node2.dest;
      link.src = node2.rt;
      link.src_wt = card_weights[node2.rt.src];
      link.cnt = 1;
      link.wt = expf(link.src_wt * M_LN2 / 8.);
#ifdef CONFIG_SITES
      uns dest = merges[merges[node2.dest]];
      if (card_sites[node2.rt.src] != card_sites[dest])
	link.wt *= 8;
#endif
      bwrite(ids3, &link, sizeof(link));
    }
  }
  u64 pruning_in = btell(ids2) / sizeof(struct rt_id);
  u64 pruning_out = btell(ids3) / sizeof(struct rt_link);
  bclose(ids2);
  brewind(ids3);
  FREE_ATTR(card_weights);
#ifdef CONFIG_SITES
  FREE_ATTR(card_sites);
#endif
#ifdef CONFIG_AREAS
  FREE_ATTR(card_areas);
#endif
  log(L_INFO, "Taken %llu from %llu reftexts", (long long) pruning_out, (long long) pruning_in);
  return ids3;
}

/* Presorting */

#define ASORT_PREFIX(x) dest_##x
#define ASORT_KEY_TYPE struct rt_link
#define ASORT_LT(x,y) (merges[merges[x.dest]] < merges[merges[y.dest]])
#include "ucw/sorter/array.h"

#define ASORT_PREFIX(x)	link_hash_##x
#define ASORT_KEY_TYPE	struct rt_link
#define ASORT_LT(x,y)	x.src.hash < y.src.hash
#include "ucw/sorter/array.h"

#define ASORT_PREFIX(x)	link_wt_##x
#define ASORT_KEY_TYPE	struct rt_link
#define ASORT_LT(x,y)	x.wt > y.wt
#include "ucw/sorter/array.h"

static inline int
compare_links(struct rt_link *a, struct rt_link *b)
{
  COMPARE(a->src_wt, b->src_wt);
#ifdef STABLE_SELECTION
  COMPARE(a->src.src, b->src.src);
  COMPARE(a->src.pos, b->src.pos);
  ASSERT(0);
#endif
  return 0;
}

static uns
sort_incoming_links(struct rt_link *links, uns count)
{
  link_hash_sort(links, count);		// sort by text hashes

  struct rt_link *tail = links;		// merge equal text hashes
  for (struct rt_link *head = links + 1; head < links + count; head++)
    if (head->src.hash == tail->src.hash)
    {
      tail->cnt += head->cnt;
      tail->wt += head->wt;
      if (compare_links(head, tail) > 0)
      {
	tail->src_wt = head->src_wt;
	tail->src = head->src;
	tail->dest = head->dest;
      }
    }
    else
      *++tail = *head;
  count = ++tail - links;
  link_wt_sort(links, count);		// sort by weight

  count = MIN(count, ref_max_count
#ifdef STABLE_SELECTION			// cut to the double of the final limit
      * 2
#endif
      );
  return count;
}

struct rt_hdr {			// header of a chain
  u32 merged_dest;
  byte num;
} PACKED;

/* Sorting by the destination and merging */

#define SORT_PREFIX(x) rt_wt_##x
#define SORT_KEY_REGULAR struct rt_hdr
#define SORT_DATA_SIZE(x) ((x).num * sizeof(struct rt_link))
#define SORT_INT(x) ((x).merged_dest)
#define SORT_UNIFY
#define SORT_INPUT_PRESORT
#define SORT_OUTPUT_FB

static void
rt_wt_write_merged(struct fastbuf *f, struct rt_hdr **keys, void **data, uns n, void *Buf)
{
  uns i, total = 0;
  struct rt_link *buf = Buf;
  for (i=0; i<n; i++)
  {
    memcpy(buf+total, data[i], keys[i]->num * sizeof(struct rt_link));
    total += keys[i]->num;
  }
  keys[0]->num = sort_incoming_links(buf, total);
  bwrite(f, keys[0], sizeof(struct rt_hdr));
  bwrite(f, buf, keys[0]->num * sizeof(struct rt_link));
}

#define	GBUF_TYPE	struct rt_link
#define	GBUF_PREFIX(x)	rtlb_##x
#include "ucw/gbuf.h"

static void
rt_wt_copy_merged(struct rt_hdr **keys, struct fastbuf **data, uns n, struct fastbuf *dest)
{
  uns i, total = 0;
  static rtlb_t bb;
  for (i=0; i<n; i++)
    total += keys[i]->num;
  rtlb_grow(&bb, total);
  total = 0;
  for (i=0; i<n; i++)
  {
    breadb(data[i], bb.ptr + total, keys[i]->num * sizeof(struct rt_link));
    total += keys[i]->num;
  }
  keys[0]->num = sort_incoming_links(bb.ptr, total);
  bwrite(dest, keys[0], sizeof(struct rt_hdr));
  bwrite(dest, bb.ptr, keys[0]->num * sizeof(struct rt_link));
}

static struct fastbuf *presort_input;

static int
rt_wt_presort(struct fastbuf *destf, void *Buf, size_t bufsize)
{
  uns record = sizeof(struct rt_link);
  struct rt_link *buf = Buf;
  uns nr = bread(presort_input, buf, bufsize / record * record);
  if (!nr)
  {
    merges_unmap();	//free the memory ASAP
    bclose(presort_input);
    return 0;
  }
  ASSERT(!(nr % record));
  nr /= record;

  dest_sort(buf, nr);
  for (uns i=0; i<nr; )
  {
    uns start = i++;
    uns dest = merges[merges[ buf[start].dest ]];
    for (; i<nr && merges[merges[ buf[i].dest ]] == dest; i++);	// find the neighbors
    uns count = sort_incoming_links(buf + start, i - start);
    struct rt_hdr hdr = { .merged_dest = dest, .num = count };
    bwrite(destf, &hdr, sizeof(hdr));
    bwrite(destf, buf + start, count * sizeof(struct rt_link));
  }
  return 1;
}

#include "ucw/sorter/sorter.h"

/* Sorting by file position */

struct rt_best {		// merged records rewritten into this
  u32 dest;
  ucw_off_t pos;
  u16 count;
  byte weight;
} PACKED;

struct rt_url_req {
  u32 src;
  ucw_off_t pos;
};

static ucw_off_t max_pos;

static struct fastbuf *
take_best(struct fastbuf *fb, struct fastbuf **ureqsp)
{
  struct fastbuf *out = index_bopen_tmp(1);
  struct fastbuf *ureqs = index_bopen_tmp(1);
  struct rt_hdr n1;
  uns count = 0;
  max_pos = 0;
  while (breadb(fb, &n1, sizeof(struct rt_hdr)))
  {
    struct rt_link links[n1.num];
    breadb(fb, links, n1.num * sizeof(struct rt_link));
#ifdef STABLE_SELECTION
    n1.num = MIN(n1.num, ref_max_count);
#endif
    for (uns i=0; i<n1.num; i++)
    {
      struct rt_best n2;
      n2.dest = links[i].dest;
      n2.pos = links[i].src.pos;
      n2.count = links[i].cnt;
      float x = logf(links[i].wt) / M_LN2 * 8.;
      n2.weight = CLAMP(x/2, 0, 255);		// divide by 2 to fit into byte
      bwrite(out, &n2, sizeof(struct rt_best));

      struct rt_url_req ur = {
	.src = links[i].src.src,
	.pos = links[i].src.pos
      };
      bwrite(ureqs, &ur, sizeof(ur));
      max_pos = MAX(max_pos, links[i].src.pos);
    }
    count += n1.num;
  }
  bclose(fb);
  brewind(out);
  brewind(ureqs);
  *ureqsp = ureqs;
  log(L_INFO, "Selected %u best reftexts", count);
  return out;
}

#define SORT_PREFIX(x) rt_best_##x
#define SORT_KEY_REGULAR struct rt_best
#define SORT_INT64(x) ((x).pos)
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#include "ucw/sorter/sorter.h"

/* Resolving URL's */

#define SORT_PREFIX(x) rt_url_req_##x
#define SORT_KEY_REGULAR struct rt_url_req
#define SORT_INT(x) ((x).src)
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#include "ucw/sorter/sorter.h"

struct rt_url {
  ucw_off_t pos;
  u16 len;
  byte url[0];
} PACKED;

#define SORT_PREFIX(x) rt_url_##x
#define SORT_KEY_REGULAR struct rt_url
#define SORT_INT64(x) ((x).pos)
#define SORT_DATA_SIZE(x) ((x).len)
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#include "ucw/sorter/sorter.h"

static struct fastbuf *
select_urls(struct fastbuf *reqs)
{
  log(L_INFO, "Resolving URL's");
  reqs = rt_url_req_sort(reqs, NULL, card_count);

  ITRACE("Walking URL list");
  struct fastbuf *url_list = index_bopen(fn_urls, O_RDONLY, 1);
  struct fastbuf *urls = index_bopen_tmp(1);
  struct rt_url_req req;
  byte url[MAX_URL_SIZE];
  bgets(url_list, url, sizeof(url));
  uns url_idx = 0;
  uns url_len = strlen(url) + 1;
  while (bread(reqs, &req, sizeof(req)))
    {
      while (url_idx < req.src)
	{
	  byte *u = bgets(url_list, url, sizeof(url));
	  ASSERT(u);
	  url_len = u - url + 1;
	  url_idx++;
	}
      struct rt_url ru;
      ru.pos = req.pos;
      ru.len = url_len;
      bwrite(urls, &ru, sizeof(ru));
      bwrite(urls, url, url_len);
    }
  bclose(url_list);
  bclose(reqs);
  brewind(urls);

  ITRACE("Sorting URL's");
  urls = rt_url_sort(urls, NULL, max_pos);
  return urls;
}

/* Dumping the reference texts */

static void
dump_reftexts(struct fastbuf *rts, struct fastbuf *urls)
{
  struct fastbuf *ref = index_bopen(fn_ref_texts, O_RDONLY, 0);
  struct fastbuf *labels = index_bopen(fn_labels_by_id, O_WRONLY | O_APPEND, 0);
  put_attr_set_type(BUCKET_TYPE_V33);
  struct rt_best rt3;
  uns dumped = 0;
  bb_t bb;
  bb_init(&bb);
  while (breadb(rts, &rt3, sizeof(struct rt_best)))
  {
    struct rt_url ru;
    byte url[MAX_URL_SIZE];
    int ru_ok = breadb(urls, &ru, sizeof(ru));
    ASSERT(ru_ok && rt3.pos == ru.pos);
    breadb(urls, url, ru.len);

    uns cardid, len;
    bsetpos(ref, rt3.pos);
    cardid = bgetl(ref);
    ASSERT(cardid != ~0U);
    bskip(ref, sizeof(struct fingerprint));
    len = bgetw(ref);
    bb_grow(&bb, len+1);
    bread(ref, bb.ptr, len);
    bb.ptr[len] = 0;
    bputl(labels, rt3.dest);
    bputc(labels, LABEL_TYPE_BODY);
    bput_attr_format(labels, 'x', "%s %d %d %s", url, rt3.weight*2, rt3.count, bb.ptr);
    bput_attr_separator(labels);
    dumped++;
  }
  bb_done(&bb);
  bclose(ref);
  bclose(urls);
  bclose(labels);
  bclose(rts);
  log(L_INFO, "Dumped %u reftexts", dumped);
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
  ASSERT(ref_max_count < 128);

  struct fastbuf *fb, **fbs, *urls;
  analyse_reftexts(&fbs);
  fb = resolve_finish(fbs);
  xfree(fbs);
  merges_map(0);	//merges unmapped and fastbuf closed in rt_wt_presort()
  presort_input = prune_references(fb);
  fb = rt_wt_sort(NULL, NULL, card_count);
  fb = take_best(fb, &urls);
  fb = rt_best_sort(fb, NULL, max_pos);
  urls = select_urls(urls);
  dump_reftexts(fb, urls);

  return 0;
}
