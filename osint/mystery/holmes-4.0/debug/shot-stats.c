/*
 *	Simple statistics for planning of screenshots
 *
 *	(c) 2007 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/conf.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "sherlock/index.h"
#include "sherlock/object.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct rec {
  struct fingerprint fp;
  u32 count;
};

#define SORT_PREFIX(x) fp_unify_##x
#define SORT_KEY_REGULAR struct rec
#define SORT_UNIFY
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

static inline int
fp_unify_compare(struct rec *x, struct rec *y)
{
  return memcmp(&x->fp, &y->fp, sizeof(struct fingerprint));
}

static void
fp_unify_write_merged(struct fastbuf *f, struct rec **keys, void **data UNUSED, uns n, void *buf UNUSED)
{
  struct rec *r = keys[0];
  for (uns i=1; i<n; i++)
    r->count += keys[i]->count;
  bwrite(f, r, sizeof(*r));
}

#include "ucw/sorter/sorter.h"

#define SORT_PREFIX(x) fp_freq_##x
#define SORT_KEY_REGULAR struct rec
#define SORT_INT(k) (k).count
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

#include "ucw/sorter/sorter.h"

static void
make_fp(char *outfile)
{
  byte buf[4096];
  struct rec r;
  struct fastbuf *in = bopen_fd(0, NULL);
  struct fastbuf *tmp = bopen_tmp_file(NULL);
  u64 cnt = 0;
  while (bgets(in, buf, sizeof(buf)))
    {
      if (buf[0] != '\t')
        continue;
      if (strchr(buf+1, ' ') || strchr(buf+1, '\t'))
        continue;
      fingerprint(buf+1, &r.fp);
      r.count = 1;
      bwrite(tmp, &r, sizeof(r));
      cnt++;
    }
  bclose(in);
  brewind(tmp);
  log(L_INFO, "%Ld URL's parsed", (long long)cnt);

  tmp = fp_unify_sort(tmp, NULL);
  log(L_INFO, "%Ld FP's after unification", (long long)(bfilesize(tmp) / sizeof(struct rec)));
  bfix_tmp_file(tmp, outfile);
}

static void
sort_freq(char *infile, char *outfile)
{
  struct fastbuf *f = bopen_file(infile, O_RDONLY, NULL);
  f = fp_freq_sort(f, NULL, ~0U);
  bfix_tmp_file(f, outfile);
}

static void
stats(char *in)
{
  struct fastbuf *f = bopen_file(in, O_RDONLY, NULL);
  u64 rec = 0;
  u64 total = 0;
  u32 max = 0;
  struct rec r;
  while (breadb(f, &r, sizeof(r)))
    {
      rec++;
      total += r.count;
      max = MAX(max, r.count);
    }
  bclose(f);
  log(L_INFO, "%Ld FP's (%Ld occurences, max %d)", (long long)rec, (long long)total, max);
}

static void
cards(char *out)
{
  struct mempool *mp = mp_new(65536);
  struct fastbuf *i = bopen_fd(0, NULL);
  struct fastbuf *tmp = bopen_tmp_file(NULL);
  uns ccnt = 0;
  for (;;)
    {
      mp_flush(mp);
      struct odes *o = obj_new(mp);
      if (!obj_read(i, o))
        break;
      ccnt++;
      uns wt = 0;
      for (struct oattr *a = obj_find_attr(o, 'U' + OBJ_ATTR_SON); a; a=a->same)
        {
          for (struct oattr *b = obj_find_attr(a->son, 'W'); b; b=b->same)
	    if (b->val[0] == 'u')
	      {
		uns w = atol(b->val+1);
		wt = MAX(wt, w);
	      }
	}
      uns cc = 0;
      for (struct oattr *a = obj_find_attr(o, 'U' + OBJ_ATTR_SON); a; a=a->same)
        {
          byte *url = obj_find_aval(a->son, 'U');
	  ASSERT(url);
	  struct rec r;
	  fingerprint(url, &r.fp);
	  r.count = wt + (cc++ ? 0x1000 : 0);
	  bwrite(tmp, &r, sizeof(r));
	}

    }
  log(L_INFO, "Extracted %Ld URL's from %d cards", (long long)(btell(tmp) / sizeof(struct rec)), ccnt);
  brewind(tmp);
  tmp = fp_unify_sort(tmp, NULL);
  bfix_tmp_file(tmp, out);
}

static void
merge(char *in1, char *in2, char *out, uns mode)
{
  struct fastbuf *f1 = bopen_file(in1, O_RDONLY, NULL);
  struct fastbuf *f2 = bopen_file(in2, O_RDONLY, NULL);
  struct fastbuf *o = bopen_file(out, O_WRONLY | O_CREAT | O_TRUNC, NULL);
  struct rec r1, r2, *r;
  int n1 = breadb(f1, &r1, sizeof(r1));
  int n2 = breadb(f2, &r2, sizeof(r2));
  u64 total = 0;
  while (n1 || n2)
    {
      int cmp;
      if (!n1)
        cmp = 1;
      else if (!n2)
        cmp = -1;
      else
	cmp = memcmp(&r1.fp, &r2.fp, sizeof(struct fingerprint));
      switch (mode)
        {
	case 0:
	  if (!cmp)
	    r1.count = MAX(r1.count, r2.count);
	  r = (cmp <= 0) ? &r1 : &r2;
	  bwrite(o, r, sizeof(*r));
	  total += r->count;
	  break;
	case 1:
	  if (!cmp)
	    {
	      r1.count = MIN(r1.count, r2.count);
	      bwrite(o, &r1, sizeof(r1));
	      total += r1.count;
	    }
	  break;
	}
      if (cmp <= 0)
        n1 = breadb(f1, &r1, sizeof(r1));
      if (cmp >= 0)
        n2 = breadb(f2, &r2, sizeof(r2));
    }
  bclose(f1);
  bclose(f2);
  log(L_INFO, "%Ld FP's (%Ld occurences)", (long long)(btell(o) / sizeof(struct rec)), (long long)total);
  bclose(o);
}

static void
cardsel(char *in, char *out, char *wts)
{
  struct fastbuf *i = bopen_file(in, O_RDONLY, NULL);
  struct fastbuf *o = bopen_file(out, O_WRONLY | O_CREAT | O_TRUNC, NULL);
  uns min_wt = atol(wts);
  struct rec r;
  uns ccnt = 0;

  while (breadb(i, &r, sizeof(r)))
    {
      uns wt = r.count & 0xff;
      if (wt >= min_wt)
        {
	  if (r.count & 0x1000)
	    ccnt++;
	  bwrite(o, &r, sizeof(r));
	}
    }

  log(L_INFO, "Selected %Ld URL's from %d cards", (long long)(btell(o) / sizeof(struct rec)), ccnt);
  bclose(i);
  bclose(o);
}

struct fph {
  byte fp[sizeof(struct fingerprint)];
  u32 wt;
};

#define HASH_NODE struct fph
#define HASH_PREFIX(x) fph_##x
#define HASH_KEY_MEMORY fp
#define HASH_KEY_SIZE sizeof(struct fingerprint)
#define HASH_WANT_FIND
#define HASH_WANT_NEW
#define HASH_AUTO_POOL 65536
#include "ucw/hashtable.h"

static void
fph_read(char *in)
{
  struct fastbuf *i = bopen_file(in, O_RDONLY, NULL);
  struct rec r;
  fph_init();
  u64 cnt = 0;
  while (breadb(i, &r, sizeof(r)))
    {
      struct fph *h = fph_new(r.fp.hash);
      h->wt = r.count;
      cnt++;
    }
  log(L_INFO, "Read %Ld FP's", (long long)cnt);
  bclose(i);
}

static void
hits(char *in)
{
  fph_read(in);

  struct fastbuf *i = bopen_fd(0, NULL);
  byte buf[4096];
  uns totals[20], stats[20], hits[10+1];
  bzero(stats, sizeof(stats));
  bzero(totals, sizeof(totals));
  bzero(hits, sizeof(hits));
  uns pos = 0;
  uns hit = 0;
  uns total_q = 0, total_in = 0, total_hits = 0, total_sec = 0;
  while (bgets(i, buf, sizeof(buf)))
    {
      if (buf[0] != '\t')
        {
	  if (pos)
	    hits[(ARRAY_SIZE(hits)-1) * hit / pos]++;
          pos = hit = 0;
	  total_q++;
	}
      else
        {
	  struct fingerprint fp;
	  fingerprint(buf+1, &fp);
	  struct fph *h = fph_find(fp.hash);
	  if (h)
	    {
	      if (pos < ARRAY_SIZE(stats))
	        stats[pos]++;
	      hit++;
	      total_hits++;
	      if (h->wt & 0x1000)
	        total_sec++;
	    }
	  if (pos < ARRAY_SIZE(totals))
	    totals[pos]++;
	  pos++;
	  total_in++;
	}
    }
  bclose(i);

  log(L_INFO, "Processed %d queries", total_q);
  log(L_INFO, "Hit on %d URL's of %d (%d secondary)", total_hits, total_in, total_sec);
  for (uns i=0; i<ARRAY_SIZE(stats); i++)
    log(L_INFO, "Pos %2d: %9d of %9d (%5.1f%%)", i+1, stats[i], totals[i], (totals[i] ? 100.*stats[i]/totals[i] : 0.));
  for (uns i=0; i<ARRAY_SIZE(hits); i++)
    log(L_INFO, "Seen %3d%% hits in %9d cases (%5.1f%%)", 100*i/(int)(ARRAY_SIZE(hits)-1), hits[i], (total_q ? 100.*hits[i]/total_q : 0.));
}

static void NONRET
usage(void)
{
  fputs("\
Usage: shot-stats <command>\n\
\n\
Commands:\n\
  makefp <out>\t\tTranslate URL's to fingerprints\n\
  stats <in>\t\tPrint statistics on known FP's\n\
  sortfreq <in> <out>\tSort FP's by their frequencies\n\
  union <a> <b> <out>\tCalculate union of two FP lists\n\
  inter <a> <b> <out>\tCalculate intersection of two FP lists\n\
  cards <cout>\t\tExtract URL's from cards as dumped by `idxdump -c'\n\
  cardsel <cin> <out> <wt>\tSelect card URL's with weight at least <wt>\n\
  hits <in>\t\tCalculate search result hits by a given FP set\n\
", stderr);
  exit(1);
}

static const char shortopts[] = CF_SHORT_OPTS;

int
main(int argc, char **argv)
{
  int opt;

  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, shortopts, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      default:
	usage();
      }
  if (optind >= argc)
    usage();

  byte *cmd = argv[optind++];
  if (!strcmp(cmd, "makefp") && optind+1 == argc)
    make_fp(argv[optind]);
  else if (!strcmp(cmd, "sortfreq") && optind+2 == argc)
    sort_freq(argv[optind], argv[optind+1]);
  else if (!strcmp(cmd, "stats") && optind+1 == argc)
    stats(argv[optind]);
  else if (!strcmp(cmd, "union") && optind+3 == argc)
    merge(argv[optind], argv[optind+1], argv[optind+2], 0);
  else if (!strcmp(cmd, "inter") && optind+3 == argc)
    merge(argv[optind], argv[optind+1], argv[optind+2], 1);
  else if (!strcmp(cmd, "cards") && optind+1 == argc)
    cards(argv[optind]);
  else if (!strcmp(cmd, "cardsel") && optind+3 == argc)
    cardsel(argv[optind], argv[optind+1], argv[optind+2]);
  else if (!strcmp(cmd, "hits") && optind+1 == argc)
    hits(argv[optind]);
  else
    usage();

  return 0;
}
