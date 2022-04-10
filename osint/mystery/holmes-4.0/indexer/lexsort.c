/*
 *	Sherlock Indexer -- Final Lexicon Sorting and Optimization
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *
 *	This module sorts all words to an order convenient for the search server
 *	and removes all unreferenced words (they usually arise when generating
 *	subindices).
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/unaligned.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "ucw/bitarray.h"
#include "charset/unicat.h"
#include "indexer/indexer.h"
#include "indexer/lexicon.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

static struct lex_entry **word_array, **word_array_orig;
static uns n_words, n_cplx;	/* Here words include complexes */
static uns *ctxt_counter;
static uns optimize;

#define ASORT_PREFIX(x) word_##x
#define ASORT_KEY_TYPE struct lex_entry *
#define ASORT_LT(x,y) word_lt(x,y)

static int
word_lt(struct lex_entry *x, struct lex_entry *y)
{
#ifdef CONFIG_CONTEXTS
  if (x->class == WC_COMPLEX)
    {
      if (y->class == WC_COMPLEX)
	{
	  uns xt = GET_CONTEXT(&x->ctxt);
	  uns yt = GET_CONTEXT(&y->ctxt);
	  if (xt < yt)
	    return 1;
	  if (xt > yt)
	    return 0;
	  uns xi = GET_U16(x+1);	/* Trick: we store the order just after the lex_entry */
	  uns yi = GET_U16(y+1);
	  if (xi < yi)
	    return 1;
	  if (xi > yi)
	    return 0;
	  ASSERT(xi == yi);
	  return 0;
	}
      else
	return 0;
    }
  else if (y->class == WC_COMPLEX)
    return 1;
#endif

  byte *xx = x->w;
  byte *yy = y->w;
  byte *xend = xx + x->length;
  byte *yend = yy + y->length;
  uns xu, yu, xuu, yuu;
  int pass1 = -1, pass2 = -1;

  while (xx < xend && yy < yend)
    {
      xx = utf8_get(xx, &xu);
      yy = utf8_get(yy, &yu);
      xuu = Uunaccent(xu);
      yuu = Uunaccent(yu);
      if (pass1 < 0)
	{
	  if (xuu < yuu)
	    pass1 = 1;
	  else if (xuu > yuu)
	    pass1 = 0;
	}
      if (pass2 < 0)
	{
	  if (xu < yu)
	    pass2 = 1;
	  else if (xu > yu)
	    pass2 = 0;
	}
    }
  if (yy < yend)
    return 1;
  if (xx < xend)
    return 0;
  if (pass1 >= 0)
    return pass1;
  if (pass2 >= 0)
    return pass2;
  ASSERT(x == y);
  return 0;
}

#include "ucw/sorter/array.h"

static inline int
lex_referenced(struct lex_entry *e)
{
  return GET_U16(e->ch_len) || e->ref_pos[0];
}

static inline void
lex_mark(struct lex_entry *e)
{
  ASSERT(!GET_U16(e->ch_len));
  e->ref_pos[0] = 1;
}

/*** Accent Equivalences ***/

#define LH_LEXSORT
#define LH_NEED_CLEANUP
#include "indexer/lexhash.h"

static bitarray_t accent_word_class_refd;

static void
accent_scan(void)
{
  struct fastbuf *in = index_bopen(fn_lex_words, O_RDONLY, 0);
  n_words = bgetl(in);
  accent_word_class_refd = bit_array_xmalloc(n_words);	// We allocate this first to reduce address space fragmentation
  struct verbum **cls = big_alloc(sizeof(cls[0]) * n_words); // Maps word to its accent class
  lh_init();						// The lowest bit of verbum->id tells if the class is referenced
  log(L_INFO, "Scanning accent classes");
  for (uns i=0; i<n_words; i++)
    {
      struct lex_entry le;
      byte w[MAX_WORD_BYTES], uw[MAX_WORD_BYTES];
      breadb(in, &le, sizeof(le));
      if (le.class != WC_COMPLEX)
	{
	  breadb(in, w, le.length);
	  byte *up = uw;
	  uns ok = 1;
	  for (const byte *p=w; p<w+le.length; )
	    {
	      uns u;
	      p = utf8_get(p, &u);
	      if (Ualpha(u))
		up = utf8_put(up, Uunaccent(u));
	      else
		ok = 0;
	    }
	  if (ok)
	    {
	      struct verbum *v = lh_lookup_raw(uw, up);
	      cls[i] = v;
	      if (GET_U16(le.ch_len) || le.class != WC_NORMAL)
		v->id |= 1;
	    }
	}
    }
  bclose(in);
  ITRACE("Found %d accent classes", lh_id / 8);
  lh_cleanup();

  for (uns i=0; i<n_words; i++)
    bit_array_assign(accent_word_class_refd, i, cls[i] && (cls[i]->id & 1));

  big_free(cls, sizeof(cls[0]) * n_words);
  lh_cleanup_pool();
}

#ifdef CONFIG_LANG
static u32 *xlat_table;  /* Shares space with word_array */

static inline uns
lex_local_id(uns id)
{
  uns i = id/8 - 1;
  ASSERT(i < n_words);
  return i;
}

static inline uns
id_renumber(uns id)
{
  uns i = lex_local_id(id);
  ASSERT(xlat_table[i]);
  return xlat_table[i] | (id&7);
}

/*** Stem Tables ***/

struct stem_pair {
  u32 stem, derived;
};

static struct stem_pair *stem_array;

#define ASORT_PREFIX(x) stem_##x
#define ASORT_KEY_TYPE struct stem_pair
#define ASORT_LT(x,y) stem_lt(x,y)

static int
stem_lt(struct stem_pair x, struct stem_pair y)
{
  return x.stem < y.stem || x.stem == y.stem && x.derived < y.derived;
}

#include "ucw/sorter/array.h"

static void
stems_renumber(void)
{
  struct fastbuf *in = index_bopen(fn_stems_ordered, O_RDONLY, 0);
  struct fastbuf *out = index_bopen(fn_stems, O_WRONLY|O_CREAT|O_TRUNC, 0);
  uns i, j, stid, lama, x;
  while ((stid = bgetl(in)) != ~0U)
    {
      lama = bgetl(in);
      bputl(out, stid);
      bputl(out, lama);
      ucw_off_t start_pos = btell(in);
      while ((x = bgetl(in)) != ~0U)
	bgetl(in);
      uns orig_cnt = (btell(in) - start_pos) / 8;
      stem_array = big_alloc(sizeof(struct stem_pair) * orig_cnt);
      bsetpos(in, start_pos);
      uns cnt = 0;
      for (i=0; i<orig_cnt; i++)
	{
	  struct stem_pair *p = &stem_array[cnt];
	  p->stem = bgetl(in);
	  if (stid != 0x80000000)
	    p->stem = id_renumber(p->stem);
	  p->derived = bgetl(in);
	  if (stid != 0x80000001)
	    p->derived = id_renumber(p->derived);
	  if (p->stem != 0xffffffff && p->derived != 0xffffffff)
	    cnt++;
	}
      bgetl(in);
      stem_sort(stem_array, cnt);
      i = 0;
      while (i < cnt)
	{
	  j = i;
	  while (j < cnt && stem_array[j].stem == stem_array[i].stem)
	    j++;
	  bputl(out, 0x80000000 | stem_array[i].stem);
	  while (i < j)
	    bputl(out, stem_array[i++].derived);
	}
      bputl(out, ~0U);
      big_free(stem_array, sizeof(struct stem_pair) * orig_cnt);
    }
  bclose(in);
  bclose(out);
}

static void
stems_scan(void)
{
  struct fastbuf *in = index_bopen(fn_stems_ordered, O_RDONLY, 0);
  uns stid, stem, wrd;
  uns acnt = 0, scnt = 0;
  while ((stid = bgetl(in)) != ~0U)
    {
      bgetl(in);
      while ((stem = bgetl(in)) != ~0U)
	{
	  wrd = bgetl(in);
	  if (stid < 0x80000000)
	    {
	      uns iwrd = lex_local_id(wrd);
	      uns istem = lex_local_id(stem);
	      struct lex_entry *w = word_array_orig[iwrd];
	      struct lex_entry *s = word_array_orig[istem];
	      if (!lex_referenced(w) && bit_array_isset(accent_word_class_refd, iwrd))
		{
		  lex_mark(w);			/* Variants, which differ only in accents from remembered words, should be remembered as well */
		  acnt++;
		}
	      if (lex_referenced(w) && !lex_referenced(s))
		{
		  lex_mark(s);			/* Stems of remembered words must be remembered, too */
		  scnt++;
		}
	    }
	}
    }
  bclose(in);
  ITRACE("Stemming keeps %d unused words for accent variants and %d for stems", acnt, scnt);
}
#endif

/*** Main ***/

static char *short_opts = CF_SHORT_OPTS "o";
static struct option longopts[] =
{
	CF_LONG_OPTS
	{ "optimize",		0, 0, 'o' },
	{ NULL,			0, 0, 0 }
};

static char *help = "\
Usage: lexsort [<options>]\n\
\n\
Options:\n"
CF_USAGE
"-o, --optimize\t\tOptimize lexicon by omitting unreferenced words\n\
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
  int opt;

  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, short_opts, longopts, NULL)) >= 0)
    switch (opt)
      {
	case 'o':
	  optimize = 1;
	  break;
	default:
	  usage();
      }
  if (optind < argc)
    usage();

  /* If in optimizing mode, scan for accent equivalences first */
  if (optimize)
    accent_scan();

  /* Read the input */
  struct mempool *pool = mp_new(65536);
  struct fastbuf *in = index_bopen(fn_lex_words, O_RDONLY, 0);
  n_words = bgetl(in);
  log(L_INFO, "Reading lexicon: %d words", n_words);
  word_array_orig = big_alloc(n_words * sizeof(struct lex_entry *));
  ctxt_counter = big_alloc_zero(lex_context_slots * sizeof(ctxt_counter[0]));
  for (uns i=0; i<n_words; i++)
    {
      struct lex_entry le, *e;
      breadb(in, &le, sizeof(le));
      if (le.class == WC_COMPLEX)
	{
	  /* We put the context slot after the lex_entry as an u16
	   * (beware, it doesn't need to fit in a context_t!)
	   */
	  uns ctxt = GET_CONTEXT(&le.ctxt);
	  e = mp_alloc_fast_noalign(pool, sizeof(le) + 2);
	  PUT_U16(e+1, ctxt_counter[ctxt]);
	  ctxt_counter[ctxt]++;
	  n_cplx++;
	}
      else
	e = mp_alloc_fast_noalign(pool, sizeof(le) + le.length);
      memcpy(e, &le, sizeof(le));
      breadb(in, e->w, le.length);
      if (!GET_U16(e->ch_len))
	{
	  /* If there is no reference chain, decide if we need to keep the
	   * entry and encode it in the ref_pos field.
	   */
	  bzero(e->ref_pos, sizeof(e->ref_pos));
	  if (le.class != WC_NORMAL)
	    lex_mark(e);
	}
      word_array_orig[i] = e;
    }
  bclose(in);

#ifdef CONFIG_LANG
  /* Scan stem expansions to identify what we cannot drop */
  if (optimize)
    {
      log(L_INFO, "Scanning stem expansions");
      stems_scan();
      xfree(accent_word_class_refd);
    }
#endif

  /* Prepare output file */
  struct fastbuf *out = index_bopen(fn_lexicon, O_WRONLY|O_CREAT|O_TRUNC, 0);
  uns n_dropped = 0;
  if (optimize)
    {
      for (uns i=0; i<n_words; i++)
	if (!lex_referenced(word_array_orig[i]))
	  n_dropped++;
      log(L_INFO, "Dropped %d unused words", n_dropped);
    }
  bputl(out, n_words - n_cplx - n_dropped);
  bputl(out, n_cplx);

  /* Sort and dump words */
  log(L_INFO, "Sorting words");
  word_array = big_alloc(sizeof(struct lex_entry *) * n_words);
  memcpy(word_array, word_array_orig, sizeof(struct lex_entry *) * n_words);
  word_sort(word_array, n_words);
  uns j = 0;
  for (uns i=0; i<n_words; i++)
    {
      struct lex_entry *e = word_array[i];
      if (lex_referenced(e) || !optimize)
	{
	  if (!GET_U16(e->ch_len))			/* Delete our `referenced' marks */
	    bzero(e->ref_pos, sizeof(e->ref_pos));
	  bwrite(out, e, sizeof(struct lex_entry) + e->length);
	  PUT_U32(e->ref_pos, 8*j+8);			/* Misuse ref_pos for new word ID */
	  j++;
	}
      else
	PUT_U32(e->ref_pos, 0xffffffff);
    }
  bclose(out);

#ifdef CONFIG_LANG
  /* Renumber, sort and dump stem expansions */
  log(L_INFO, "Sorting stem expansions");
  xlat_table = (u32 *) word_array;
  for (uns j=0; j<n_words; j++)
    xlat_table[j] = GET_U32(word_array_orig[j]->ref_pos);
  big_free(word_array_orig, n_words * sizeof(struct lex_entry *));
  stems_renumber();
#endif

  return 0;
}
