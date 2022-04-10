/*
 *	Sherlock Indexer -- Lexicon Ordering
 *
 *	(c) 2001--2005 Martin Mares <mj@ucw.cz>
 *
 *	This module reorders the lexicon to get words which are likely
 *	to be processed together (e.g. if they differ only in accents)
 *	close to each other, thus making the reference chains close to
 *	each other as well, minimizing disk seek time.
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/math.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/hashfunc.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "charset/unicat.h"
#include "indexer/indexer.h"
#include "indexer/lexicon.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

#define LH_NEED_CLEANUP
#define LH_LEXORDER
#include "indexer/lexhash.h"

static struct verbum **word_array, **id_to_word;
static uns n_words, n_context_words, n_all_words;

struct ctxt_verbum {
  struct verbum v;
  u32 null;			/* empty word name at the end */
};

static struct ctxt_info {
  struct verbum *root;
  struct ctxt_verbum *contexts;
} *ctxt_info;

static void
lex_load_words(struct fastbuf *s, uns cnt)
{
  for (uns i=0; i<cnt; i++)
    {
      byte buf[MAX_WORD_BYTES+1];
      u32 origid = bgetl(s);
      uns class = origid & 7;
      ASSERT(class != WC_COMPLEX);
      u32 origcnt = bgetl(s);
      bget_context(s);
      uns len = bgetc(s);
      breadb(s, buf, len);
      buf[len] = 0;
      struct verbum *v = lh_insert(buf, 0);
      if (!v)
	die("Malformed lexicon: Duplicate word <%s>", buf);
      v->id = origid;
      v->u.count = origcnt;
      ASSERT(v->id/8 <= cnt);
      word_array[i] = v;
    }
  ASSERT(lh_id == 8*cnt);
}

static void
lex_assign_ids(struct verbum **w, uns cnt)
{
  for (uns i=0; i<cnt; i++)
    {
      struct verbum *v = *w++;
      v->id = (v->id & 7) | (8*(i+1));
    }
}

/* Maintaining equivalence classes */

/* Tarjan's Union-Find tree for word equivalence classes, tree roots have 0x80000000 + tree size */
static uns *equiv;

static uns
get_class_root(uns i)
{
  /* Get root of Tarjan's tree containing i and perform path compression along the way */
  uns j = i;
  while (!(equiv[j] & 0x80000000))
    j = equiv[j];
  while (i != j)
    {
      uns k = equiv[i];
      equiv[i] = j;
      i = k;
    }
  return i;
}

static inline void
merge_classes(uns i, uns j)
{
  i = get_class_root(i);
  j = get_class_root(j);
  if (i != j)
    {
      equiv[i] += equiv[j] & 0x7fffffff;
      equiv[j] = i;
    }
}

static struct fastbuf *f_classes;

static uns
find_class_rep(uns *cls, uns n UNUSED)
{
  /* Find the most frequent word as a representative */
  uns bestc = 0, rep = 0;
  for (uns i=0; i<n; i++)
    if (id_to_word[cls[i]]->u.count > bestc)
      {
	bestc = id_to_word[cls[i]]->u.count;
	rep = cls[i];
      }
  if (!bestc)		/* Synonymic classes can have no frequency */
    return cls[0];

#ifdef LOCAL_DEBUG
  DBG("Class: n=%d", n);
  for (uns i=0; i<n; i++)
    DBG("\t<%s> (%x) %d%s", id_to_word[cls[i]]->word, id_to_word[cls[i]]->id & 7,
	id_to_word[cls[i]]->u.count, cls[i] == rep ? " *" : "");
#endif

  if (f_classes)
    {
      bprintf(f_classes, "# Class with %d entries\n", n);
      for (uns i=0; i<n; i++)
	{
	  struct verbum *v = id_to_word[cls[i]];
	  char *r = (cls[i] == rep ? " *" : "");
	  if ((v->id & 7) != WC_COMPLEX)
	    bprintf(f_classes, "%s%s\n", v->word, r);
	  else
	    {
	      struct ctxt_info *c = &ctxt_info[GET_CONTEXT(&v->context_class)];
	      struct verbum *w = c->root;
	      bprintf(f_classes, "%s [%d]%s\n", w->word, (int)((struct ctxt_verbum *)v - c->contexts), r);
	    }
	}
    }

  /* Check class collisions */
  struct verbum *r = id_to_word[rep];
  for (uns i=0; i<n; i++)
    if (cls[i] != rep)
      {
	struct verbum *v = id_to_word[cls[i]];
	if (((v->id ^ r->id) & 7) && v->u.count)
	  {
#if 0					/* FIXME: Temporarily disabled */
	    log(L_WARN, "Words <%s> and <%s> are in the same class, categories differ (%d != %d)",
		r->word, v->word, r->id & 7, v->id & 7);
#endif
	    /* We leave the colliding pairs in the stem file as there is no easy way how to delete them */
	  }
      }

  return rep;
}

static void
find_class_reps(void)
{
  uns total = 0;
  uns nclasses = 0;
  uns maxclass = 0;
  DBG("Searching for representatives...");
  for (uns i=1; i<=n_words; i++)
    if (equiv[i] == 0x80000001)
      equiv[i] = 0x80000000;
    else if (equiv[i] > 0x80000001)
      total += equiv[i] & 0x7fffffff;
  DBG("%d words in non-trivial classes", total);

  f_classes = index_maybe_bopen(fn_lex_classes, O_WRONLY|O_CREAT|O_TRUNC, 1);

  if (total)			/* There are non-trivial classes */
    {
      uns *cl = big_alloc(sizeof(uns) * total);
      total = 0;
      /* Prepare slots for classes, each class starts with its root */
      for (uns i=1; i<=n_words; i++)
	if (equiv[i] > 0x80000000)
	  {
	    uns j = equiv[i] & 0x7fffffff;
	    cl[total] = i;
	    equiv[i] = 0x80000000 + total + 1;
	    total += j;
	  }
      for (uns i=1; i<=n_words; i++)
	if (equiv[i] < 0x80000000)
	  {
	    uns j = get_class_root(i);
	    ASSERT(equiv[j] > 0x80000000);
	    cl[equiv[j]++ & 0x7fffffff] = i;
	  }
      uns k = 0;
      while (k < total)
	{
	  ASSERT(equiv[cl[k]] > 0x80000000);
	  uns l = k+1;
	  while (l < total && equiv[cl[l]] < 0x80000000)
	    l++;
	  nclasses++;
	  maxclass = MAX(maxclass, l-k);
	  uns rep = find_class_rep(cl+k, l-k);
	  while (k < l)
	    equiv[cl[k++]] = rep;
	}
      big_free(cl, sizeof(uns) * total);
    }
  /* Trivial classes are their own representatives */
  for (uns i=1; i<=n_words; i++)
    if (equiv[i] == 0x80000000)
      equiv[i] = i;
    else
      ASSERT(equiv[i] < 0x80000000);
  bclose(f_classes);
  log(L_INFO, "Found %d trivial classes + %d non-trivial classes (largest=%d)", n_words-total, nclasses, maxclass);
}

/* Construction of context-dependent word slots */

#ifdef CONFIG_CONTEXTS

static void
generate_context_words(void)
{
  log(L_INFO, "Generating slots for %d context-dependent words", n_context_words);
  if (!n_context_words)
    return;

  uns cnt = 0;
  uns nn = n_words;
  ctxt_info = big_alloc_zero(sizeof(struct ctxt_info) * n_context_words);
  for (uns i=0; i<n_words; i++)
    {
      struct verbum *v = word_array[i];
      if ((v->id & 7) == WC_CONTEXT)
	{
	  struct ctxt_info *c = &ctxt_info[cnt];
	  c->root = v;
	  c->contexts = big_alloc_zero(sizeof(struct ctxt_verbum) * 2 * lex_context_slots);
	  for (uns j=0; j<2*lex_context_slots; j++)
	    {
	      struct verbum *w = &c->contexts[j].v;
	      word_array[nn++] = w;
	      w->id = (lh_id += 8);
	      id_to_word[w->id/8] = w;
	      PUT_CONTEXT(&w->context_class, cnt);
	      merge_classes(v->id/8, w->id/8);
	    }
	  cnt++;
	}
    }
  n_words = nn;
  ASSERT(n_words == n_all_words);
}

static void
assign_context_ids(void)
{
  if (!n_words)
    return;

  uns intervals = 0;
  u64 total_interval_size = 0;
  for (uns i=0; i<n_words;)
    {
      uns ilen = 0;
      while (i < n_words && (word_array[i]->id & 7) != WC_CONTEXT)
	{
	  if ((word_array[i]->id & 7) != WC_COMPLEX)
	    {
	      total_interval_size += MAX(word_array[i]->u.count, 1);
	      ilen++;
	    }
	  i++;
	}
      if (ilen)
	intervals++;
      i++;
    }
  if (lex_context_slots < intervals + n_context_words)
    die("Out of Lexicon.ContextSlots (we have %d context-dependent words and %d intervals between them)", n_context_words, intervals);

  uns nfree = lex_context_slots - intervals - n_context_words;
  u64 avg_slot_size = nfree ? (total_interval_size+nfree-1) / nfree : ~0ULL;
  u64 t = 0;
  uns bucki = 0;
  /* FIXME: Use a better algorithm */
  for (uns i=0; i<n_words;)
    {
      if ((word_array[i]->id & 7) == WC_CONTEXT)
	{
	  word_array[i++]->context_class = bucki++;
	  continue;
	}
      uns ilen = 0;
      while (i < n_words && (word_array[i]->id & 7) != WC_CONTEXT)
	{
	  struct verbum *v = word_array[i];
	  if ((v->id & 7) != WC_COMPLEX)
	    {
	      v->context_class = bucki;
	      t += MAX(v->u.count, 1);
	      if (t >= avg_slot_size)
		{
		  t -= avg_slot_size;
		  bucki++;
		}
	      ilen++;
	    }
	  i++;
	}
      if (ilen)
	bucki++;
    }
  log(L_INFO, "Context slots: used %d out of %d", bucki, lex_context_slots);
  ASSERT(bucki <= lex_context_slots);
}

#endif

#ifdef CONFIG_LANG

#include "lang/lang.h"

static struct fastbuf *
generate_stems(void)
{
  struct fastbuf *b = index_bopen_tmp(0);
  struct stemmer *st;
  struct mempool *mp;

  if (clist_empty(&stemmer_list))
    return b;

  log(L_INFO, "Searching for stems");
  mp = mp_new(4096);
  lang_init_stemmers();
  CLIST_WALK(st, stemmer_list)
    {
      DBG("Trying stemmer %d langmask %08x", st->id, st->lang_mask);
      bputl(b, st->id);
      bputl(b, st->lang_mask);
      uns npairs = 0;
      uns old_id = lh_id;
      for (uns i=0; i<n_words; i++)
	{
	  struct verbum *v = word_array[i];
	  struct word_node *stem;
	  clist *stems;
	  PROGRESS(i, "%s: %d words of %d, created %d new stems", st->name, i, n_words, (lh_id-old_id)/8);
	  mp_flush(mp);
	  struct word_node req = { .word_form = WORD_FORM_OTHER, .stem_form = WORD_FORM_LEMMA, .unaccented = 0, .w = v->word };
	  if (stems = lang_stem(st, &req, mp))
	    CLIST_WALK(stem, *stems)
	      {
		struct verbum *w = lh_lookup_utf8(stem->w);
		if (v == w)
		  ;
		else if (w->id < 8)
		  log(L_WARN, "Word <%s> has too long stem <%s>", v->word, stem->w);
		else
		  {
		    bputl(b, w->id);
		    bputl(b, v->id);
		    npairs++;
		  }
	      }
	}
      bputl(b, ~0U);
      log(L_INFO, "Stemmer %s (langmask %x): Processed %d words, created %d new stems",
	  st->name, st->lang_mask, npairs, (lh_id-old_id)/8);
    }
  bflush(b);
  mp_delete(mp);
  return b;
}

static void
generate_syn_classes(struct fastbuf *b)
{
  struct syndict *s;
  struct mempool *mp;
  byte **cls;

  if (clist_empty(&syndict_list))
    return;

  mp = mp_new(4096);
  log(L_INFO, "Constructing synonymic classes");
  struct fastbuf *c = bopen_file(b->name, O_RDONLY, &indexer_fb_params);
  bsetpos(c, btell(b));
  CLIST_WALK(s, syndict_list)
    {
      uns nclasses = 0;
      uns nwords = 0;
      uns old_id = lh_id;

      /* Construct direct map */
      syndict_open(s);
      bputl(b, 0x80000000);
      bputl(b, s->lang_mask);
      while (cls = syndict_read_entry(s, mp))
	{
	  PROGRESS(nclasses, "%s: %d classes", s->name, nclasses);
	  nclasses++;
	  if (!cls[0] || !cls[1])
	    continue;
	  struct verbum *w = NULL;
	  for (uns i=0; cls[i]; i++)
	    {
	      struct verbum *v = lh_lookup_utf8(cls[i]);
	      if (v->id < 8)
		log(L_WARN, "Word <%s> is too long", cls[i]);
	      else if (w && ((v->id ^ w->id) & 7))
		log(L_WARN, "Synonyms <%s> and <%s> have different classes (%d != %d)", v->word, w->word, v->id&7, w->id&7);
	      else
		{
		  bputl(b, nclasses);
		  bputl(b, v->id);
		  w = v;
		  nwords++;
		}
	    }
	}
      syndict_close(s);
      bputl(b, ~0U);
      bflush(b);

      /* Construct reverse map */
      bgetl(c);
      bgetl(c);
      bputl(b, 0x80000001);
      bputl(b, s->lang_mask);
      u32 x, y;
      while ((x = bgetl(c)) != ~0U)
	{
	  y = bgetl(c);
	  bputl(b, y);
	  bputl(b, x);
	}
      bputl(b, ~0U);

      log(L_INFO, "Synonymic dictionary %s (langmask %x): Processed %d classes (%d words, %d new)",
	  s->name, s->lang_mask, nclasses, nwords, (lh_id-old_id)/8);
    }
  bflush(b);
  bclose(c);
  mp_delete(mp);
}

static void
stem_equivs(struct fastbuf *s)
{
  u32 x, y;

  bsetpos(s, 0);
  while ((x = bgetl(s)) < 0x80000000)
    {
      bgetl(s);
      while ((x = bgetl(s)) != ~0U)
	{
	  y = bgetl(s);
	  merge_classes(x/8, y/8);
	}
    }
}

static void
renumber_stems(struct fastbuf *s)
{
  struct fastbuf *d = index_bopen(fn_stems_ordered, O_WRONLY|O_CREAT|O_TRUNC, 0);
  u32 stid, langmask, x, y;

  bsetpos(s, 0);
  while ((stid = bgetl(s)) != ~0U)
    {
      langmask = bgetl(s);
      bputl(d, stid);
      bputl(d, langmask);
      while ((x = bgetl(s)) != ~0U)
	{
	  y = bgetl(s);
	  if (stid != 0x80000000)
	    x = id_to_word[x/8]->id;
	  bputl(d, x);
	  if (stid != 0x80000001)
	    y = id_to_word[y/8]->id;
	  bputl(d, y);
	}
      bputl(d, ~0U);
    }
  bclose(s);
  bclose(d);
}

#endif

/* Building equivalence classes of words differing by accents -- sort in accentless order first */

static inline int
accent_lt(struct verbum *x, struct verbum *y)
{
  byte *xx = x->word;
  byte *yy = y->word;
  uns xu, yu, xuu, yuu;
  int pass2 = 0;

  while (1)
    {
      xx = utf8_get(xx, &xu);
      yy = utf8_get(yy, &yu);
      if (!xu || !yu)
	break;
      xuu = Uunaccent(xu);
      yuu = Uunaccent(yu);
      if (xuu < yuu)
	return 1;
      if (xuu > yuu)
	return 0;
      if (!pass2)
	{
	  if (xu < yu)
	    pass2 = -1;
	  else if (xu > yu)
	    pass2 = 1;
	}
    }
  if (yu)
    return 1;
  if (xu)
    return 0;
  if (pass2 < 0)
    return 1;
  else if (pass2 > 0)
    return 0;
  ASSERT(x == y);
  return 0;
}

static inline int
accentless_same(struct verbum *x, struct verbum *y)
{
  byte *xx = x->word;
  byte *yy = y->word;
  uns xu, yu;

  while (1)
    {
      xx = utf8_get(xx, &xu);
      yy = utf8_get(yy, &yu);
      if (!xu || !yu)
	return (!xu && !yu);
      if (Uunaccent(xu) != Uunaccent(yu))
	return 0;
    }
}

#define ASORT_PREFIX(id) accent_##id
#define ASORT_KEY_TYPE struct verbum *
#define ASORT_LT(x,y) accent_lt(x,y)
#include "ucw/sorter/array.h"

static void
accent_equivs(void)
{
  for (uns i=1; i<n_words; i++)
    if (accentless_same(word_array[i-1], word_array[i]))
      merge_classes(word_array[i-1]->id/8, word_array[i]->id/8);
}

/*
 * Sort words in reference chain ordering. Try to bring together things which are
 * likely to be accessed together. The precise ordering is:
 *
 *	1.  by class representatives
 *	2.  normal words before context slots (and those by context ID)
 *	3.  words (or roots of the slots) in accentless order
 *	4.  words (or roots) in accented order.
 */

static inline int
word_lt(struct verbum *x, struct verbum *y)
{
  uns xclass = equiv[x->id/8];
  uns yclass = equiv[y->id/8];
  if (xclass != yclass)
    {
      x = id_to_word[xclass];
      y = id_to_word[yclass];
    }
#ifdef CONFIG_CONTEXTS
  else if ((x->id & 7) == WC_COMPLEX)
    {
      if ((y->id & 7) == WC_COMPLEX)
	{
	  struct ctxt_info *xc = &ctxt_info[x->context_class];
	  struct ctxt_info *yc = &ctxt_info[y->context_class];
	  uns xi = (struct ctxt_verbum *)x - xc->contexts;
	  uns yi = (struct ctxt_verbum *)y - yc->contexts;
	  if (xi < yi)
	    return 1;
	  if (xi > yi)
	    return 0;
	  x = xc->root;
	  y = yc->root;
	  /* We know that even the new x,y belong to the same class */
	}
      else
	return 0;
    }
  else if ((y->id & 7) == WC_COMPLEX)
    return 1;
#endif
  ASSERT((x->id & 7) != WC_COMPLEX && (y->id & 7) != WC_COMPLEX);
  return accent_lt(x, y);
}

#define ASORT_PREFIX(id) word_##id
#define ASORT_KEY_TYPE struct verbum *
#define ASORT_LT(x,y) word_lt(x,y)
#include "ucw/sorter/array.h"

/* Squeezing of word frequencies to range [0,255] */

static void
lex_squeeze_freqs(void)
{
  uns maxf = 0;
  for (uns i=0; i<n_words; i++)
    maxf = MAX(maxf, word_array[i]->u.count);
  double maxflog = log2(maxf+2);
  for (uns i=0; i<n_words; i++)
    {
      uns cnt = word_array[i]->u.count;
      if (cnt)
	{
	  int logf = 1 + (int)(.5 + 254 * log2(cnt) / maxflog);
	  word_array[i]->u.count = CLAMP(logf, 1, 255);
	}
    }
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  setproctitle_init(argc, argv);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  /* Load words from the lexicon to memory */
  struct fastbuf *s = index_bopen(fn_lex_raw, O_RDONLY, 0);
  n_words = bgetl(s);
  log(L_INFO, "Loading lexicon: %d words", n_words);
  word_array = big_alloc(sizeof(struct verbum *) * n_words);
  lh_init();
  lex_load_words(s, n_words);
  bclose(s);

#ifdef CONFIG_LANG
  /* Generate stem list for all stemmers on all known words, new words can arise */
  struct fastbuf *stems_tmp = generate_stems();
  /* Generate synonymic classes */
  generate_syn_classes(stems_tmp);
#endif

  /* Generate array of all words indexed by original word ID */
  big_free(word_array, sizeof(struct verbum *) * n_words);
  n_words = lh_id/8;
  LH_WALK(l)
    if ((l->id & 7) == WC_CONTEXT)
      n_context_words++;
  n_all_words = n_words + 2*lex_context_slots*n_context_words;
  word_array = big_alloc(sizeof(struct verbum *) * n_all_words);
  DBG("Gathering IDs for %d words", n_all_words);
  id_to_word = big_alloc(sizeof(struct verbum *) * (n_all_words+1));
  uns cnt = 0;
  LH_WALK(l)
    {
      id_to_word[l->id/8] = l;
      word_array[cnt++] = l;
    }
  ASSERT(cnt == n_words);
  lh_cleanup();

  /* Now we know all the words, so let's group them to equivalence classes and find their representatives */
  log(L_INFO, "Constructing equivalence classes for %d words", n_all_words);
  equiv = big_alloc(sizeof(uns) * (n_all_words+1));
  for (uns i=1; i<=n_all_words; i++)
    equiv[i] = 0x80000001;
  log(L_INFO, "Merging classes according to accents");
  accent_sort(word_array, n_words);
  accent_equivs();
#ifdef CONFIG_LANG
  log(L_INFO, "Merging classes according to stems");
  stem_equivs(stems_tmp);
#endif
#ifdef CONFIG_CONTEXTS
  /* Add the context-dependent words to both id_to_word and word_array */
  /* and also merge them to the corresponding classes; increases n_words */
  generate_context_words();
#endif
  find_class_reps();

  /* Sort the word array and assign new IDs to words */
  log(L_INFO, "Sorting words");
  word_sort(word_array, n_words);
  lex_assign_ids(word_array, n_words);

#ifdef CONFIG_CONTEXTS
  /* Assign final numbers of contexts to all words */
  assign_context_ids();
#endif

  /* Recalculate word frequencies to logarithmic scale */
  lex_squeeze_freqs();

  /* Write out everything we know */
  log(L_INFO, "Writing final lexicon");
  struct fastbuf *d = index_bopen(fn_lex_ordered, O_WRONLY|O_CREAT|O_TRUNC, 0);
  bputl(d, n_words);
  for (uns i=0; i<n_words; i++)
    {
      struct verbum *v = word_array[i];
      bputl(d, v->id);
      bputl(d, v->u.count);
      bput_context(d, (v->id & 7) == WC_COMPLEX ?
		   GET_CONTEXT(&ctxt_info[GET_CONTEXT(&v->context_class)].root->context_class) :
		   GET_CONTEXT(&v->context_class));
      uns c = str_len(v->word);
      bputc(d, c);
      bwrite(d, v->word, c);
    }
  bclose(d);

#ifdef CONFIG_LANG
  /* Renumber the stem list */
  log(L_INFO, "Writing stems");
  renumber_stems(stems_tmp);
#endif

  return 0;
}
