/*
 *	Sherlock Indexer -- Sort Lexicon by Word Frequencies
 *
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "indexer/indexer.h"
#include "indexer/lexicon.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

struct lex {
  u32 id;
  u32 freq;
  context_t ctxt;
  byte length;
  byte w[0];
} PACKED;

static struct lex **words;

static inline int
word_lt(struct lex *x, struct lex *y)
{
  return (x->freq > y->freq);
}

#define ASORT_PREFIX(id) word_##id
#define ASORT_KEY_TYPE struct lex *
#define ASORT_LT(x,y) word_lt(x,y)
#include "ucw/sorter/array.h"

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

  log(L_INFO, "Sorting lexicon by word frequencies");

  struct fastbuf *s = index_bopen(fn_lex_raw, O_RDONLY, 0);
  struct mempool *mp = mp_new(65536);
  uns n_words = bgetl(s);
  words = big_alloc(sizeof(struct lex *) * n_words);
  for (uns i=0; i<n_words; i++)
    {
      uns id = bgetl(s);
      uns fr = bgetl(s);
      bget_context(s);
      uns l = bgetc(s);
      struct lex *w = mp_alloc_fast(mp, sizeof(struct lex) + l);
      w->id = id;
      w->freq = fr;
      PUT_CONTEXT(&w->ctxt, 0);
      w->length = l;
      bread(s, w->w, l);
      words[i] = w;
    }
  bclose(s);

  word_sort(words, n_words);

  struct fastbuf *d = index_bopen(fn_lex_by_freq, O_WRONLY|O_CREAT|O_TRUNC, 0);
  bputl(d, n_words);
  for (uns i=0; i<n_words; i++)
    bwrite(d, words[i], sizeof(struct lex) + words[i]->length);
  bclose(d);

  return 0;
}
