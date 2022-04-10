/*
 *	Sherlock Indexer -- Sorting of Word Index
 *
 *	(c) 2001--2003 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/unaligned.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "ucw/ff-unicode.h"
#include "ucw/ff-binary.h"
#include "ucw/heap.h"
#include "indexer/indexer.h"
#include "indexer/lexicon.h"
#include "indexer/params.h"

#include <stdio.h>
#include <stdlib.h>

struct ws_key {
  u32 size;
  u32 wordid;
};

#define SORT_PREFIX(x) ws_##x
#define SORT_KEY struct ws_key
#define SORT_DATA_SIZE(k) ((k).size)
#define SORT_UNIFY
#define SORT_UNIFY_WORKSPACE(k) REFCHAIN_UNIFY_WORKSPACE
#define SORT_DELETE_INPUT sort_delete_src
#define SORT_INPUT_FILE
#define SORT_OUTPUT_FB

static inline int
ws_compare(struct ws_key *x, struct ws_key *y)
{
  COMPARE(x->wordid, y->wordid);
  return 0;
}

static inline int
ws_read_key(struct fastbuf *f, struct ws_key *x)
{
  uns id = bgetl(f);
  if (id == ~0U)
    return 0;
  x->wordid = id;
  x->size = bgetl(f);
  return 1;
}

static inline void
ws_write_key(struct fastbuf *f, struct ws_key *x)
{
  bputl(f, x->wordid);
  bputl(f, x->size);
}

#include "indexer/refmerger.h"
#include "indexer/refslicer.h"

static void
ws_write_merged(struct fastbuf *dest, struct ws_key **keys, void **data, uns n, void *buf)
{
  bputl(dest, keys[0]->wordid);
  refchain_write_merged(n, (void *)keys, data, dest, buf);
}

static void
ws_copy_merged(struct ws_key **keys, struct fastbuf **data, uns n, struct fastbuf *dest)
{
  bputl(dest, keys[0]->wordid);
  refchain_copy_merged(n, (void *)keys, data, dest);
}

#include "ucw/sorter/sorter.h"

static void
split(struct fastbuf *sorted)
{
  struct fastbuf *lex_tmp, *lex, *refs;
  u32 wid_lex;
  uns wid_ref, wlen, wcount;
  enum word_class wclass;
  byte word[MAX_WORD_BYTES+1];

  lex_tmp = index_bopen(fn_lex_ordered, O_RDONLY, 0);
  refs = index_bopen(fn_references, O_WRONLY | O_CREAT | O_APPEND, 0);
  lex = index_bopen(fn_lex_words, O_WRONLY | O_CREAT | O_TRUNC, 0);
  wid_ref = bgetl(sorted);
  wid_lex = 1;
  wcount = bgetl(lex_tmp);
  bputl(lex, wcount);
  slice_init();
  while (wid_lex <= wcount)
    {
      u32 in_id = bgetl(lex_tmp);
      ASSERT(in_id/8 == wid_lex);
      uns wfreq = bgetl(lex_tmp);
      wclass = in_id & 7;
      uns ctxt = bget_context(lex_tmp);
      wlen = bgetc(lex_tmp);
      breadb(lex_tmp, word, wlen);
      bputo(lex, btell(refs));
      if (wid_lex >= wid_ref)
	{
	  uns rsize = slice_chain(sorted, refs, 0xfffffff, wid_ref);
	  bputw(lex, (rsize + 0xfff) >> 12U);
	  wid_ref = bgetl(sorted);
	}
      else
	bputw(lex, 0);
      bputc(lex, wclass);
#ifdef CONFIG_SPELL
      bputc(lex, wfreq);
#endif
      bput_context(lex, ctxt);
      bputc(lex, wlen);
      bwrite(lex, word, wlen);
      wid_lex++;
    }
  ASSERT(wid_ref = 0xffffffff);
  slice_cleanup();
  if ((u64)btell(refs) >= ((u64)1 << 8 * BYTES_PER_O))
    die("Too large references. Check CONFIG_LARGE_DB configuration switch.");
  bclose(lex_tmp);
  bclose(refs);
  bclose(lex);
}

int
main(int argc, char **argv)
{
  struct fastbuf *sorted;

  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  log(L_INFO, "Sorting word index");
  sorted = ws_sort(index_name(fn_word_index), NULL);
  log(L_INFO, "Splitting word index");
  split(sorted);
  bclose(sorted);
  return 0;
}
