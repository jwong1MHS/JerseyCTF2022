/*
 *	Sherlock Indexer -- Lexical Mapping Tester
 *
 *	(c) 2002 Martin Mares <mj@ucw.cz>
 */

#define LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "sherlock/index.h"
#include "ucw/unaligned.h"
#include "sherlock/tagged-text.h"
#include "ucw/unicode.h"
#include "indexer/lexicon.h"
#include "charset/unicat.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#undef DBG
#define DBG(x,y...) printf(x "\n",##y)

#define LH_MKLEX
#include "indexer/lexhash.h"

static enum word_class
lm_lookup(enum word_class orig_class, u16 *uni, uns ulen, word_id_t *idp, const byte *orig, uns olen, void *user UNUSED)
{
  struct verbum *v;
  byte wbuf[MAX_WORD_BYTES+1];
  enum word_class class = orig_class;

  if (orig)
    {
      memcpy(wbuf, orig, olen);
      wbuf[olen] = 0;
    }
  else
    wbuf[0] = 0;
  if (orig_class == WC_NORMAL)
    {
      v = lh_lookup(uni, ulen);
      *idp = v;
      class = v->id & 7;
    }
  printf("lookup: <%s> oc=%d nc=%d\n", wbuf, orig_class, class);
  return class;
}

static void
lm_got_word(uns pos, uns cat, word_id_t w, void *user UNUSED)
{
  printf("\t-> @%d T%d C%d I%x <%s>\n", pos, w->id & 7, cat, w->id, w->word);
  w->u.count++;
}

static void
lm_got_complex(uns pos, uns cat, word_id_t wroot, word_id_t wctxt, uns after, void *user UNUSED)
{
  printf("\t-> @%d T%d <%x,%x,%d>\n", pos, cat, wroot->id, wctxt->id, after);
}

#define LM_TRACK_TEXT
#include "indexer/lexmap.h"

static void
lh_dump(void)
{
  printf("\nLexical hash table:\n\n");
  LH_WALK(v)
    printf("%08x <%s> C%d R%d\n", v->id, v->word, v->id & 7, v->u.count);
}

static void
help(void)
{
    fputs("This program supports only the following command-line arguments:\n\n" CF_USAGE
	  "-c\t\t\tInput is a card file (otherwise it's plain text)\n"
	  "-h\t\t\tDump hash tables at the end\n",
	  stderr);
    exit(1);
}

int
main(int argc, char **argv)
{
  byte line[4096];
  int opt;
  int is_card = 0;
  int dump_hashes = 0;

  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS "ch", CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      case 'c':
	is_card = 1;
	break;
      case 'h':
	dump_hashes = 1;
	break;
      default:
	help();
      }
  if (optind < argc)
    help();

  lm_init();
  lh_init();
  lm_doc_start(NULL);
  while (fgets(line, sizeof(line), stdin))
    {
      byte *e = strchr(line, '\n');
      ASSERT(e);
      *e = 0;
      if (is_card)
	{
	  if (line[0] == 'X')
	    lm_map_text(line+1, e);
	}
      else
	lm_map_text(line, e);
    }
  if (dump_hashes)
    lh_dump();

  return 0;
}
