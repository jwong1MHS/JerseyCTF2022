/*
 *	Sherlock Language Processing Library -- Test Utility for Stemming
 *
 *	(c) 2003--2006 Martin Mares <mj@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "lang/lang.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void NONRET
usage(void)
{
  fputs("Usage: stemtest <options>\n\
\n\
Options:\n\
-i\t\t\tIgnore case\n\
-l <language>\t\tSet language in which stemming will take place\n\
-x\t\t\tExpand words\n\
-X\t\t\tExpand stems\n\
", stderr);
  exit(1);
}

static u32 langs;
static int expand, nocase;
static struct mempool *mp;

static void
go(byte *w)
{
  if (utf8_strlen(w) > MAX_WORD_CHARS)
    {
      msg(L_ERROR, "Word too long");
      return;
    }
  struct stemmer *st;
  CLIST_WALK(st, stemmer_list)
    if (st->lang_mask & langs)
      {
	printf("Stemmer %s(%s):\n", st->name, st->params);
	fflush(stdout);
	mp_flush(mp);
	clist *x;
	struct word_node req = {
	  .word_form = (expand == 2 ? WORD_FORM_LEMMA : WORD_FORM_OTHER),
	  .stem_form = (expand ? WORD_FORM_OTHER : WORD_FORM_LEMMA),
	  .unaccented = nocase,
	  .w = w
	};
	if (x = lang_stem(st, &req, mp))
	  {
	    struct word_node *w;
	    CLIST_WALK(w, *x)
	      {
		printf("\t%s (%s -> %s", w->w, word_form_names[w->word_form], word_form_names[w->stem_form]);
		if (w->variant)
		  printf(" VAR%d", w->variant);
		if (w->unaccented)
		  printf(" NOCASE");
		printf(")\n");
	      }
	  }
	else
	  puts("---");
      }
  puts(".");
}

int
main(int argc, char **argv)
{
  int opt, l;
  byte buf[256];

  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS "il:xX", CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      case 'i':
	nocase = 1;
	break;
      case 'l':
	l = lang_name_to_code(optarg);
	if (l < 0)
	  die("Unknown language %s", optarg);
	langs |= 1 << l;
	break;
      case 'x':
	expand = 1;
	break;
      case 'X':
	expand = 2;
	break;
      default:
	usage();
      }
  if (!langs)
    langs = ~0U;
  mp = mp_new(4096);
  lang_init_stemmers();

  if (optind < argc)
    {
      while (optind < argc)
	go(argv[optind++]);
    }
  else
    while (fgets(buf, sizeof(buf)-1, stdin))
      {
	byte *nl = strchr(buf, '\n');
	if (nl)
	  *nl = 0;
	go(buf);
      }

  return 0;
}
