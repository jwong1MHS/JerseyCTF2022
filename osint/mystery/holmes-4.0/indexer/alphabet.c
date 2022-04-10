/*
 *	Sherlock Indexer -- Character Class Table for Lexical Mapping
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/unicode.h"
#include "ucw/slists.h"
#include "charset/unicat.h"
#include "indexer/alphabet.h"
#include "sherlock/conf.h"

#include <string.h>

byte alpha_class[65536];

struct alpha_entry {
  snode n;
  struct unirange range;
  enum alphabet_class class;
};

static slist alpha_conf;
static uns alpha_built;

static void
alpha_build(void)
{
  bzero(alpha_class, sizeof(alpha_class));
  SLIST_FOR_EACH(struct alpha_entry *, e, alpha_conf)
    for (uns i=e->range.min; i<=e->range.max; i++)
      {
	uns c = e->class;
	if (c == AC_INHERIT)
	  {
	    uns u = Ucategory(i);
	    if (u & _U_LETTER)
	      c = AC_ALPHA;
	    else if (u & _U_DIGIT)
	      c = AC_DIGIT;
	    else if (u & (_U_SPACE | _U_CTRL))
	      c = AC_SPACE;
	    else if (u & _U_LIGATURE)
	      c = AC_LIGATURE;
	    else
	      c = AC_PUNCT;
	  }
	alpha_class[i] = c;
      }
  alpha_built = 1;
}

static byte *
parse_class(uns n, byte **pars, void *pclass)
{
  alpha_built = 0;
  for (uns i=0; i<n; i++)
    {
      struct alpha_entry *e = cf_malloc_zero(sizeof(*e));
      byte *err;
      if (err = cf_type_unirange.parser(pars[i], &e->range))
	return err;
      e->class = (enum alphabet_class) pclass;
      /* Rather crude journaling of slists */
      CF_JOURNAL_VAR(alpha_conf.last);
      if (alpha_conf.last)
	CF_JOURNAL_VAR(alpha_conf.last->next);
      else
	CF_JOURNAL_VAR(alpha_conf.head);
      slist_add_tail(&alpha_conf, &e->n);
    }
  return NULL;
}

static struct cf_section alphabet_config = {
  CF_ITEMS {
    CF_PARSER("Inherit", (void*) AC_INHERIT, parse_class, CF_ANY_NUM),
    CF_PARSER("Space", (void*) AC_SPACE, parse_class, CF_ANY_NUM),
    CF_PARSER("Alpha", (void*) AC_ALPHA, parse_class, CF_ANY_NUM),
    CF_PARSER("Punct", (void*) AC_PUNCT, parse_class, CF_ANY_NUM),
    CF_PARSER("Break", (void*) AC_BREAK, parse_class, CF_ANY_NUM),
    CF_PARSER("Singleton", (void*) AC_SINGLETON, parse_class, CF_ANY_NUM),
    CF_END
  }
};

static void CONSTRUCTOR
alphabet_preinit(void)
{
  cf_declare_section("Alphabet", &alphabet_config, 0);
}

void
alphabet_init(void)
{
  if (!alpha_built)
    alpha_build();
}
