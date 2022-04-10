/*
 *	Sherlock Indexer -- Lexicon Functions
 *
 *	(c) 2001--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2006 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/mempool.h"
#include "indexer/indexer.h"
#include "indexer/lexicon.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/*
 *  Configurable parameters
 */

struct lexicon_config lexicon_config = {
  .min_len_ign = 1,
  .min_len = 1,
  .max_len = MAX_WORD_CHARS,
  .max_num_len = MAX_WORD_CHARS,
  .max_mixed_len = MAX_WORD_CHARS,
  .max_ctrl_len = MAX_WORD_CHARS,
  .max_gap = 3,
  .context_slots = 256
};

clist lex_exceptions;

static clist list_ignored, list_normal, list_garbage;
#ifdef CONFIG_CONTEXTS
static clist list_context;
#endif

static void
copy_exc(clist *src, enum word_class class)
{
  CLIST_FOR_EACH(struct exception *, n, *src)
    {
      struct exception *d = cf_malloc(sizeof(*d));
      d->class = class;
      d->w = n->w;
      clist_add_tail(&lex_exceptions, &d->n);
    }
}

static char *
merge_lists(void *ptr UNUSED)
{
  CF_JOURNAL_VAR(lex_exceptions);
  clist_init(&lex_exceptions);
  copy_exc(&list_ignored, WC_IGNORED);
  copy_exc(&list_normal, WC_NORMAL);
  copy_exc(&list_garbage, WC_GARBAGE);

#ifdef CONFIG_CONTEXTS
  copy_exc(&list_context, WC_CONTEXT);
  if (lex_context_slots > CONFIG_MAX_CONTEXTS)
    return "Lexicon.ContextSlots is larger than CONFIG_MAX_CONTEXTS.";
#endif
  return NULL;
}

#ifndef CONFIG_CONTEXTS
static char *
no_contexts(uns number UNUSED, char **pars UNUSED, void *ptr UNUSED)
{
  return "Support for word contexts not compiled in";
}
#endif

static struct cf_section word_list_config = {
  CF_TYPE(struct exception),
#define F(x)	PTR_TO(struct exception, x)
  CF_ITEMS {
    CF_STRING("Word", (char **)F(w)),
    CF_END
  }
#undef F
};

static struct cf_section lex_config = {
  CF_COMMIT(merge_lists),
  CF_ITEMS {
    CF_UNS("MinWordLenIgnore", &lex_min_len_ign),
    CF_UNS("MinWordLen", &lex_min_len),
    CF_UNS("MaxWordLen", &lex_max_len),
    CF_UNS("MaxNumWordLen", &lex_max_num_len),
    CF_UNS("MaxMixedWordLen", &lex_max_mixed_len),
    CF_UNS("MaxCtrlWordLen", &lex_max_ctrl_len),
    CF_LIST("WordIgnored", &list_ignored, &word_list_config),
    CF_LIST("WordNormal", &list_normal, &word_list_config),
    CF_LIST("WordGarbage", &list_garbage, &word_list_config),
#ifdef CONFIG_CONTEXTS
    CF_LIST("WordContext", &list_context, &word_list_config),
#else
    CF_PARSER("WordContext", NULL, no_contexts, CF_ANY_NUM),
#endif
    CF_UNS("MaxGap", &lex_max_gap),
    CF_UNS("ContextSlots", &lex_context_slots),
    CF_END
  }
};

static void CONSTRUCTOR lexconf_init(void)
{
  cf_declare_section("Lexicon", &lex_config, 0);
}
