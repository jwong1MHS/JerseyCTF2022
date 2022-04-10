/*
 *	Sherlock Language Processing Library -- Stemming
 *
 *	(c) 2003--2008 Martin Mares <mj@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/conf.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "lang/lang.h"
#include "lang/stemmer.h"

#include <string.h>

/* Configuration of stemmers and synonymic dictionaries */

clist stemmer_list, syndict_list;

static const char * const stemmer_names[] = {
#define STEMMER(id, name) [id] = #name,
#include "lang/stemmers.h"
#undef STEMMER
NULL
};

static char *
lang_cf_commit_stemmer(struct stemmer *st)
{
  st->name = stemmer_names[st->id];
  if (!st->params)
    st->params = "";
  return NULL;
}

static struct cf_section lang_cf_stemmer = {
  CF_TYPE(struct stemmer),
  CF_COMMIT(lang_cf_commit_stemmer),
  CF_ITEMS {
    CF_PARSER("Languages", PTR_TO(struct stemmer, lang_mask), lang_cf_parse_set, 1),
    CF_LOOKUP("Algorithm", PTR_TO(struct stemmer, id), stemmer_names),
    CF_STRING("Params", PTR_TO(struct stemmer, params)),
    CF_END
  }
};

static struct cf_section lang_cf_syndict = {
  CF_TYPE(struct syndict),
  CF_ITEMS {
    CF_PARSER("Languages", PTR_TO(struct syndict, lang_mask), lang_cf_parse_set, 1),
    CF_STRING("Name", PTR_TO(struct syndict, name)),
    CF_END
  }
};

static struct cf_section lang_cf_stemmers = {
  CF_ITEMS {
    CF_LIST("Stemmer", &stemmer_list, &lang_cf_stemmer ),
    CF_LIST("SynDict", &syndict_list, &lang_cf_syndict ),
    CF_END
  }
};

static void CONSTRUCTOR lang_stemmer_init_config(void)
{
  cf_declare_section("Stemmers", &lang_cf_stemmers, 0);
}

/* Stemmer hooks */

static clist *(*stemmer_funcs[])(struct stemmer *, struct word_node *, struct mempool *) = {
#define STEMMER(id, name) [id] = stem_##name,
#include "lang/stemmers.h"
#undef STEMMER
};

static void (*stemmer_init_funcs[])(struct stemmer *) = {
#define STEMMER(id, name) [id] = stem_init_##name,
#include "lang/stemmers.h"
#undef STEMMER
};

char *word_form_names[] = { "OTHER", "BASIC", "STEM", "LEMMA" };

clist *
lang_stem(struct stemmer *st, struct word_node *w, struct mempool *mp)
{
  ASSERT((uns)st->id < ARRAY_SIZE(stemmer_funcs));
  return stemmer_funcs[st->id](st, w, mp);
}

void
lang_init_stemmers(void)
{
  struct stemmer *st;
  CLIST_WALK(st, stemmer_list)
    stemmer_init_funcs[st->id](st);
}

struct word_node *
word_list_add(struct mempool *mp, clist *l, byte *w, uns word_form, uns stem_form, uns variant, uns unaccented)
{
  /* The stemmers do not need to check the size themselves, so do it here. */
  if (utf8_strlen(w) > MAX_WORD_CHARS)
    return NULL;
  int len = strlen(w);
  struct word_node *n = mp_alloc(mp, sizeof(*n) + len + 1);
  n->w = (byte *)(n+1);
  memcpy(n->w, w, len+1);
  n->word_form = word_form;
  n->stem_form = stem_form;
  n->variant = variant;
  n->unaccented = unaccented;
  clist_add_tail(l, &n->n);
  return n;
}

struct word_node *
word_list_find(clist *l, byte *w, uns word_form, uns stem_form, uns variant, uns unaccented)
{
  struct word_node *n;
  CLIST_WALK(n, *l)
    if (!strcmp(n->w, w) &&
	(n->word_form == word_form || word_form == ~0U) &&
	(n->stem_form == stem_form || stem_form == ~0U) &&
	n->variant == variant || variant == ~0U &&
	n->unaccented == unaccented || unaccented == ~0U)
      return n;
  return NULL;
}

struct word_node *
word_list_add_unique(struct mempool *mp, clist *l, byte *w, uns word_form, uns stem_form, uns variant, uns unaccented)
{
  struct word_node *n = word_list_find(l, w, word_form, stem_form, variant, unaccented);
  if (n)
    return n;
  else
    return word_list_add(mp, l, w, word_form, stem_form, variant, unaccented);
}
