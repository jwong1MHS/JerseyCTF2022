/*
 *	Sherlock Language Processing Library
 *
 *	(c) 2003--2006 Martin Mares <mj@ucw.cz>
 */

#ifndef _LANG_LANG_H
#define _LANG_LANG_H

#include "ucw/clists.h"

/*
 *  Each language gets its own code (0..31). Codes are assigned in the
 *  configuration file, but code 0 is reserved for documents with
 *  unrecognized language and code 31 is used internally by the search
 *  server for texts not belonging to any language.
 */

#define LANG_UNKNOWN 0
#define LANG_NONE 31

#define MAX_LANGUAGES 31

/* Language names (tags) and lists of them as defined by RFC 1766 and RFC 2616 */

int lang_name_to_code(byte *name);	/* So far, we ignore subtags */
int lang_code_exists(uns code);
byte *lang_code_to_name(uns code);
int lang_normalize_tag(byte *tag);
int lang_parse_list(byte *in, byte **langs, int *codes, int strip_subtags);	/* Destructive */
void lang_construct_list(byte *buf, byte **langs, uns n);
int lang_normalize_list(byte *buf, byte *in);
int lang_primary_language(byte *in);	/* Extract the primary language from a list */
uns lang_list_to_set(byte *in);		/* Extract all languages */

#define MAX_LANG_LIST_SIZE 1000		/* Technically, there is no limit on the length of a language list, but having one is useful. */
#define MAX_LANG_LIST_ITEMS 32

/* Configuration parsers */

byte *lang_cf_parse_set(uns c, byte **pars, uns *ptr);

/*
 *  Stemmers (lemmatizers)
 *
 *  The stemmers are able to find a basic form (either a lemma or a truncated lemma)
 *  for a given word and possibly also to find all forms of a given word. The stemmers
 *  are always given a single word_node and they return a (possibly empty) clist of word_node's.
 *
 *  The behavior is controlled by the stem_form and word_form fields:
 *
 *  word_form == WORD_FORM_ANY, stem_form == WORD_FORM_LEMMA	find all lemmata/stems for a given word
 *  word_form == WORD_FORM_LEMMA, stem_form == WORD_FORM_ANY	find all forms for a given lemma
 *  word_form == WORD_FORM_ANY, stem_form == WORD_FORM_ANY	find all forms for a given word
 *
 *  Also, you can set the `unaccented' field in the request to ask for ignoring
 *  accents during the search.
 *
 *  All stemmers are guaranteed to work in any -> lemma/stem option with accents,
 *  everything else is optional.
 */

struct stemmer {
  cnode n;
  u32 lang_mask;			/* Languages this stemmer applies to */
  const char *name;			/* Name of the stemmer */
  int id;				/* ... and its internal ID */
  char *params;				/* Additional stemmer-specific parameters */
  void *priv;				/* Data private to the stemmer */
};

struct word_node {			/* Stemmers and expanders return a list of these nodes */
  cnode n;
  byte word_form;			/* Which word form we entered (WORD_FORM_xxx) */
  byte stem_form;			/* Which word form we return (WORD_FORM_xxx) */
  byte variant;				/* In case of multi-variant lemmata or stems; optional */
  byte unaccented;			/* Accents weren't matched */
  byte *w;				/* The word */
};

enum word_form {
  WORD_FORM_OTHER,			/* General word form */
  WORD_FORM_BASIC,			/* Basic form, but not lemma */
  WORD_FORM_STEM,			/* Stem (i.e., a truncated form of the lemma) */
  WORD_FORM_LEMMA,			/* Lemma */
};

extern clist stemmer_list;
struct mempool;
clist *lang_stem(struct stemmer *st, struct word_node *w, struct mempool *mp);
void lang_init_stemmers(void);

extern char *word_form_names[];
struct word_node *word_list_find(clist *l, byte *w, uns word_form, uns stem_form, uns variant, uns unaccented);
struct word_node *word_list_add(struct mempool *mp, clist *l, byte *w, uns word_form, uns stem_form, uns variant, uns unaccented);
struct word_node *word_list_add_unique(struct mempool *mp, clist *l, byte *w, uns word_form, uns stem_form, uns variant, uns unaccented);

/*
 *  Synonymic dictionaries
 */

struct syndict {
  cnode n;
  u32 lang_mask;
  char *name;
  struct fastbuf *fb;
};

extern clist syndict_list;
void syndict_open(struct syndict *sd);
byte **syndict_read_entry(struct syndict *sd, struct mempool *mp);
void syndict_close(struct syndict *sd);

#endif
