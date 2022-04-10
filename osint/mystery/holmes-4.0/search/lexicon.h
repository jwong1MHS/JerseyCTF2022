/*
 *	Sherlock Search Engine -- Lexicon and Related Operations
 *
 *	(c) 1997-2005 Martin Mares <mj@ucw.cz>
 */

#ifndef _SEARCH_LEXICON_H
#define _SEARCH_LEXICON_H

#include "indexer/lexicon.h"

/*** General functions ***/

void lexicon_init(struct database *db);

uns word_unaccent_utf8(byte *w, byte *to);

/*** Access to lexicon ***/

static inline struct lex_entry *
lex_get(uns i)
{
  return current_dbase->lex_array[i];
}

static inline uns			/* Convert ID from lexicon format to our format */
lex_import_id(uns id)
{
  return id/8 - 1;
}

static inline uns			/* Convert ID from our format to lexicon format */
lex_export_id(uns pos)
{
  return 8*pos + 8 + lex_get(pos)->class;
}

void lex_extract(uns lex_id, byte *buf);
void lex_extract_noacc(uns lex_id, byte *buf);
uns lex_find_first(uns len, byte *key);
int lex_find_exact(byte *w);

/*** Searching for complexes (simple array lookup) ***/

static inline uns
cplx_make_id(uns root_ctxt, uns context_ctxt, uns dir)
{
  ASSERT(root_ctxt < lex_context_slots && context_ctxt < lex_context_slots && dir < 2);
  if (dir)
    context_ctxt += lex_context_slots;
  return (root_ctxt << 16) | context_ctxt;
}

static inline uns
cplx_dissect_id(uns i, uns *root, uns *ctxt)
{
  *root = i >> 16;
  *ctxt = i & 0xffff;
  if (*ctxt >= lex_context_slots)
    {
      *ctxt -= lex_context_slots;
      return 1;
    }
  else
    return 0;
}

static inline struct lex_entry *
cplx_get(uns i)
{
  uns root = i >> 16;
  uns context = i & 0xffff;
  ASSERT(root < lex_context_slots && context < 2*lex_context_slots && current_dbase->cplx_array[root]);
  return current_dbase->cplx_array[root][context];
}

/*** Exception hash ***/

int query_word_classify(struct database *db, byte *word);

/*** Stemming ***/

#ifdef CONFIG_LANG

#include "lang/lang.h"

struct stem_block {
  cnode n;
  struct stemmer *stemmer;
  u32 *array;
  uns array_items;
};

struct syn_block {
  cnode n;
  u32 lang_mask;
  u32 *direct_array, *inverse_array;
  uns direct_items, inverse_items;
};

u32 *stem_lookup_expansion(u32 *ary, uns nitems, u32 stem_id);

#endif

#endif
