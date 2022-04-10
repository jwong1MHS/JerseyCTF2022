/*
 *	Sherlock Indexer -- Lexicon Functions
 *
 *	(c) 2001--2005 Martin Mares <mj@ucw.cz>
 */

#ifndef _SHERLOCK_INDEXER_LEXICON_H
#define _SHERLOCK_INDEXER_LEXICON_H

#include "ucw/clists.h"

/* Word classes */

enum word_class {
  WC_COMPLEX,				/* (ctxt-dependent-word,slot) pair */
  WC_IGNORED,				/* Silently ignored */
  WC_NORMAL,				/* Just an ordinary word */
  WC_GARBAGE,				/* Takes place (word number), but isn't indexed */
  WC_CONTEXT,				/* Indexed only in context */
  WC_BREAK,				/* Explicit sentence break */
  /* lexhash.h assumes that word_class fits in 3 bits */
};

/* Configuration */

struct lexicon_config {
  uns min_len_ign;
  uns min_len;
  uns max_len;
  uns max_num_len;
  uns max_mixed_len;
  uns max_ctrl_len;
  uns max_gap;
  uns context_slots;
};

extern struct lexicon_config lexicon_config;

/* We want to be able to exchange configurations, but not at the cost of dereferencing an extra pointer */
#define lex_min_len_ign lexicon_config.min_len_ign
#define lex_min_len lexicon_config.min_len
#define lex_max_len lexicon_config.max_len
#define lex_max_num_len lexicon_config.max_num_len
#define lex_max_mixed_len lexicon_config.max_mixed_len
#define lex_max_ctrl_len lexicon_config.max_ctrl_len
#define lex_max_gap lexicon_config.max_gap
#define lex_context_slots lexicon_config.context_slots

struct exception {
  cnode n;
  enum word_class class;
  byte *w;
};

extern clist lex_exceptions;

/* Entries in LexWords and Lexicon */

struct lex_entry {	/* Beware, unaligned */
  byte ref_pos[BYTES_PER_O];
  byte ch_len[2];
  byte class;
#ifdef CONFIG_SPELL
  byte freq;
#endif
  context_t ctxt;
  byte length;
  byte w[0];
} PACKED;

#endif /* _INDEXER_LEXICON_H */
