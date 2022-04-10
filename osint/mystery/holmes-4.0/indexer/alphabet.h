/*
 *	Sherlock Indexer -- Character Class Table for Lexical Mapping
 *
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 */

#ifndef _SHERLOCK_INDEXER_ALPHABET_H
#define _SHERLOCK_INDEXER_ALPHABET_H

enum alphabet_class {
  AC_SPACE,
  AC_ALPHA,
  AC_DIGIT,
  AC_PUNCT,
  AC_BREAK,
  AC_SINGLETON,
  AC_LIGATURE,
  AC_INHERIT
};

extern byte alpha_class[];

void alphabet_init(void);

#endif
