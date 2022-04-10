/*
 *	Sherlock Language Processing Library -- Stemmer Internals
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 */

#define STEMMER(id,name) \
	clist *stem_##name(struct stemmer *, struct word_node *, struct mempool *); \
	void stem_init_##name(struct stemmer *);
#include "lang/stemmers.h"
#undef STEMMER
