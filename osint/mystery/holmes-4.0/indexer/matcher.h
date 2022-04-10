/*
 *	Detecting similar documents
 *
 *	(c) 2002, Robert Spalek <robert@ucw.cz>
 */

#ifndef _SHERLOCK_INDEXER_MATCHER_H
#define _SHERLOCK_INDEXER_MATCHER_H

#include "sherlock/sherlock.h"

struct hash_permuter
{
	u32 mult;
};

struct matcher_context {
	struct hash_permuter *perm;
	uns sign_delete_shift;
	u32 sign_cyclic_hash;
	u32 *sign_last_word_hashes;
	uns sign_context;
	uns sign_deleting;
	uns sign_total_words;
	u32 *sign_min;
};

void matcher_init(void);
struct matcher_context *matcher_new(struct matcher_context *clone);
uns matcher_compute_minima(u32 *min, struct matcher_context *ctxt, struct oattr *oa);

#endif
