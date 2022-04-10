/*
 *	Sherlock Filter Engine --- hash tables
 *
 *	(c) 2002 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/hashfunc.h"
#include "ucw/prime.h"
#include "filter/filter.h"

#include <string.h>

struct filter_hash_table *
filter_ht_new(struct mempool *mp, uns count, uns icase)
{
	uns size = nextprime(count*2);
	struct filter_hash_table *ht;
	ht = mp_alloc_zero(mp, sizeof(struct filter_hash_table) + size * sizeof(struct filter_hash_record *));
	ht->size = size;
	ht->count = 0;
	ht->icase = icase;
	ht->mp = mp;
	return ht;
}

void
filter_ht_add(struct filter_hash_table *ht, byte *string, struct filter_case *cas)
{
	struct filter_hash_record *rec = mp_alloc(ht->mp, sizeof(struct filter_hash_record));
	uns hash = ht->icase ? hash_string_nocase(string) : hash_string(string);
	hash %= ht->size;
	rec->next = ht->h[hash];
	rec->string = string;
	rec->cas = cas;
	ht->h[hash] = rec;
	ht->count++;
}

void
filter_ht_find(struct filter_hash_table *ht, byte *string, struct filter_cases *res)
{
	uns hash = ht->icase ? hash_string_nocase(string) : hash_string(string);
	struct filter_hash_record *rec;
	hash %= ht->size;
	for (rec=ht->h[hash]; rec; rec=rec->next)
	{
		if (ht->icase && !strcasecmp(string, rec->string)
		|| !ht->icase && !strcmp(string, rec->string))
			filter_cases_add(res, rec->cas);
	}
}
