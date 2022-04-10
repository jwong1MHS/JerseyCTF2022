/*
 *	Sherlock Search Engine -- Lexical Hash a.k.a. Il Vocabolario
 *
 *	(c) 2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "search/vocabolario.h"
#include "ucw/prime.h"

#include <stdio.h>

struct vocabolario *
voc_new(struct mempool *pool, uns max_items, struct vocabolario *clone)
{
  struct vocabolario *voc = mp_alloc_zero(pool, sizeof(*voc));
  voc->pool = pool;
  if (clone)
    max_items += clone->num_items;
  voc->max_items = max_items;

  if (clone && max_items <= clone->hash_limit)
    {
      /* Can share data with the hash table being cloned */
      voc->hash_size = clone->hash_size;
      voc->hash_limit = clone->hash_limit;
      voc->num_items = clone->num_items;
      voc->hash_table = mp_memdup(pool, clone->hash_table, clone->hash_size * sizeof(struct vocabolo *));
    }
  else
    {
      /* Need to create a new table. We make the table gappy, because lookups will be
       * usually called on words which aren't present.
       */
      voc->hash_size = next_table_prime(max_items);
      voc->hash_limit = voc->hash_size / 2;
      voc->hash_table = mp_alloc_zero(pool, voc->hash_size * sizeof(struct vocabolo *));
      if (clone)
	{
	  /* In this case, we re-insert everything from the old table */
	  for (uns i=0; i<clone->hash_size; i++)
	    for (struct vocabolo *v = clone->hash_table[i]; v; v=v->next)
	      {
		ASSERT(v->word_id >= 0x80 && !v->same);
		voc_key_t key;
		voc_key_utf8(voc, &key, v->w);
		struct vocabolo *w = voc_insert(voc, &key);
		w->word_class = v->word_class;
		w->word_id = 0xfe;
		w->penalty = 0;
		w->noaccent_only = 0;
		w->lang_mask = 0;
	      }
	}
    }
  return voc;
}

#ifdef CONFIG_DEBUG
void
voc_dump(struct vocabolario *voc)
{
  log(L_DEBUG, "Dump of vocabulario %p (hash_size=%d, num_items=%d, max_items=%d)", voc, voc->hash_size, voc->num_items, voc->max_items);
  for (uns i=0; i<voc->hash_size; i++)
    for (struct vocabolo *v = voc->hash_table[i]; v; v=v->next)
      for (struct vocabolo *w = v; w; w=w->same)
	{
	  byte buf[1024], *b = buf;
	  if (w != v)
	    b += sprintf(b, "\t\t");
	  else if (v == voc->hash_table[i])
	    b += sprintf(b, "%5d: ", i);
	  else
	    b += sprintf(b, "       ");
	  b += sprintf(b, "<%s> (class=%d, id=%d, pen=%d, noacc=%d, lmask=%x)",
		       w->w, w->word_class, w->word_id, w->penalty, w->noaccent_only, w->lang_mask);
	  log(L_DEBUG, "%s", buf);
	}
  log(L_DEBUG, "END");
}
#endif
