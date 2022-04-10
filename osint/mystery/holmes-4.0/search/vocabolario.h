/*
 *	Sherlock Search Engine -- Lexical Hash a.k.a. Il Vocabolario
 *
 *	(c) 2005 Martin Mares <mj@ucw.cz>
 *
 *	This module takes care of representing a set of words (vocabola)
 *	and their attributes (class, word ID etc.), optimized for both lookup
 *	and insert speed. Many of the optimizations were inspired by indexer/lexhash.h,
 *	but what makes this structure unique is the ability to clone hash tables
 *	with sharing as much information as possible.
 *
 *	For each database, Sherlock keeps a vocabolario of its exceptional words.
 *	When performing fulltext matching, it constructs a clone of this structure
 *	and adds all variants of query words to it.
 *
 *	Currently, we expect that that an upper bound on the number of words inserted
 *	is known upon structure creation and that query words are never cloned.
 */

#ifndef _SEARCH_VOCABOLARIO_H
#define _SEARCH_VOCABOLARIO_H

#include "ucw/mempool.h"
#include "ucw/unaligned.h"
#include "ucw/unicode.h"
#include "charset/unicat.h"

#include <string.h>

struct vocabolo {
  struct vocabolo *next;		/* Next in hash chain */
  struct vocabolo *same;		/* Next entry for the same word, but with different attributes */
  byte word_class;			/* WC_xxx */
  byte word_id;				/* Query word ID or 0xff if not a part of the query (changed to 0xfe for copied entries) */
  byte penalty;				/* Matching conditions extracted from struct word and struct variant */
  byte noaccent_only;
  u32 lang_mask;
  byte w[0];				/* The word itself */
};

struct vocabolario {
  struct mempool *pool;			/* Pool the whole structure lives in */
  struct vocabolo **hash_table;		/* The hash table */
  uns hash_size;			/* Hash table size (a prime) */
  uns hash_limit;			/* The table can sustain at most this many items without slowdown */
  uns num_items;			/* Items currently present */
  uns max_items;			/* Max number of items the user promised to insert */
};

struct vocabolario *voc_new(struct mempool *pool, uns max_items, struct vocabolario *clone);
void voc_dump(struct vocabolario *voc);

typedef struct {
  uns len;
  uns hash;
  byte k[MAX_WORD_BYTES+4];
} voc_key_t;

static inline u32 PURE
voc_hash(struct vocabolario *voc, u32 *x, uns l)
{
  u32 h = *x++;
  while (--l)
    h = ROL(h, 5) ^ *x++;
  return h % voc->hash_size;
}

static inline uns
voc_key_finish(struct vocabolario *voc, voc_key_t *key, byte *k)
{
  byte *kk = key->k;
#if CPU_ALLOW_UNALIGNED
  *(u32 *)k = 0;
#else
  switch ((w - kk) & 3)
    {
    default: *k++ = 0;
    case 1:  *k++ = 0;
    case 2:  *k++ = 0;
    case 3:  *k   = 0;
    }
#endif
  key->len = (k - kk + 4)/4;
  key->hash = voc_hash(voc, (u32*)key->k, key->len);
  return 1;
}

static inline uns
voc_key_ucs2(struct vocabolario *voc, voc_key_t *key, u16 *w, uns len)
{
  byte *k = key->k;
  while (len--)
    {
      uns u = *w++;
      u = Utolower(u);
      k = utf8_put(k, u);
    }
  return voc_key_finish(voc, key, k);
}

static inline uns
voc_key_utf8(struct vocabolario *voc, voc_key_t *key, byte *w)
{
  byte *k = key->k;
  for(;;)
    {
      uns u;
      w = utf8_get(w, &u);
      if (!u)
	break;
      u = Utolower(u);
      k = utf8_put(k, u);
    }
  return voc_key_finish(voc, key, k);
}

static inline struct vocabolo *
voc_find(struct vocabolario *voc, voc_key_t *key)
{
  struct vocabolo *v;
  for (v = voc->hash_table[key->hash]; v; v=v->next)
    {
      for (uns i=0; i<key->len; i++)
	if (((u32*)v->w)[i] != ((u32*)key->k)[i])
	  goto cont;
      return v;
    cont: ;
    }
  return NULL;
}

static inline struct vocabolo *
voc_insert(struct vocabolario *voc, voc_key_t *key)
{
  struct vocabolo *v = mp_alloc_fast(voc->pool, sizeof(*v) + 4*key->len);
  v->next = voc->hash_table[key->hash];
  voc->hash_table[key->hash] = v;
  v->same = NULL;
  v->word_class = 0xff;
  memcpy(v->w, key->k, 4*key->len);
  /* The rest is up to the caller */
  return v;
}

static inline struct vocabolo *
voc_insert_variant(struct vocabolario *voc, voc_key_t *key)
{
  struct vocabolo *v = voc_find(voc, key);
  struct vocabolo *w;
  if (!v || v->word_id == 0xff)
    w = voc_insert(voc, key);
  else if (v->word_id == 0xfe)
    w = v;
  else
    {
      w = mp_alloc_fast(voc->pool, sizeof(*w));
      w->next = v;
      w->same = v->same;
      v->same = w;
    }
  if (v)
    w->word_class = v->word_class;
  return w;
}

#endif
