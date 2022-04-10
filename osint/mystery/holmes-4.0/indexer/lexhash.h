/*
 *	Sherlock Indexer -- Lexical Hashing
 *
 *	(c) 2002--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2002 Robert Spalek <robert@ucw.cz>
 *
 *	Super-fast lexicon hash table. Many of the tricks are based
 *	on Robert's ucw/hashfunc.c, but here we have the advantage that
 *	we can pad everything by zeroes to a multiple of word size.
 *
 *	You define:
 *	   - LH_MKLEX if you are indexer/mklex.c
 *	   - LH_LEXORDER if you are indexer/lexorder.c
 *	   - LH_CHEWER if you are indexer/chewer.c
 *	   - LH_LEXSORT if you are indexer/lexsort.c
 *	   - LH_NEED_CLEANUP if you want lh_cleanup()
 *
 *	All functions assume that the hashed words are no longer that
 *	MAX_WORD_CHARS Unicode characters.
 */

#include "ucw/mempool.h"
#include "ucw/prime.h"

typedef struct verbum {
  struct verbum *next;
  u32 id;				/* Word id, lowest 3 bits are word class */
  union {
#ifndef LH_LEXSORT
    u32 count;
    struct lentry *first_lent;
#endif
  } u;
#if defined(LH_LEXORDER) || defined(LH_CHEWER)
  context_t context_class;
#endif
  byte word[0];
} *word_id_t;

static struct verbum **lh_hash_table;
static struct mempool *lh_pool;
static uns lh_hash_size = 64;
static uns lh_hash_count;
static uns lh_hash_limit;
static uns lh_id;

#define LH_WALK(var) \
  for (uns lwi=0; lwi<lh_hash_size; lwi++) \
    for (struct verbum *var=lh_hash_table[lwi]; var; var=var->next)

/* The number of bits the hash is rotated by after every word.
 * It should be prime with the word size.  */
#define SHIFT_BITS 5

static inline uns PURE
lh_wlen(byte *x)
{
  uns l = 1;

  while (x[3])
    {
      x += 4;
      l++;
    }
  return l;
}

static inline u32 PURE
lh_hash(u32 *x, uns l)
{
  u32 h = *x++;
  while (--l)
    h = ROL(h, SHIFT_BITS) ^ *x++;
  return h % lh_hash_size;
}

static void
lh_alloc_hash(void)
{
  lh_hash_size = nextprime(lh_hash_size);
  lh_hash_limit = lh_hash_size*2;
  lh_hash_table = xmalloc_zero(sizeof(struct verbum *) * lh_hash_size);
}

static void
lh_rehash(uns newsize)
{
  struct verbum **oht = lh_hash_table;
  uns i, h;
  uns on = lh_hash_size;
  struct verbum *v, *next, **nht, **w;

  lh_hash_size = newsize;
  lh_alloc_hash();
  nht = lh_hash_table;
  for (i=0; i<on; i++)
    for (v=oht[i]; v; v=next)
      {
	next = v->next;
	h = lh_hash((u32*)v->word, lh_wlen(v->word));
	w = &nht[h];
#ifndef LH_LEXSORT
	while (*w && (*w)->u.count > v->u.count)
	  w = &(*w)->next;
#endif
	v->next = *w;
	*w = v;
      }
  xfree(oht);
}

static inline struct verbum *
lh_hash_new(u32 *x, uns l, uns h)
{
  struct verbum *v = mp_alloc_fast(lh_pool, sizeof(struct verbum) + 4*l);
  struct verbum **w = &lh_hash_table[h];
  uns i;

  while (*w)
    w = &(*w)->next;
  v->next = NULL;
  *w = v;
  v->id = (lh_id += 8);
  bzero(&v->u, sizeof(v->u));
  for (i=0; i<l; i++)
    ((u32*)v->word)[i] = x[i];
  if (++lh_hash_count > lh_hash_limit)
    lh_rehash(2*lh_hash_size);
  return v;
}

static void
lh_init_word(struct verbum *v UNUSED)
{
#if defined(LH_MKLEX) || defined(LH_LEXORDER)
  struct verbum *ex;
  byte ww[MAX_WORD_BYTES+4], *w, *t;
  uns u, l, h, i, chars=0;

  /* Create normalized version of the word */
  t = v->word;
  w = ww;
  while (*t)
    {
      t = utf8_get(t, &u);
      u = Uunaccent(u);
      w = utf8_put(w, u);
      chars++;
    }
  PUT_U32(w, 0);
  l = (w-ww+4)/4;

  /* Try to find it in the hash table as an exception and inherit the class */
  h = lh_hash((u32*)ww, l);
  for (ex=lh_hash_table[h]; ex; ex=ex->next)
    if (ex != v)
      {
	for (i=0; i<l; i++)
	  if (((u32*)ww)[i] != ((u32*)ex->word)[i])
	    goto cont;
	v->id |= ex->id & 7;
	return;
      cont: ;
      }

  /* Not found => classify word by its length */
  v->id |= (chars < lex_min_len_ign) ? WC_IGNORED : (chars < lex_min_len) ? WC_GARBAGE : WC_NORMAL;
#elif defined(LH_LEXSORT)
  /* No initialization needed */
#else
  die("Bug: Word <%s> not found in lexicon", v->word);
#endif
}

static inline struct verbum *
lh_lookup_raw(byte *ww, byte *w)
{
  uns l, h, i;
  struct verbum *v;

#if CPU_ALLOW_UNALIGNED
  *(u32 *)w = 0;
#else
  switch ((w - ww) & 3)
    {
    default: *w++ = 0;
    case 1:  *w++ = 0;
    case 2:  *w++ = 0;
    case 3:  *w   = 0;
    }
#endif
  l = (w - ww + 4)/4;
  h = lh_hash((u32*)ww, l);
  for (v=lh_hash_table[h]; v; v=v->next)
    {
      for (i=0; i<l; i++)
	if (((u32*)v->word)[i] != ((u32*)ww)[i])
	  goto cont;
      return v;
    cont: ;
    }

  v = lh_hash_new((u32*)ww, l, h);
  lh_init_word(v);
  return v;
}

static inline struct verbum *
lh_lookup(u16 *uni, uns ulen)
{
  byte ww[MAX_WORD_BYTES+4], *w;
  uns u;

  w = ww;
  while (ulen--)
    {
      u = *uni++;
      u = Utolower(u);
      w = utf8_put(w, u);
    }
  return lh_lookup_raw(ww, w);
}

static inline struct verbum *
lh_lookup_utf8(byte *c)
{
  byte ww[MAX_WORD_BYTES+4], *w;
  uns u;

  w = ww;
  for(;;)
    {
      c = utf8_get(c, &u);
      if (!u)
	break;
      u = Utolower(u);
      w = utf8_put(w, u);
    }
  return lh_lookup_raw(ww, w);
}

#ifndef LH_LEXSORT
static struct verbum *
lh_insert(byte *c, uns noacc)
{
  byte ww[MAX_WORD_BYTES+1];
  u32 *ww32 = (u32*)ww;
  byte *w = ww;
  uns u, l, h;
  struct verbum *v;
  while (*c)
    {
      c = utf8_get(c, &u);
      u = Utolower(u);
      if (noacc)
	u = Uunaccent(u);
      w = utf8_put(w, u);
    }
  PUT_U32(w, 0);
  l = (w-ww+4)/4;
  h = lh_hash(ww32, l);
  for (v=lh_hash_table[h]; v; v=v->next)
    if (!strcmp(v->word, ww))
      return NULL;
  v = lh_hash_new(ww32, l, h);
  return v;
}
#endif

static void
lh_load_exceptions(void)
{
#ifdef LH_MKLEX
  struct exception *e;

  CLIST_WALK(e, lex_exceptions)
    {
      struct verbum *v = lh_insert(e->w, 1);
      if (!v)
	die("Lexical exception <%s> defined twice", e->w);
      v->id |= e->class;
    }
#endif
}

#ifdef LH_NEED_CLEANUP
static void
lh_cleanup(void)
{
  xfree(lh_hash_table);
}
#endif

#ifdef LH_LEXSORT
static void
lh_cleanup_pool(void)
{
  mp_delete(lh_pool);
}
#endif

static void
lh_init(void)
{
  lh_pool = mp_new(65536);
  lh_alloc_hash();
  lh_load_exceptions();
}
