/*
 *	Sherlock Shepherd Daemon -- Sorting of URL Indices
 *
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#define SORT_KEY_REGULAR struct url_state

#ifdef SORT_BY_FP
#undef SORT_BY_FP
#define SORT_HASH_FP
static inline int
SORT_PREFIX(compare)(struct url_state *x, struct url_state *y)
{
  return fp_cmp(&x->fp, &y->fp);
}
#endif

#ifdef SORT_HASH_FP
#undef SORT_HASH_FP
#define SORT_HASH_BITS 32

static inline uns
SORT_PREFIX(hash)(struct url_state *x)
{
  return x->fp.site.x[0];
}
#endif

#ifdef SORT_UNIFY
static inline void
SORT_PREFIX(write_merged)(struct fastbuf *f, struct url_state **keys, void **data UNUSED, uns n UNUSED, void *buf UNUSED)
{
  bwrite(f, *keys, sizeof(**keys));
}
#endif

#include "ucw/sorter/sorter.h"
