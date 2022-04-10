/*
 *	Sherlock Filter Engine --- KMP (substrings)
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/hashfunc.h"
#include "ucw/chartype.h"
#include "ucw/fastbuf.h"
#include "filter/filter.h"

#include <string.h>

#define DO_FOUND(kmp, src, search) \
  do { for (struct filter_kmp_record *rec = search->s->u.rec; rec; rec = rec->next) \
        filter_cases_add(search->u.res, rec->cas); } while(0)

#define KMP_PREFIX(x) kmp_##x
#define KMP_WANT_SEARCH
#define KMP_STATE_VARS struct filter_kmp_record *rec;
#define KMP_INIT_STATE(kmp, s) s->u.rec = NULL;

#define KMPS_VARS struct filter_cases *res;
#define KMPS_FOUND(kmp, src, search) DO_FOUND(kmp, src, search)
#include "ucw/kmp.h"

#define KMPS_KMP_PREFIX(x) kmp_##x
#define KMPS_PREFIX(x) kmpi_##x
#define KMPS_GET_CHAR(kmp,src,search) (search->c = Clocase(*src++))
#define KMPS_VARS struct filter_cases *res;
#define KMPS_FOUND(kmp, src, search) DO_FOUND(kmp, src, search)
#include "ucw/kmp-search.h"

struct filter_kmp_table *
filter_kmp_new(struct mempool *mp, uns icase)
{
  struct filter_kmp_table *kmp = mp_alloc_zero(mp, sizeof(*kmp));
  kmp->kmp = mp_alloc_zero(mp, sizeof(struct kmp_struct));
  kmp->mp = mp;
  kmp->icase = icase;
  kmp_init(kmp->kmp);
  return kmp;
}

void
filter_kmp_add(struct filter_kmp_table *kmp, byte *string, struct filter_case *cas)
{
  struct filter_kmp_record *rec = mp_alloc(kmp->mp, sizeof(*rec));
  uns l = strlen(string);
  ASSERT(l > 2 && string[0] == '*' && string[l - 1] == '*');
  rec->cas = cas;
  byte s[l], *p = s;
  string++;
  while ((*p = *string++) != '*')
    {
      if (*p == '\\')
        *p = *string++;
      ASSERT(*p);
      if (kmp->icase)
	*p = Clocase(*p);
      p++;
    }
  ASSERT(!*string);
  *p = 0;
  struct kmp_state *state = kmp_add(kmp->kmp, s);
  rec->next = state->u.rec;
  state->u.rec = rec;
}

void
filter_kmp_build(struct filter_kmp_table *kmp)
{
  kmp_build(kmp->kmp);
}

void
filter_kmp_find(struct filter_kmp_table *kmp, byte *string, struct filter_cases *res)
{
  if (kmp->icase)
    {
      struct kmpi_search s;
      s.u.res = res;
      kmpi_search(kmp->kmp, &s, string);
    }
  else
    {
      struct kmp_search s;
      s.u.res = res;
      kmp_search(kmp->kmp, &s, string);
    }
}
