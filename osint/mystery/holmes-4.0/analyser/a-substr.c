/*
 *	Sherlock SubStrings Analyser -- Substring Search
 *
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "analyser/analyser.h"
#include "ucw/mempool.h"
#include "ucw/fastbuf.h"
#include "ucw/chartype.h"
#include "sherlock/tagged-text.h"
#include "sherlock/conf.h"
#include "charset/unicat.h"

#include <string.h>

#define KMP_PREFIX(x) kmp_##x
#define KMP_CHAR u16
#define KMP_STATE_VARS u32 mask;
#define KMP_ADD_EXTRA_ARGS uns mask
#define KMP_ADD_INIT(kmp,src) DBG("Adding string %s with mask %d", src, mask)
#define KMP_ADD_NEW(kmp,src,s) s->u.mask |= mask
#define KMP_ADD_DUP KMP_ADD_NEW
#define KMP_BUILD_STATE(kmp,s) do{ if (s->next) s->u.mask |= s->next->u.mask; }while(0)
#define KMP_CONTROL_CHAR ':'
#define KMP_USE_UTF8
#define KMP_TOLOWER
#define KMP_USE_POOL cf_pool
#define KMP_WANT_SEARCH
#define KMP_WANT_CLEANUP
#define KMPS_VARS uns phrase; uns mask;
#define KMPS_SOURCE struct fastbuf *
static const KMP_CHAR kmp_sentence_break[] = { ':', '.' };
#define KMPS_GET_CHAR(kmp,src,s) ({				\
  int result = 1;						\
  if (s->u.phrase)						\
    s->c = kmp_sentence_break[--s->u.phrase];			\
  else								\
    {								\
      uns w = bget_tagged_char(src);				\
      if ((int)w == -1)						\
        result = 0;						\
      else if (w >= 0x80000000)					\
        {							\
          if ((w & 0xf0) == 0x90)				\
            s->u.phrase = 2;					\
          s->c = ':';						\
        }							\
      else							\
        {							\
          w = Utolower(w);					\
          if (!Ualpha(w))					\
            w = ':';						\
          s->c = w;						\
        }							\
      }								\
    result; })
#define KMPS_INIT(kmp,src,s) s->u.phrase = 0
#define KMPS_STEP(kmp,src,s) s->u.mask |= s->s->u.mask
#define KMPS_ADD_CONTROLS
#define KMPS_MERGE_CONTROLS
#include "ucw/kmp.h"

enum substr_parts {
  PART_TEXT = 1,
  PART_METAS = 2,
  PART_URLS = 4
};

struct substr_context {
  cnode n;
  char *name;
  uns attr;
  uns append;
  uns parts;
  clist links;
  uns initialized;
  struct kmp_struct kmp;
};

struct substr_group {
  cnode n;
  char *name;
  clist items;
};

struct substr_link {
  cnode n;
  char *group;
  uns mask;
};

struct substr_item {
  cnode n;
  char *str;
};

static clist contexts;
static clist groups;

static byte *
substr_context_commit(struct substr_context *p)
{
  if (!p->name)
    return "Missing SubStrings.Context.Name";
  if (!p->attr)
    return "Missing SubStrings.Context.Attr";
  if (!p->parts)
    return "Missing or empty SubStrings.Context.Parts";
  if (p->initialized)
    {
      CF_JOURNAL_VAR(p->initialized);
      CF_JOURNAL_VAR(p->kmp);
      p->initialized = 0;
    }
  return NULL;
}

static byte *
substr_group_commit(struct substr_group *p)
{
  if (!p->name)
    return "Missing SubStrings.Group.Name";
  return NULL;
}

static byte *
substr_link_init(struct substr_link *p)
{
  p->mask = 1;
  return NULL;
}

static struct cf_section item_config = {
#define F(x) PTR_TO(struct substr_item, x)
  CF_TYPE(struct substr_item),
  CF_ITEMS {
    CF_STRING("String", F(str)),
    CF_END
  }
#undef F
};

static struct cf_section group_config = {
#define F(x) PTR_TO(struct substr_group, x)
  CF_TYPE(struct substr_group),
  CF_COMMIT(&substr_group_commit),
  CF_ITEMS {
    CF_STRING("Name", F(name)),
    CF_LIST("Search", F(items), &item_config),
    CF_END
  }
#undef F
};

static struct cf_section link_config = {
#define F(x) PTR_TO(struct substr_link, x)
  CF_TYPE(struct substr_group),
  CF_INIT(&substr_link_init),
  CF_ITEMS {
    CF_STRING("Group", F(group)),
    CF_UNS("Mask", F(mask)),
    CF_END
  }
#undef F
};

static struct cf_section context_config = {
#define F(x) PTR_TO(struct substr_context, x)
  CF_TYPE(struct substr_context),
  CF_COMMIT(&substr_context_commit),
  CF_ITEMS {
    CF_STRING("Name", F(name)),
    CF_USER("Attr", F(attr), &cf_type_attr),
    CF_UNS("Append", F(append)),
    CF_BITMAP_LOOKUP("Parts", F(parts), ((const char * const []) {"text", "metas", "urls", NULL })),
    CF_LIST("Groups", F(links), &link_config),
    CF_END
  }
#undef F
};

static struct cf_section an_substr_config = {
  CF_ITEMS {
    CF_LIST("Context", &contexts, &context_config),
    CF_LIST("Group", &groups, &group_config),
    CF_END
  }
};

static void CONSTRUCTOR
an_substr_config_init(void)
{
  cf_declare_section("SubStrings", &an_substr_config, 0);
}

static void
an_substr_init(struct an_hook *h)
{
  if (!h->parameter)
    die("Missing context name in substr analyser");
  DBG("an_substr_init(name=%s)", h->parameter);
  CLIST_FOR_EACH(struct substr_context *, c, contexts)
    if (!strcasecmp(c->name, h->parameter))
      {
	h->need_mask = 0;
	if (c->parts & PART_TEXT)
	  h->need_mask |= AN_NEED_TEXT;
	if (c->parts & PART_METAS)
	  h->need_mask |= AN_NEED_METAS;
	if (c->parts & PART_URLS)
	  h->need_mask |= AN_NEED_ALL_URLS;
	ASSERT(h->need_mask);
	h->priv = c;
	if (c->initialized)
	  return;
	c->initialized++;
	bzero(&c->kmp, sizeof(c->kmp));
	kmp_init(&c->kmp);
	CLIST_FOR_EACH(struct substr_link *, l, c->links)
	  {
	    CLIST_FOR_EACH(struct substr_group *, g, groups)
	      if (!strcasecmp(g->name, l->group))
		{
		  CLIST_FOR_EACH(struct substr_item *, i, g->items)
		    kmp_add(&c->kmp, i->str, l->mask);
		  goto found;
		}
	      die("Undefined group name '%s' in substr context", l->group);
found: ;
	  }
	kmp_build(&c->kmp);
	return;
      }
  die("Undefined context name '%s' in substr analyser", h->parameter);
}

static int
an_substr_need(struct an_hook *h, struct an_iface *ai)
{
  DBG("an_substr_need()");
  struct substr_context *c = h->priv;
  return c->append || !obj_find_attr(ai->obj, c->attr);
}

static void
an_substr_analyse(struct an_hook *h, struct an_iface *ai)
{
  DBG("an_substr_analyse()");
  struct substr_context *c = h->priv;
  struct kmp_search search;
  struct fastbuf fb;
  search.u.mask = 0;
  if (ai->text && (h->need_mask & AN_NEED_TEXT))
    kmp_search(&c->kmp, &search, ai->text);
  if (ai->metas && (h->need_mask & AN_NEED_METAS))
    kmp_search(&c->kmp, &search, ai->metas);
  if (h->need_mask & AN_NEED_ALL_URLS)
    for (struct odes **o = ai->all_urls; *o; o++)
      {
        byte *url = obj_find_aval(*o, 'U');
        if (url)
          {
            fbbuf_init_read(&fb, url, strlen(url), 0);
            kmp_search(&c->kmp, &search, &fb);
          }
      }
  if (search.u.mask)
    DBG("Found document with mask 0x%x", search.u.mask);
  if (c->append)
    search.u.mask |= obj_find_x32(ai->obj, c->attr, 0);
  obj_set_attr_format(ai->obj, c->attr, "%x", search.u.mask);
}

struct analyser an_substr = {
  .name = "substr",
  .init = an_substr_init,
  .need = an_substr_need,
  .analyse = an_substr_analyse,
};

