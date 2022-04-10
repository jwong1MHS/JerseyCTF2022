/*
 *	Sherlock Content Analyser -- Configuration
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "ucw/conf.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "analyser/analyser.h"

#include <string.h>

#define TRACE(x...) do { if (an_trace) log(L_DEBUG, x); } while (0)

static struct analyser *analysers[] = {
#define AN_MODULE(x) &x,
#include "analyser/a-list.h"
  NULL
#undef AN_MODULE
};

static clist an_hooks[AN_HOOK_MAX];

uns an_trace;
static uns an_log_stats;

static char *
hook_init(struct an_hook *h)
{
  h->a = NULL;
  h->older_than = ~(ucw_time_t)0;
  return NULL;
}

static char *
hook_commit(struct an_hook *h)
{
  if (!h->a)
    return "Missing analyser";
  return NULL;
}

static char *
analyser_parser(uns number UNUSED, char **pars, struct analyser **ptr)
{
  CF_JOURNAL_VAR(*ptr);
  uns a;
  for (a=0; analysers[a] && strcmp(analysers[a]->name, pars[0]); a++)
    ;
  if (!(*ptr = analysers[a]))
    return "Unknown analyser";
  return NULL;
}

static char *
condition_parser(uns number UNUSED, char **pars, ucw_time_t *ptr)
{
  byte *err;
  uns t;
  if (!strcmp(pars[0], "always"))
    *ptr = ~(ucw_time_t)0;
  else if (!strcmp(pars[0], "needed"))
    *ptr = 0;
  else if (err = cf_parse_int(pars[0], &t))
    return err;
  else
    *ptr = t;
  return NULL;
}

static struct cf_section hook_section = {
  CF_TYPE(struct an_hook),
  CF_INIT(hook_init),
  CF_COMMIT(hook_commit),
  CF_ITEMS {
    CF_PARSER("Analyser", PTR_TO(struct an_hook, a), analyser_parser, 1),
    CF_PARSER("Condition", PTR_TO(struct an_hook, older_than), condition_parser, 1),
    CF_STRING("Parameter", PTR_TO(struct an_hook, parameter)),
    CF_END
  }
};

static struct cf_section analyser_config = {
  CF_ITEMS{
    CF_UNS("Trace", &an_trace),
    CF_UNS("LogStats", &an_log_stats),
    CF_LIST("HookRaw", an_hooks + AN_HOOK_RAW, &hook_section),
    CF_LIST("HookGather", an_hooks + AN_HOOK_GATHERER, &hook_section),
    CF_LIST("HookScanner", an_hooks + AN_HOOK_SCANNER, &hook_section),
    CF_LIST("HookChewer", an_hooks + AN_HOOK_CHEWER, &hook_section),
    CF_LIST("HookATest", an_hooks + AN_HOOK_ATEST, &hook_section),
    CF_END
  }
};

static const byte * const an_hook_names[AN_HOOK_MAX] = { "raw", "gather", "scanner", "chewer", "atest" };

static void CONSTRUCTOR
analyser_init_config(void)
{
  cf_declare_section("Analyser", &analyser_config, 0);
}

void
analyser_init_hook(enum an_hook_type hook_type)
{
  DBG("analyser_init_hook(%d)", hook_type);
  CLIST_FOR_EACH(struct an_hook *, h, an_hooks[hook_type])
    {
      struct analyser *a = h->a;
      if (!h->initialized)
        {
          if (a->init)
            a->init(h);
          h->initialized++;
        }
    }
}

void
analyser_init(struct an_context *c, enum an_hook_type hook_type, uns avail_need_mask, struct an_context *master)
{
  DBG("analyser_init()");
  c->init_pool = mp_new(1024);
  clist_init(&c->list);
  c->need_mask = 0;
  c->master = (master && master != c) ? master : NULL;
  CLIST_FOR_EACH(struct an_hook *, hook, an_hooks[hook_type])
    {
      struct an_hook *h = mp_memdup(c->init_pool, hook, sizeof(*h));
      struct analyser *a = h->a;
      ASSERT(hook->initialized);
      h->c = c;
      clist_add_tail(&c->list, &h->n);
      if (a->init_context)
        a->init_context(h);
      if (h->need_mask & ~avail_need_mask)
        die("Analyser %s cannot be plugged to %s (have=%x, need=%x)",
            a->name, an_hook_names[hook_type], avail_need_mask, h->need_mask);
      c->need_mask |= h->need_mask;
      h->doc_count = 0;
    }
}

void
analyser_cleanup(struct an_context *c)
{
  CLIST_FOR_EACH(struct an_hook *, h, c->list)
    {
      struct analyser *a = h->a;
      if (a->cleanup_context)
	a->cleanup_context(h);
    }
  mp_delete(c->init_pool);
}

static int
analyser_needed_p(struct an_hook *h, struct an_iface *ai)
{
  struct analyser *a = h->a;
  if (h->older_than == ~(ucw_time_t)0)
    {
      TRACE("Analyser %s: Always", a->name);
      return 1;
    }
  else if (obj_find_anum(ai->url_block, 'D', 0) < h->older_than)
    {
      TRACE("Analyser %s: Too old", a->name);
      return 1;
    }
  else if (!a->need || a->need(h, ai))
    {
      TRACE("Analyser %s: Needed", a->name);
      return 1;
    }
  else
    {
      TRACE("Analyser %s: Up-to-date", a->name);
      return 0;
    }
}

uns
analyser_need(struct an_context *c, struct an_iface *ai)
{
  uns needed = 0;
  CLIST_FOR_EACH(struct an_hook *, h, c->list)
    {
      h->triggered = analyser_needed_p(h, ai);
      if (h->triggered)
	needed |= AN_NEED_TO_RUN | h->need_mask;
    }
  return needed;
}

void
analyser_run(struct an_context *c, struct an_iface *ai)
{
  CLIST_FOR_EACH(struct an_hook *, h, c->list)
    {
      TRACE("Analyser %s: Running", h->a->name);
      h->doc_count++;
      if (ai->text)
	brewind(ai->text);
      if (ai->metas)
	brewind(ai->metas);
      if (ai->thumbnail)
	brewind(ai->thumbnail);
      h->a->analyse(h, ai);
    }
}

void
analyser_run_needed(struct an_context *c, struct an_iface *ai)
{
  CLIST_FOR_EACH(struct an_hook *, h, c->list)
    if (h->triggered)
      {
	h->doc_count++;
	if (ai->text)
	  brewind(ai->text);
	if (ai->metas)
	  brewind(ai->metas);
	if (ai->thumbnail)
	  brewind(ai->thumbnail);
	h->a->analyse(h, ai);
      }
}

void
analyser_merge_stats(struct an_context *c)
{
  if (an_log_stats && c->master)
    {
      struct an_hook *m = (struct an_hook *)clist_head(&c->master->list);
      CLIST_FOR_EACH(struct an_hook *, h, c->list)
        {
	  m->doc_count += h->doc_count;
	  m = (struct an_hook *)clist_next(&c->master->list, &m->n);
	}
    }
}

void
analyser_log_stats(struct an_context *c)
{
  if (an_log_stats)
    CLIST_FOR_EACH(struct an_hook *, h, c->list)
      log(L_INFO, "Analyser %s: processed %u documents", h->a->name, h->doc_count);
}

int
analyser_lookup_hook(byte *name)
{
  for (int i=0; i<AN_HOOK_MAX; i++)
    if (!strcmp(an_hook_names[i], name))
      return i;
  return -1;
}
