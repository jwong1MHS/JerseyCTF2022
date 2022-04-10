/*
 *	Sherlock Content Analyser
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 */

#ifndef _ANALYSER_ANALYSER_H
#define _ANALYSER_ANALYSER_H

#include "ucw/clists.h"
#include "ucw/slists.h"
#include "sherlock/index.h"

struct an_iface {		// Information passed to an analyser
  struct odes *obj;		// Original object
  struct odes *url_block;	// The main per-URL block of the object
  struct odes **all_urls;	// All per-URL blocks (NULL-terminated)
  struct fastbuf *text;		// Parsed contents (NULL=not present in this document)
  struct fastbuf *metas;
  struct fastbuf *thumbnail;
  struct mempool *pool;
};

enum an_needed {		// Which parts of the interface are required
  AN_NEED_TEXT = 1,
  AN_NEED_METAS = 2,
  AN_NEED_ALL_URLS = 4,
  AN_NEED_THUMBNAIL = 8,
  AN_NEED_TO_RUN = 128		// Set by analyser_need() to ensure non-zero output
};

enum an_hook_type {
  AN_HOOK_RAW,
  AN_HOOK_GATHERER,
  AN_HOOK_SCANNER,
  AN_HOOK_CHEWER,
  AN_HOOK_ATEST,
  AN_HOOK_MAX
};

struct an_hook {
  cnode n;
  struct analyser *a;
  struct an_context *c;
  ucw_time_t older_than;		// Older documents are re-analysed; 0=never, ~0=always
  void *priv;			// Private to the analyser
  uns doc_count;
  uns triggered;		// Requested by previous analyser_need()
  uns need_mask;		// A mask of AN_NEED_xxx flags
  char *parameter;		// Parameter from config
  uns initialized;
};

struct an_context {
  clist list;			// Local copy of hooks list
  struct mempool *init_pool;	// Context pool (for list allocation or analysers initialization)
  uns need_mask;		// OR combination of all hooks
  struct an_context *master;
};

struct analyser {
  snode n;			// We have a list of all analysers
  char *name;
  void (*init)(struct an_hook *h);				// Called once per each hook (optional)
  void (*init_context)(struct an_hook *h);			// For each thread (optional)
  void (*cleanup_context)(struct an_hook *h);			// Cleanup context (optional)
  int (*need)(struct an_hook *h, struct an_iface *ai);		// Do we need to re-analyze the object? (optional)
								// Only object attributes available here, no streams
  void (*analyse)(struct an_hook *h, struct an_iface *ai);	// Do the analysis
};

extern uns an_trace;

void analyser_init_hook(enum an_hook_type hook_type);
void analyser_init(struct an_context *c, enum an_hook_type hook_type, uns avail_need_mask, struct an_context *master);
void analyser_cleanup(struct an_context *c);
void analyser_merge_stats(struct an_context *c);
void analyser_log_stats(struct an_context *c);
int analyser_lookup_hook(byte *name);

/* thread-safe */
uns analyser_need(struct an_context *c, struct an_iface *ai);
void analyser_run(struct an_context *c, struct an_iface *ai);
void analyser_run_needed(struct an_context *c, struct an_iface *ai);

/* Analyser modules */

#define AN_MODULE(x) extern struct analyser x;
#include "analyser/a-list.h"
#undef AN_MODULE

/* a-lang.c: Extra functions for language detection */

#ifdef CONFIG_LANG
byte *an_lang_decide_language(struct odes *obj);
#else
static inline byte *an_lang_decide_language(struct odes *obj UNUSED) { return NULL; }
#endif

#endif
