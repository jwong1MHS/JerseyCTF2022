/*
 *	Sherlock Gatherer -- Interface to Analyser
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "ucw/mempool.h"
#include "analyser/analyser.h"
#include "gather/gather.h"

static struct an_context an_raw_context;
static struct an_context an_gather_context;

void
gather_init_analyser(void)
{
  analyser_init_hook(AN_HOOK_RAW);
  analyser_init(&an_raw_context, AN_HOOK_RAW, AN_NEED_TEXT | AN_NEED_ALL_URLS, NULL);

  analyser_init_hook(AN_HOOK_GATHERER);
  analyser_init(&an_gather_context, AN_HOOK_GATHERER, AN_NEED_TEXT | AN_NEED_METAS | AN_NEED_ALL_URLS | AN_NEED_THUMBNAIL, NULL);
}

/* AN_HOOK_RAW */

void
gather_raw_analyse(void)
{
  struct odes *o = gthis->aa;
  struct an_iface ai = {
    .obj = o,
    .url_block = o,
    .all_urls = (struct odes *[]) { o, NULL },
    .pool = gthis->pool,
  };

  /*
   * We need to temporarily create object attributes for stuff stored
   * in the gobject itself, because they might be needed by the analysers.
   * This is somewhat nasty, but hard to avoid without redesigning libgather.
   */
  obj_set_attr(o, 'U', gthis->url);
  obj_set_attr(o, 'T', gthis->content_type);

  uns need = analyser_need(&an_raw_context, &ai);
  if (need)
    {
      if ((need & AN_NEED_TEXT) && gthis->contents)
	ai.text = fbmem_clone_read(gthis->contents);
      analyser_run_needed(&an_raw_context, &ai);
      bclose(ai.text);
    }

  /* Remove the temporary object attributes */
  obj_set_attr(o, 'U', NULL);
  obj_set_attr(o, 'T', NULL);

  /* And re-run the gatherer filter, because we might want to filter on analyser results */
  gather_filter(0);
}

/* AN_HOOK_GATHERER */

int
gather_analysis_needed(void)
{
  ASSERT(gthis->refreshing);
  struct an_iface ai = {
    .obj = gthis->refreshing,
    .url_block = gthis->refreshing,
    .all_urls = (struct odes *[]) { gthis->refreshing, NULL },
    .pool = gthis->pool
  };
  return analyser_need(&an_gather_context, &ai);
}

void
gather_analyse(void)
{
  /* Do not analyse redirects */
  if (gthis->error_code == 1)
    return;

  struct odes *o = gthis->aa;
  struct an_iface ai = {
    .obj = o,
    .url_block = o,
    .all_urls = (struct odes *[]) { o, NULL },
    .pool = gthis->pool,
  };

  /*
   * We need to temporarily create object attributes for stuff stored
   * in the gobject itself, because they might be needed by the analysers.
   * This is somewhat nasty, but hard to avoid without redesigning libgather.
   */
  obj_set_attr(o, 'U', gthis->url);
  obj_set_attr(o, 'T', gthis->content_type);
  obj_set_attr(o, 'l', gthis->language);

  uns need = analyser_need(&an_gather_context, &ai);
  if (need)
    {
      if ((need & AN_NEED_TEXT) && gthis->text)
	ai.text = fbmem_clone_read(gthis->text);
      if ((need & AN_NEED_METAS) && gthis->meta)
	ai.metas = fbmem_clone_read(gthis->meta);
      if ((need & AN_NEED_THUMBNAIL) && gthis->thumbnail)
	ai.thumbnail = fbmem_clone_read(gthis->thumbnail);
      analyser_run_needed(&an_gather_context, &ai);
      bclose(ai.text);
      bclose(ai.metas);
    }

  /* Remove the temporary object attributes */
  obj_set_attr(o, 'U', NULL);
  obj_set_attr(o, 'T', NULL);
  obj_set_attr(o, 'l', NULL);

  /* And re-run the gatherer filter, because we might want to filter on analyser results */
  gather_filter(1);
}
