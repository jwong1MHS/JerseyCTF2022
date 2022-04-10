/*
 *	Sherlock Content Analyser -- A Simple Testing Analyser
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "analyser/analyser.h"
#include "ucw/fastbuf.h"

static void
an_test_init(struct an_hook *h)
{
  h->need_mask = AN_NEED_TEXT | AN_NEED_METAS;
}

static int
an_test_need(struct an_hook *h UNUSED, struct an_iface *ai)
{
  return !obj_find_aval(ai->obj, '1');
}

static void
an_test_analyse(struct an_hook *h UNUSED, struct an_iface *ai)
{
  obj_set_attr(ai->obj, '1', "eeek!");
  if (ai->text)
    obj_set_attr_num(ai->obj, '2', bfilesize(ai->text));
  if (ai->metas)
    obj_set_attr_num(ai->obj, '3', bfilesize(ai->metas));
}

struct analyser an_test = {
  .name = "test",
  .init = an_test_init,
  .need = an_test_need,
  .analyse = an_test_analyse
};
