/*
 *	Sherlock Analyser -- Language Detection
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "analyser/analyser.h"
#include "lang/lang.h"
#include "lang/detect.h"

#include <string.h>

static void
an_lang_init(struct an_hook *h)
{
  h->need_mask = AN_NEED_TEXT | AN_NEED_METAS;
  lang_detect_load_tables();
}

static void
an_lang_init_context(struct an_hook *h)
{
  h->priv = lang_detect_alloc(h->c->init_pool);
}

static int
an_lang_need(struct an_hook *h UNUSED, struct an_iface *ai)
{
  switch (lang_detect_mode)
    {
    case 0:
      return 0;
    case 1:
      {
	byte *l = obj_find_aval(ai->obj, 'l');
	if (l && lang_list_to_set(l))
	  return 0;
	/* fall-thru */
      }
    case 2:
      return !obj_find_aval(ai->obj, 'K');
    default:
      die("Unknown LangDetect.Mode %d", lang_detect_mode);
    }
}

static void
an_lang_analyse(struct an_hook *h, struct an_iface *ai)
{
  struct lang_detect *ld = h->priv;
  if (ai->text || ai->metas)
    {
      lang_detect_start(ld);
      if (ai->text)
	lang_detect_add_fastbuf(ld, ai->text);
      if (ai->metas)
	lang_detect_add_fastbuf(ld, ai->metas);
      uns lang = lang_detect_choose_best(ld);
      obj_set_attr(ai->obj, 'K', lang_code_to_name(lang));
    }
  else
    obj_set_attr(ai->obj, 'K', NULL);
}

struct analyser an_lang = {
  .name = "lang",
  .init = an_lang_init,
  .init_context = an_lang_init_context,
  .need = an_lang_need,
  .analyse = an_lang_analyse
};

byte *
an_lang_decide_language(struct odes *obj)
{
  byte *doc = obj_find_aval(obj, 'l');
  byte *det = obj_find_aval(obj, 'K');
  int ldoc, ldet, id;

  ldoc = (doc ? lang_primary_language(doc) : -1);
  if (!det)
    ldet = -1;
  else if (!strcmp(det, "?"))
    ldet = LANG_UNKNOWN;
  else
    ldet = lang_primary_language(det);

  switch (lang_detect_mode)
    {
    case 0:
      id = ldoc;
      break;
    case 1:
      id = (ldoc >= 0) ? ldoc : ldet;
      break;
    case 2:
      id = ldet;
      break;
    default:
      die("LangDetect.Mode set to an invalid value %d", lang_detect_mode);
    }

  return (id < 0) ? (byte *)"?" : lang_code_to_name(id);
}
