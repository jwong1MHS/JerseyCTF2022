/*
 *	Sherlock ImageSig Analyser -- Image signatures
 *
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "ucw/mempool.h"
#include "analyser/analyser.h"
#include "images/images.h"
#include "images/object.h"
#include "images/color.h"
#include "images/signature.h"

struct context {
  struct image_context ctx;
  struct image_io io;
};

static void
an_imagesig_init(struct an_hook *h)
{
  DBG("an_imagesig_init()");
  h->need_mask = AN_NEED_THUMBNAIL;
  srgb_to_luv_init();
}

static void
an_imagesig_init_context(struct an_hook *h)
{
  DBG("an_imagesig_init_context()");
  struct context *c = h->priv = mp_alloc(h->c->init_pool, sizeof(*c));
  image_context_init(&c->ctx);
  if (!image_io_init(&c->ctx, &c->io))
    die("Cannot initialize imagesig analyser");
}

static void
an_imagesig_cleanup_context(struct an_hook *h)
{
  DBG("an_imagesig_cleanup_context()");
  struct context *c = h->priv;
  image_io_cleanup(&c->io);
  image_context_cleanup(&c->ctx);
}

static int
an_imagesig_need(struct an_hook *h UNUSED, struct an_iface *ai)
{
  return obj_find_attr(ai->obj, 'G') && !obj_find_attr(ai->obj, 'H');
}

static void
an_imagesig_analyse(struct an_hook *h, struct an_iface *ai)
{
  struct odes *o = ai->obj;
  struct context *c = h->priv;
  struct image_obj_info ioi;
  struct image *img;
  struct image_signature sig;
  if (!get_image_obj_info(&ioi, o))
    return;
  if (!ai->thumbnail)
    return;
  if (!(img = read_image_obj_thumb(&ioi, ai->thumbnail, &c->io, ai->pool)))
    goto exit;
  if (!compute_image_signature(&c->ctx, &sig, img))
    goto exit;
  sig.cols = ioi.cols;
  sig.rows = ioi.rows;
  put_image_obj_signature(o, &sig);
  DBG("Written signature attribute `%s'", obj_find_aval(o, 'H'));
exit:
  image_io_reset(&c->io);
}

struct analyser an_imagesig = {
  .name = "imagesig",
  .init = an_imagesig_init,
  .init_context = an_imagesig_init_context,
  .cleanup_context = an_imagesig_cleanup_context,
  .need = an_imagesig_need,
  .analyse = an_imagesig_analyse,
};
