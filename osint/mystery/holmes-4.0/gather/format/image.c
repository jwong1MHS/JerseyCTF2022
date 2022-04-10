/*
 *	Sherlock Gatherer: Image Parser
 *
 *	(c) 2002 Tomas Holusa <tomas.holusa@netcentrum.cz>
 *	(c) 2002 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "sherlock/tagged-text.h"
#include "ucw/conf.h"
#include "ucw/mempool.h"
#include "ucw/chartype.h"
#include "ucw/stkstring.h"
#include "gather/gather.h"
#include "images/images.h"
#include "images/color.h"

#include <stdlib.h>

/* Config */

static uns maxx = 150;
static uns maxy = 150;
static uns jpeg_quality = 75;
static uns colors_threshold = 16;
static uns min_pixels = 16;
static uns max_pixels = 4000000;
static uns good_name_threshold = ~0;

static char *
images_commit(void *ptr UNUSED)
{
  if (!maxx || !maxx || maxx > 0xffff || maxy > 0xffff)
    return "Invalid image size limits in Images section";
  if (!jpeg_quality || jpeg_quality > 100)
    return "Images.ThumbnailJPEGQualityFactor does not fit in range [1, 100]";
  if (colors_threshold < 2 || colors_threshold > 256)
    return "Images.ThumbnailTypeThresholsColors does not fit in range [2, 256]";
  return NULL;
}

static struct cf_section images_config = {
  CF_COMMIT(images_commit),
  CF_ITEMS {
    CF_UNS("MinSrcImagePixelCount", &min_pixels),
    CF_UNS("MaxSrcImagePixelCount", &max_pixels),
    CF_UNS("ThumbnailMaxWidth", &maxx),
    CF_UNS("ThumbnailMaxHeight", &maxy),
    CF_UNS("ThumbnailJPEGQualityFactor", &jpeg_quality),
    CF_UNS("ThumbnailTypeThresholdColors", &colors_threshold),
    CF_UNS("GoodNameThreshold", &good_name_threshold),
    CF_END
  }
};

static void CONSTRUCTOR images_init(void)
{
  cf_declare_section("Images", &images_config, 0);
}

static void
context_cleanup(struct image_context *ctx, struct image_io *io)
{
  bclose(io->fastbuf);
  image_io_cleanup(io);
  image_context_cleanup(ctx);
}

int
image_parse(char **args UNUSED)
{
  if (gthis->truncated)
    gerror(2205, "Image truncated");

  enum image_format format;
  if (match_ct_patt("image/jpeg", gthis->content_type) || match_ct_patt("image/pjpeg", gthis->content_type))
    format = IMAGE_FORMAT_JPEG;
  else if (match_ct_patt("image/png", gthis->content_type))
    format = IMAGE_FORMAT_PNG;
  else if (match_ct_patt("image/gif", gthis->content_type))
    format = IMAGE_FORMAT_GIF;
  else
    gerror(2200, "Unknown image content type");

  /* Cannot be done in commit hook, because it depends on another section */
  ASSERT(maxx <= image_max_dim && maxy <= image_max_dim);

  /* Initialize libimages */
  struct image_context ctx;
  struct image_io io;
  image_context_init(&ctx);
  ctx.msg_callback = image_context_msg_silent;
  if (!image_io_init(&ctx, &io))
    die("Cannot initialize image I/O (%s)", ctx.msg);

  /* Read image header */
  io.fastbuf = fbmem_clone_read(gthis->contents);
  io.format = format;
  if (!image_io_read_header(&io))
    {
      byte *msg = stk_strdup(ctx.msg);
      context_cleanup(&ctx, &io);
      gerror(2202, "Cannot read image header (%s)", msg);
    }

  /* Process parameters */
  byte *image_type; /* TRUE, GRAY, PAL */
  uns number_of_colors = io.number_of_colors;
  uns area = io.cols * io.rows;
  uns cols = io.cols;
  uns rows = io.rows;
  if (area > max_pixels || area < min_pixels)
    {
      context_cleanup(&ctx, &io);
      gerror(2204, "Original image too big or small (%ux%u)", io.cols, io.rows);
    }
  if (io.flags & IMAGE_IO_HAS_PALETTE)
    image_type = "PAL";
  else if (io.flags & IMAGE_COLOR_SPACE == COLOR_SPACE_GRAYSCALE)
    image_type = "GRAY";
  else
    image_type = "TRUE";

  /* Scale image to thumbnail size */
  image_dimensions_fit_to_box(&io.cols, &io.rows, maxx, maxy, 0);

  /* Remove transparency (use white background if the image contains no background info) */
  if (io.flags & IMAGE_ALPHA)
    {
      io.flags &= ~IMAGE_ALPHA;
      if (!io.background_color.color_space)
        io.background_color = color_white;
      io.flags |= IMAGE_IO_USE_BACKGROUND;
    }

  /* Convert color images to RGB (YCbCr would be probably better for JPEGs, but there is no support yet) */
  if ((io.flags & IMAGE_COLOR_SPACE) != COLOR_SPACE_GRAYSCALE)
    io.flags = (io.flags & ~IMAGE_PIXEL_FORMAT) | COLOR_SPACE_RGB;

  /* Read image pixels */
  if (!image_io_read_data(&io, 0))
    {
      byte *msg = stk_strdup(ctx.msg);
      context_cleanup(&ctx, &io);
      gerror(2002, "Cannot read image data (%s)", msg);
    }
  bclose(io.fastbuf);

  /* Setup write parameters */
  byte *thumbnail_type;
  if (io.flags & IMAGE_IO_HAS_PALETTE && io.number_of_colors <= colors_threshold)
    {
      thumbnail_type = "png";
      io.format = IMAGE_FORMAT_PNG;
    }
  else
    {
      thumbnail_type = "jpeg";
      io.format = IMAGE_FORMAT_JPEG;
      io.jpeg_quality = jpeg_quality;
    }

  /* Write image */
  io.fastbuf = fbmem_create(4096);
  if (!image_io_write(&io))
    {
      byte *msg = stk_strdup(ctx.msg);
      context_cleanup(&ctx, &io);
      gerror(2002, "Cannot write image (%s)", msg);
    }
  gthis->thumbnail = io.fastbuf;

  /* Create 'G' attribute */
  struct image *img = io.image;
  obj_set_attr_format(gthis->aa, 'G', "%u %u %s %u %u %u %s",
    cols, rows, image_type, number_of_colors, img->cols, img->rows, thumbnail_type);

  /* Cleanup libimages */
  image_io_cleanup(&io);
  image_context_cleanup(&ctx);

  return 1;
}

struct gobj_ref *
image_add_ref(byte *src, byte *ww, byte *hh, uns nofollow)
{
  struct gobj_ref *ref;
  int w = ww ? atol(ww) : -1;
  int h = hh ? atol(hh) : -1;

  ref = gobj_add_ref_full('I', src, NULL, NULL);
  if (!ref)
    return NULL;

  if (w >= 0 && h >= 0)
    {
      uns area = w*h;
      if (area < min_pixels || area > max_pixels)
	ref->dont_follow = 1;
    }
  if (nofollow)
    ref->dont_follow = 1;

  return ref;
}

static int
image_good_name(byte *url)
{
  byte *c;
  uns cnt = 0;

  if (!good_name_threshold)
    return 1;
  if (good_name_threshold == ~0U)
    return 0;

  c = url + strlen(url);
  while (c > url && c[-1] != '/')
    c--;
  while (*c && *c != '.')
    {
      if (cnt >= good_name_threshold)
	return 1;
      if (Calpha(*c))
	cnt++;
      else
	cnt = 0;
      c++;
    }
  return 0;
}

void
image_filter_refs(void)
{
  /*
   *  Process all references, find which of them look like images and filter
   *  the ones which are not worth indexing. This includes:
   *
   *  (1) <IMG>'s with no ALT and no reasonable sequences of letters in name.
   *  (2) <A> pointing to image with no reasonable name and no text inside.
   *  (3) <IMG>'s inside <A> whose HREF is also an image (probably a thumbnail).
   */

  struct fastbuf *t;
  struct gobj_ref *r;
  byte *refmap;
  enum {
    RM_IS_IMG = 1,
    RM_IS_IMAGE = 2,
    RM_TEXT_INSIDE = 4,
    RM_ECLIPSED = 8,
    RM_GOOD_NAME = 16
  };
  uns u, i;
  uns refstack[8], refsp, seen_text;

  if (!gthis->ref_count)
    return;
  refmap = alloca(gthis->ref_count);
  bzero(refmap, gthis->ref_count);

  CLIST_WALK(r, gthis->ref_list)
    if (r->id && !r->dont_follow)
      {
	uns f = 0;
	if (r->type == 'I')
	  f |= RM_IS_IMG;
	if (!strncmp(r->content_type, "image/", 6))
	  f |= RM_IS_IMAGE;
	if (f && image_good_name(r->url))
	  f |= RM_GOOD_NAME;
	refmap[r->id] = f;
      }

  t = fbmem_clone_read(gthis->text);
  refsp = 0;
  seen_text = 0;
  while ((u = bget_tagged_char(t)) != ~0U)
    {
      if (u < 0x80000000)
	seen_text |= RM_TEXT_INSIDE;
      else if (u < 0x80010000)
	;
      else if (u < 0x80020000)
	{
	  u &= 0xffff;
	  ASSERT(u < gthis->ref_count);
	  ASSERT(refsp < ARRAY_SIZE(refstack));
	  for (i=0; i<refsp; i++)
	    if (refmap[refstack[i]] & RM_IS_IMAGE)
	      refmap[u] |= RM_ECLIPSED;
	  refstack[refsp++] = u;
	}
      else
	{
	  ASSERT(refsp);
	  u = refstack[--refsp];
	  refmap[u] |= seen_text;
	  seen_text = 0;
	  if (refsp)
	    refmap[refstack[refsp-1]] |= (refmap[u] & RM_TEXT_INSIDE);
	}
    }
  bclose(t);
  ASSERT(!refsp);

  CLIST_WALK(r, gthis->ref_list)
    if (r->id && !r->dont_follow)
      {
	uns f = refmap[r->id];
	if ((f & (RM_IS_IMAGE | RM_IS_IMG)) && !(f & RM_TEXT_INSIDE) && !(f & RM_GOOD_NAME) ||
	    (f & RM_IS_IMG) && (f & RM_ECLIPSED))
	  r->dont_follow = 1;
	DBG("[%d] %c %s: %02x -> %d", r->id, r->type, r->url, f, r->dont_follow);
      }
}
