/*
 *	Sherlock Gatherer -- Dumping Objects to Buckets
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2004--2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/base224.h"
#include "ucw/lizard.h"
#include "ucw/ff-unicode.h"
#include "ucw/string.h"
#include "sherlock/index.h"
#include "sherlock/tagged-text.h"
#include "sherlock/lizard-fb.h"
#include "gather/gather.h"

#include <string.h>
#include <sys/time.h>
#include <sys/types.h>

static inline void
gobj_write_str(struct fastbuf *b, int type, byte *s)
{
  if (!s)
    return;
  bput_attr_str(b, type, s);
}

static inline void
gobj_write_num(struct fastbuf *b, int type, unsigned int n)
{
  if (!n)
    return;
  bput_attr_num(b, type, n);
}

static void
gobj_write_stream(struct fastbuf *b, int type, struct fastbuf *f)
{
  byte buf[520];
  int w = 0;
  int c;

  if (!f)
    return;
  f = fbmem_clone_read(f);
  while ((c = bgetc(f)) >= 0)
    {
      if (w > 256 && (c <= ' ' || (c >= 0x80 && c < 0xa0)) || w > 512)
	{
	  bput_attr(b, type, buf, w);
	  w = 0;
	  if (c <= ' ')
	    continue;
	}
      if (c != '\n')
	buf[w++] = c;
      else
      {
	bput_attr(b, type, buf, w);
	w = 0;
      }
      if (c >= 0xc0)
	{
	  /* Copy the whole UTF-8 character to avoid line breaks inside */
	  while (c & 0x40)
	    {
	      buf[w++] = bgetc(f);
	      c <<= 1;
	    }
	}
      else if (c >= 0xa0 && c < 0xb0)
	{
	  c = bgetc(f);
	  ASSERT(c >= 0x80);
	  buf[w++] = c;
	}
    }
  bput_attr(b, type, buf, w);
  bclose(f);
}

static void
gobj_write_base224_stream(struct fastbuf *b, int type, struct fastbuf *f)
{
  byte ib[BASE224_IN_CHUNK*6], ob[BASE224_OUT_CHUNK*6];
  uns l;

  if (!f)
    return;
  f = fbmem_clone_read(f);
  while (l = bread(f, ib, sizeof(ib)))
  {
    l = base224_encode(ob, ib, l);
    bput_attr(b, type, ob, l);
  }
  bclose(f);
}

static void
gobj_write_sum(struct fastbuf *b)
{
  if (!gthis->MD5_valid)
    return;

  char sum[MD5_HEX_SIZE];
  mem_to_hex(sum, gthis->MD5, MD5_SIZE, MEM_TO_HEX_UPCASE);
  bput_attr_str(b, 'C', sum);
}

static void
gobj_write_ref(struct fastbuf *b, struct gobj_ref *ref)
{
  bput_attr_format(b, ref->type, "%s %d%s", ref->url, ref->id,
    (ref->dont_follow || gthis->dont_follow_links) ? " 1" : "");
}

static void
gobj_write_meta_stream(struct fastbuf *b, struct fastbuf *f)
{
  if (!f)
    return;
  byte buf[520];
  f = fbmem_clone_read(f);
  uns c, len = 0, cropped = 0;
  while ((c = bget_tagged_char(f)) != ~0U)
    {
      if (c >= 0x80000000)
	{
	  ASSERT(c >= 0x80000090 && c < 0x80010000);
	  if (len)
	    bput_attr(b, 'M', buf, len);
	  buf[0] = c;
	  len = 1;
	  cropped = 0;
	}
      else
	{
	  ASSERT(len);
	  if (len > 512 || cropped)
	    continue;
	  if (c <= ' ')
	    {
	      if (len > 256)
		{
		  cropped = 1;
		  continue;
		}
	      c = ' ';
	    }
	  byte *ptr = buf + len;
	  ptr = utf8_put(ptr, c);
	  len = ptr - buf;
	}
    }
  if (len)
    bput_attr(b, 'M', buf, len);
  bclose(f);
}

static void
gobj_dump(struct fastbuf *b_head, struct fastbuf *b, uns bucket_type, uns flags)
{
  put_attr_set_type(bucket_type);

  gobj_write_str(b_head, 'U', gthis->url);
  bput_attr_separator(b_head);

#if defined(WT_RESERVED) || defined(ST_RESERVED)
#error You must remove WT_RESERVED and ST_RESERVED
#endif
  gobj_write_str(b, 'v', BUCKET_VERSION);
  gobj_write_num(b, 'D', gthis->start_time);
  gobj_write_num(b, 'L', gthis->lastmod_time);
  gobj_write_num(b, 'e', gthis->expires_time);
  gobj_write_str(b, 'E', gthis->content_encoding);
  gobj_write_str(b, 'T', gthis->content_type);
  gobj_write_str(b, 'S', gthis->http_server);
  gobj_write_str(b, 'g', gthis->etag);
  gobj_write_str(b, 'l', gthis->language);
  gobj_write_num(b, 's', gthis->orig_size);
  struct timeval tv;
  if (gettimeofday(&tv, NULL) < 0)
    die("gettimeofday failed: %m");
  gobj_write_num(b, 'h', 1000*(tv.tv_sec - gthis->start_time) + ((int)(tv.tv_usec - gthis->start_time_us)) / 1000);
  if (gthis->truncated)
    gobj_write_str(b, '.', "Truncated");
  gobj_write_sum(b);
  CLIST_FOR_EACH(struct gobj_ref *, ref, gthis->ref_list)
    gobj_write_ref(b, ref);
  bput_object(b, gthis->aa);
  gobj_write_meta_stream(b, gthis->meta);
  if ((flags & GWF_DUMP_SOURCE) && gthis->content_type && !strncasecmp(gthis->content_type, "text/", 5))
    gobj_write_stream(b, 'Z', gthis->contents);
  if ((flags & GWF_DUMP_BODY) && !gthis->dont_save_contents)
    {
      gobj_write_stream(b, 'X', gthis->text);
      gobj_write_base224_stream(b, 'N', gthis->thumbnail);
    }
  bput_attr_format(b, '!', "%04d %s", gthis->error_code, gthis->error_msg);
}

static uns
safe_strlen(char *c)
{
  return c ? strlen(c) : 0;
}

static uns
gobj_stream_size(struct fastbuf *b)
{
  /*
   *  Trick: bfilesize() cannot be used there since the streams are write parts of fbmem's
   *  which are not seekable. However, we can be sure that they are positioned just after
   *  the last byte written, hence btell() does the job.
   */
  return (b ? btell(b) : 0);
}

static uns
gobj_estimate_length(uns flags)
{
  uns l = strlen(gthis->url) + safe_strlen(gthis->http_server) + safe_strlen(gthis->etag) + 4096;
  CLIST_FOR_EACH(struct gobj_ref *, ref, gthis->ref_list)
    l += strlen(ref->url) + 16;
  for(struct oattr *a=gthis->aa->attrs; a; a=a->next)
    for(struct oattr *b=a; b; b=b->same)
      l += strlen(b->val) + 16;
  l += gobj_stream_size(gthis->meta) * 1.1;
  if (flags & GWF_DUMP_SOURCE)
    l += gobj_stream_size(gthis->contents) * 1.1;
  if (flags & GWF_DUMP_BODY)
    l += gobj_stream_size(gthis->text) * 1.1 + gobj_stream_size(gthis->thumbnail) * 1.2;
  return l;
}

static uns
gobj_write_compressed(struct fastbuf *b, uns bucket_type, uns flags)
{
  if (gthis->thumbnail)				// do not try to compress images
  {
    bucket_type = BUCKET_TYPE_V33;
    gobj_dump(b, b, bucket_type, flags);
    return bucket_type;
  }

  uns len_in, block_in, est_len;
  byte *buf_in;
  struct fastbuf *w_body, *b_body;

  est_len = gobj_estimate_length(flags);
  w_body = fbmem_create(est_len);		// should be long enough to fit in just 1 block, however not necessary

  gobj_dump(b, w_body, BUCKET_TYPE_V33, flags);

  bflush(w_body);
  len_in = btell(w_body);
  b_body = fbmem_clone_read(w_body);
  bclose(w_body);

  block_in = bdirect_read_prepare(b_body, &buf_in);
  if (block_in < len_in)			// cannot use zero-copy input
    {
      log(L_WARN, "gobj_write_compressed: Wrong estimate of body size: %d > %d on URL %s, cannot zero-copy", len_in, block_in, gthis->url);
      buf_in = xmalloc(len_in);
      bread(b_body, buf_in, len_in);
    }

  byte *write_pos;
  int avail_out = bdirect_write_prepare(b, &write_pos);
  struct lizard_block_req req = {
    .type = BUCKET_TYPE_V33_LIZARD,
    .ratio = gather_min_compression / 100.,
    .in_ptr = buf_in,
    .in_len = len_in,
    .out_ptr = write_pos,
    .out_len = avail_out,
  };
  lizard_compress_req_static(&req);
  if (req.out_ptr == write_pos)
    bdirect_write_commit(b, req.out_ptr + req.out_len);
  else
    bwrite(b, req.out_ptr, req.out_len);

  if (block_in < len_in)
    xfree(buf_in);
  bclose(b_body);				// bdirect_read_commit() not needed

  return req.type;
}

uns
gobj_write(struct fastbuf *b, uns bucket_type, uns flags)
{
  ASSERT(b);
  if (bucket_type == BUCKET_TYPE_V33_LIZARD && gather_min_compression)
    return gobj_write_compressed(b, bucket_type, flags);
  else
    {
      if (bucket_type == BUCKET_TYPE_V33_LIZARD)
	bucket_type = BUCKET_TYPE_V33;
      gobj_dump(b, b, bucket_type, flags);
      return bucket_type;
    }
}
