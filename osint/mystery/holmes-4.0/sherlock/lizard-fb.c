/*
 *	LiZaRd -- Reading from and writing to a fastbuf
 *
 *	(c) 2004--2005, Robert Spalek <robert@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#include "sherlock/sherlock.h"
#include "ucw/lizard.h"
#include "ucw/bbuf.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "sherlock/lizard-fb.h"
#include "sherlock/object.h"

#include <errno.h>

static uns liz_type;
static float liz_min_compr;

static bb_t bb_in, bb_out;

void
lizard_set_type(uns type, float min_compr)
{
  liz_type = type;
  liz_min_compr = min_compr;
}

int
lizard_compress_req(struct lizard_block_req *req)
{
  if (req->type == BUCKET_TYPE_V33_LIZARD && req->ratio)
  {
    int est_out = req->in_len * LIZARD_MAX_MULTIPLY + LIZARD_MAX_ADD + 8;	// 8 for uncompressed length and Adler32 checksum
    if (req->out_len < est_out)
    {
      req->out_ptr = NULL;
      req->out_len = est_out;
      return -1;
    }
    req->out_len = lizard_compress(req->in_ptr, req->in_len, req->out_ptr+8)+8;
    if (req->out_len < req->in_len * req->ratio)
    {
      PUT_U32(req->out_ptr, req->in_len);
      PUT_U32(req->out_ptr+4, adler32(req->in_ptr, req->in_len));
      return 1;
    }
  }
  if (req->type == BUCKET_TYPE_V33_LIZARD)
    req->type = BUCKET_TYPE_V33;
  req->out_ptr = req->in_ptr;
  req->out_len = req->in_len;
  return 0;
}

int
lizard_compress_req_static(struct lizard_block_req *req)
{
  int res = lizard_compress_req(req);
  if (res < 0)
  {
    bb_grow(&bb_out, req->out_len + 4+LIZARD_COMPRESS_HEADER);	// maximum header we use is 6
    req->out_ptr = bb_out.ptr + 4+LIZARD_COMPRESS_HEADER;
    res = lizard_compress_req(req);
    ASSERT(res >= 0);
  }
  return res;
}

int
lizard_compress_req_header(struct lizard_block_req *req, uns header_room)
{
  req->out_ptr += LIZARD_COMPRESS_HEADER;
  req->out_len -= LIZARD_COMPRESS_HEADER;
  int outside_buf = lizard_compress_req_static(req);
  if (outside_buf || header_room)
  {
    req->out_ptr -= LIZARD_COMPRESS_HEADER;
    req->out_len += LIZARD_COMPRESS_HEADER;
    req->out_ptr[0] = req->type & 0xff;
    put_attr_separator(req->out_ptr+1);
    return 1;
  }
  return 0;
}

int
lizard_bwrite(struct fastbuf *fb_out, byte *ptr_in, uns len_in)
{
  byte *write_pos;
  int avail_out = bdirect_write_prepare(fb_out, &write_pos);
  struct lizard_block_req req = {
    .type = liz_type,
    .ratio = liz_min_compr,
    .in_ptr = ptr_in,
    .in_len = len_in,
    .out_ptr = write_pos + 4,
    .out_len = avail_out - 4,
  };
  int has_header = lizard_compress_req_header(&req, 0);
  if (req.out_ptr == write_pos + 4)
  {
    ASSERT(has_header);
    PUT_U32(write_pos, req.out_len);
    bdirect_write_commit(fb_out, req.out_ptr + req.out_len);
  }
  else
  {
    if (!has_header)
    {
      bputl(fb_out, req.out_len + LIZARD_COMPRESS_HEADER);
      bputc(fb_out, req.type & 0xff);
      bput_attr_separator(fb_out);
    }
    else
      bputl(fb_out, req.out_len);
    bwrite(fb_out, req.out_ptr, req.out_len);
  }
  return req.type;
}

int
lizard_bbcopy_compress(struct fastbuf *fb_out, struct fastbuf *fb_in, uns len_in)
{
  byte *ptr_in;
  uns i = bdirect_read_prepare(fb_in, &ptr_in);
  if (i < len_in)
  {
    bb_grow(&bb_in, len_in);
    bread(fb_in, bb_in.ptr, len_in);
    ptr_in = bb_in.ptr;
  }
  else
    bdirect_read_commit(fb_in, ptr_in + len_in);
  return lizard_bwrite(fb_out, ptr_in, len_in);
}

static int
decompress(struct lizard_buffer *liz_buf, byte *ptr_in, byte **ptr_out)
{
  uns orig_len = GET_U32(ptr_in);
  uns orig_adler = GET_U32(ptr_in + 4);
  ptr_in += 8;
  *ptr_out = lizard_decompress_safe(ptr_in, liz_buf, orig_len);
  if (!*ptr_out)
    return -1;
  if (adler32(*ptr_out, orig_len) != orig_adler)
  {
    errno = EINVAL;
    return -1;
  }
  return orig_len;
}

int
lizard_memread(struct lizard_buffer *liz_buf, byte *ptr_in, byte **ptr_out, uns *type)
{
  uns stored_len = GET_U32(ptr_in);
  ptr_in += 4;
  *type = ptr_in[0] + BUCKET_TYPE_PLAIN;
  if (*type < BUCKET_TYPE_PLAIN || *type > BUCKET_TYPE_V33_LIZARD)
  {
    errno = EINVAL;
    return -1;
  }
  ASSERT(!ptr_in[1]);	// separator of a bucket header and body
  ptr_in += LIZARD_COMPRESS_HEADER;
  ASSERT(stored_len >= LIZARD_COMPRESS_HEADER);
  stored_len -= LIZARD_COMPRESS_HEADER;
  if (*type == BUCKET_TYPE_V33_LIZARD)
    return decompress(liz_buf, ptr_in, ptr_out);
  else
  {
    *ptr_out = ptr_in;
    return stored_len;
  }
}

int
lizard_bread(struct lizard_buffer *liz_buf, struct fastbuf *fb_in, byte **ptr_out, uns *type)
{
  uns stored_len = bgetl(fb_in);
  *type = bgetc(fb_in) + BUCKET_TYPE_PLAIN;
  if (*type < BUCKET_TYPE_PLAIN || *type > BUCKET_TYPE_V33_LIZARD)
  {
    if (*type == ~0U)			// EOF
      errno = EBADF;
    else
      errno = EINVAL;
    return -1;
  }
  uns sep = bgetc(fb_in);
  ASSERT(!sep);				// separator of a bucket header and body
  ASSERT(stored_len >= LIZARD_COMPRESS_HEADER);
  stored_len -= LIZARD_COMPRESS_HEADER;
  byte *ptr_in;
  uns i = bdirect_read_prepare(fb_in, &ptr_in);
  if (i < stored_len)
  {
    bb_grow(&bb_in, stored_len);
    bread(fb_in, bb_in.ptr, stored_len);
    ptr_in = bb_in.ptr;
  }
  else
    bdirect_read_commit(fb_in, ptr_in + stored_len);
  if (*type == BUCKET_TYPE_V33_LIZARD)
    return decompress(liz_buf, ptr_in, ptr_out);
  else
  {
    *ptr_out = ptr_in;
    return stored_len;
  }
}
