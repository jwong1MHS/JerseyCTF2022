/*
 * 	Sherlock Shepherd Protocol -- FastBuf Emulation
 *
 * 	(c) 2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/protocol.h"

struct shepp_fb {
  struct fastbuf fb;
  struct shepp_packet_hdr *reply_to;
  int eof_p;
  int remaining;
  u32 id;
};
#define FB_SHEP(f) ((struct shepp_fb *)(f)->is_fastbuf)

static void
shepp_fb_spout(struct fastbuf *B)
{
  struct shepp_fb *b = FB_SHEP(B);
  if (B->bstop == B->bptr)
    return;
  struct shepp_packet_hdr hdr, hrp;
  if (b->reply_to)
    shepp_send_raw(&hdr, SHEPP_REPLY_DATA_BLOCK, b->reply_to, B->buffer, B->bptr - B->bstop);
  else
    {
      shepp_send_raw(&hdr, SHEPP_REQ_SEND_DATA_BLOCK, NULL, B->buffer, B->bptr - B->bstop);
      shepp_recv(&hrp, &hdr);
      if (hrp.type != SHEPP_REPLY_OK)
	shepp_err("Unexpected reply %08x to data packet", hrp.type);
    }
  B->bptr = B->bstop = B->buffer;
  B->pos += hdr.data_len;
}

static void
shepp_fb_wclose(struct fastbuf *B)
{
  struct shepp_fb *b = FB_SHEP(B);
  struct shepp_packet_hdr hdr, hrp;
  if (b->reply_to)
    shepp_send_none(&hdr, SHEPP_REPLY_DATA_END, b->reply_to);
  else
    {
      shepp_send_none(&hdr, SHEPP_REQ_SEND_DATA_END, NULL);
      shepp_recv(&hrp, &hdr);
      if (hrp.type != SHEPP_REPLY_OK)
	shepp_err("Unexpected reply %08x to data end packet");
    }
  xfree(b);
}

static void
shepp_fb_reply_ok(struct shepp_fb *b)
{
  if (b->reply_to)
    return;
  struct shepp_packet_hdr h;
  h.id = b->id;
  shepp_send_none(NULL, SHEPP_REPLY_OK, &h);
}

static int
shepp_fb_refill(struct fastbuf *B)
{
  struct shepp_fb *b = FB_SHEP(B);
  struct shepp_packet_hdr h;
  for(;;)
    {
      if (b->eof_p)
	{
	  shepp_fb_reply_ok(b);
	  return 0;
	}
      if (b->remaining)
	{
	  uns l = MIN(b->remaining, B->bufend - B->buffer);
	  shepp_read(B->buffer, l);
	  b->remaining -= l;
	  if (!b->remaining)
	    shepp_fb_reply_ok(b);
	  B->bptr = B->buffer;
	  B->bstop = B->buffer + l;
	  B->pos += l;
	  return 1;
	}
      if (!shepp_recv_hdr(&h, b->reply_to))
	shepp_err("Unexpected EOF when reading data stream");
      if (b->reply_to)
	{
	  if (h.type == SHEPP_REPLY_DATA_BLOCK)
	    b->remaining = h.data_len;
	  else if (h.type == SHEPP_REPLY_DATA_END)
	    b->eof_p = 1;
	  else
	    shepp_err("Unexpected packet type %08x when reading data stream", h.type);
	}
      else
	{
	  if (h.type == SHEPP_REQ_SEND_DATA_BLOCK)
	    b->remaining = h.data_len;
	  else if (h.type == SHEPP_REQ_SEND_DATA_END)
	    b->eof_p = 1;
	  else
	    shepp_err("Unexpected packet type %08x when reading data stream", h.type);
	  b->id = h.id;
	}
    }
}

static void
shepp_fb_rclose(struct fastbuf *B)
{
  struct shepp_fb *b = FB_SHEP(B);
  ASSERT(b->eof_p);
  xfree(b);
}

static struct shepp_fb *
shepp_fb_create(struct shepp_packet_hdr *reply_to)
{
  struct shepp_fb *b = xmalloc(sizeof(struct shepp_fb) + 65536);
  bzero(b, sizeof(struct shepp_fb));
  struct fastbuf *B = &b->fb;
  B->buffer = (byte *)(b+1);
  B->bufend = B->buffer + 65536;
  B->bptr = B->bstop = B->buffer;
  B->name = "Shepherd socket";
  b->reply_to = reply_to;
  return b;
}

struct fastbuf *
shepp_fb_open_write(struct shepp_packet_hdr *reply_to)
{
  struct shepp_fb *b = shepp_fb_create(reply_to);
  b->fb.spout = shepp_fb_spout;
  b->fb.close = shepp_fb_wclose;
  return &b->fb;
}

struct fastbuf *
shepp_fb_open_read(struct shepp_packet_hdr *reply_to)
{
  struct shepp_fb *b = shepp_fb_create(reply_to);
  b->fb.refill = shepp_fb_refill;
  b->fb.close = shepp_fb_rclose;
  b->fb.can_overwrite_buffer = 2;
  return &b->fb;
}
