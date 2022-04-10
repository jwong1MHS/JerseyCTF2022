/*
 *	OGG Parser - Vorbis Codec
 *
 * 	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/mempool.h"
#include "ucw/unaligned.h"
#include "ucw/fastbuf.h"
#include "ucw/unicode.h"
#include "ucw/ff-unicode.h"
#include "gather/format/ogg/ogg.h"
#include "gather/format/ogg/ogg-vorbis.h"

#include <string.h>
#include <limits.h>

#define STRACE(s, l, ...) ogg_stream_msg((s), L_DEBUG | ((l) << 8), __VA_ARGS__)

/*** Decoding audio packet ***/

static void
ogg_vorbis_packet_audio(struct ogg_stream *s, byte *packet, uns len)
{
  struct ogg_file *f = s->file;
  struct ogg_page *p = &f->page;
  struct ogg_vorbis_stream *v = s->user;
  if (!len || (packet[0] & 1))
    ogg_packet_error(s, "Corrupted audio packet");
  if (!~p->granule)
    ogg_packet_error(s, "Invalid granule position");
  if (!~v->end_sample || v->end_sample < p->granule)
    v->end_sample = p->granule;
}

/*** Decoding setup header (third packet) ***/

static void
ogg_vorbis_packet_setup(struct ogg_stream *s, byte *packet, uns len)
{
  STRACE(s, 1, "Scanning Vorbis setup header");
  if (len < 7 || memcmp(packet, "\x05vorbis", 7))
    ogg_stream_error(s, "Corrupted setup header");
  s->packet = ogg_vorbis_packet_audio;
}

/*** Decoding comment header (second packet) ***/

static byte *
ogg_vorbis_read_str(struct ogg_stream *s, byte **p, byte *end)
{
  if (4 > (uns)(end - *p))
trunc:
    ogg_stream_error(s, "Truncated comment header");
  uns len = get_u32_le(*p);
  *p += 4;
  if (len > (uns)(end - *p))
    goto trunc;
  struct fastbuf in;
  fbbuf_init_read(&in, *p, len, 0);
  *p += len;
  byte *r = mp_start_noalign(s->pool, 1);
  int u;
  while ((u = bget_utf8_32(&in)) >= 0)
    {
      u = unicode_sanitize_char(u);
      r = utf8_put(mp_spread(s->pool, r, 4), u);
    }
  *r++ = 0;
  return mp_end(s->pool, r);
}

static void
ogg_vorbis_packet_comment(struct ogg_stream *s, byte *packet, uns len)
{
  STRACE(s, 1, "Scanning Vorbis comment header");
  struct ogg_vorbis_stream *v = s->user;

  if (len < 7 || memcmp(packet, "\x03vorbis", 7))
    ogg_stream_error(s, "Corrupted comment header");
  byte *p = packet + 7, *end = packet + len, *id, *val;

  v->vendor = ogg_vorbis_read_str(s, &p, end);
  STRACE(s, 2, "Vendor `%s'", v->vendor);

  if (4 > (uns)(end - p))
    goto trunc;
  uns n = get_u32_le(p);
  p += 4;
  if (n > UINT_MAX / sizeof(*v->comments))
    {
      ogg_stream_msg(s, L_WARN_R, "Too many comments");
      goto end;
    }
  v->comments = mp_alloc_zero(s->pool, n * sizeof(*v->comments));
  for (uns i = 0; i < n; i++)
    {
      if (!(id = ogg_vorbis_read_str(s, &p, end)))
	goto end;
      val = strchr(id, '=');
      if (!val)
	ogg_stream_error(s, "Invalid comment format");
      for (byte *x = id; x != val; x++)
	if (*x < 0x20 || *x > 0x7d)
	  ogg_stream_error(s, "Invalid comment encoding");
      *val++ = 0;
      v->comments[i].name = id;
      v->comments[i].value = val;
      STRACE(s, 2, "Comment `%s'=`%s'", id, val);
    }
  v->comment_count = n;
  if (p == end)
    goto trunc;
  if (*p++ != 1 || p != end)
    ogg_stream_error(s, "Corrupted comment header");
end:
  s->packet = ogg_vorbis_packet_setup;
  v->flags |= OGGS_VORBIS_COMMENT_HEADER_VALID;
  return;
trunc:
  ogg_stream_error(s, "Truncated comment header");
}

/*** Decoding identification header (first packet) ***/

static void
ogg_vorbis_end(struct ogg_stream *s)
{
  if (s->packet_num < 3)
    ogg_stream_error(s, "Missing Vorbis headers");
}

static void
ogg_vorbis_start(struct ogg_stream *s, byte *packet, uns len)
{
  STRACE(s, 1, "Scanning Vorbis identification header");
  struct ogg_vorbis_stream *v = s->user = mp_alloc_zero(s->pool, sizeof(*v));
  v->stream = s;
  s->end = ogg_vorbis_end;
  s->packet = ogg_vorbis_packet_comment;

  v->start_sample = ~0ULL;
  v->end_sample = ~0ULL;

  byte *p = packet;
  uns i;
  if (len != 30 || memcmp(p, "\x01vorbis", 7))
    ogg_stream_error(s, "Corrupted identification header");
  if ((i = get_u32_le(p + 7)) != 0)
    ogg_stream_error(s, "Unsupported version (0x%08x)", i);
  v->audio_channels = p[11];
  if (!v->audio_channels)
    ogg_stream_error(s, "No channels");
  v->audio_sample_rate = get_u32_le(p + 12);
  if (!v->audio_sample_rate)
    ogg_stream_error(s, "Zero sample rate");
  v->bitrate_maximum = (s32)get_u32_le(p + 16);
  v->bitrate_nominal = (s32)get_u32_le(p + 20);
  v->bitrate_minimum = (s32)get_u32_le(p + 24);
  v->blocksize_0_log = p[28] & 15;
  v->blocksize_1_log = p[28] >> 4;
  if (v->blocksize_0_log > v->blocksize_1_log || v->blocksize_0_log < 6 || v->blocksize_1_log > 13)
    ogg_stream_error(s, "Invalid block sizes %u/%u", v->blocksize_0_log, v->blocksize_1_log);
  v->blocksize_0 = 1 << v->blocksize_0_log;
  v->blocksize_1 = 1 << v->blocksize_1_log;
  STRACE(s, 2, "Vorbis channels=%u sample=%u bitrate=%d/%d/%d blocksize=%u/%u", v->audio_channels, v->audio_sample_rate,
      v->bitrate_maximum, v->bitrate_nominal, v->bitrate_minimum, v->blocksize_0, v->blocksize_1);
  v->flags |= OGGS_VORBIS_ID_HEADER_VALID;
}

/*** Vorbis codec declaration ***/

static uns
ogg_vorbis_detect(byte *packet, uns len)
{
  return (len >= 7) && !memcmp(packet, "\x01vorbis", 7);
}

struct ogg_codec ogg_vorbis_codec = {
  .name = "Vorbis",
  .type = OGGS_TYPE_AUDIO,
  .detect = ogg_vorbis_detect,
  .start = ogg_vorbis_start,
};

/*** OGG Vorbis physical stream ***/

static void
ogg_vorbis_before_stream_start(struct ogg_stream *s)
{
  struct ogg_file *f = s->file;
  if (s->codec != &ogg_vorbis_codec)
    ogg_error(f, "Only Vorbis streams allowed in OGG Vorbis file");
  struct ogg_stream *prev_s = (struct ogg_stream *)clist_prev(&f->streams, &s->node);
  if (prev_s && !(prev_s->flags & OGGS_CLOSED))
    ogg_error(f, "OGG Vorbis specification does not allow multiplexed streams");
}

void
ogg_vorbis_scan_file(struct ogg_file *f)
{
  ogg_vorbis_scan_head(f);
  ogg_scan(f);
}

void
ogg_vorbis_scan_head(struct ogg_file *f)
{
  f->before_stream_start = ogg_vorbis_before_stream_start;
  if (!ogg_scan_page(f))
    ogg_error(f, "Empty OGG file");
  struct ogg_stream *s = f->stream;
  struct ogg_vorbis_stream *v = s->user;
  while (s->packet_num < 3)
    {
      if (!ogg_read_page(f) || (s->flags & OGGS_CLOSED))
	ogg_error(f, "Incomplete OGG header");
      if (f->page.serial != s->serial)
	ogg_error(f, "OGG Vorbis specification does not allow multiplexed streams");
      ogg_decode_page(f);

    }
  if (s->packet_num > 3)
    ogg_error(f, "First audio packet must start on a fresh page");
  while (s->packet_num == 3)
    if (!ogg_scan_page(f))
      ogg_error(f, "Missing audio packets");
  s64 g = f->page.granule;
  g -= (s->packet_num - 3) * (u64)(v->blocksize_1 / 2);
  g = MAX(g, 0);
  v->start_sample = g;
}

void
ogg_vorbis_scan_tail(struct ogg_file *f)
{
  struct ogg_page *p = &f->page;
  struct ogg_vorbis_stream *v = f->stream->user;
  if (bpeekc(f->fb) < 0)
    return;
  ogg_seek_last_page(f);
  if (p->serial != f->stream->serial)
    return;
  ogg_decode_page_header(f);
  if (~p->granule && (!~v->end_sample || v->end_sample < p->granule))
    v->end_sample = p->granule;
  v->end_sample = f->page.granule;
}
