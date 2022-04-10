/*
 *	OGG Parser - Decoding of OGG Streams
 *
 * 	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#define LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/fastbuf.h"
#include "ucw/unaligned.h"
#include "ucw/conf.h"
#include "gather/format/ogg/ogg.h"
#include "gather/format/ogg/ogg-vorbis.h"
#include "gather/format/ogg/ogg-codecs.h"

/*** CONFIGURATION ***/

clist ogg_codecs;
uns ogg_trace;
uns ogg_warnings;
uns ogg_max_warnings = ~0U;
uns ogg_max_streams = ~0U;
uns ogg_max_packet_size = ~0U / 2;
uns ogg_max_seek_size = 2 * OGG_MAX_PAGE_SIZE;
uns ogg_max_seek_checks = ~0U;

static u32 ogg_crc_tab[256];

static struct cf_section ogg_config = {
  CF_ITEMS{
    CF_UNS("Trace", &ogg_trace),
    CF_UNS("Warnings", &ogg_warnings),
    CF_UNS("MaxWarnings", &ogg_max_warnings),
    CF_UNS("MaxStreams", &ogg_max_streams),
    CF_UNS("MaxPacketSize", &ogg_max_packet_size),
    CF_UNS("MaxSeekInterval", &ogg_max_seek_size),
    CF_UNS("MaxSeekChecks", &ogg_max_seek_checks),
    CF_END
  }
};

static void CONSTRUCTOR
ogg_init_config(void)
{
  cf_declare_section("OGG", &ogg_config, 0);

  /* Initialize known codecs */
  clist_init(&ogg_codecs);
  clist_add_tail(&ogg_codecs, &ogg_vorbis_codec.node);
  clist_add_tail(&ogg_codecs, &ogg_speex_codec.node);
  clist_add_tail(&ogg_codecs, &ogg_flac_codec.node);
  clist_add_tail(&ogg_codecs, &ogg_theora_codec.node);
  clist_add_tail(&ogg_codecs, &ogg_writ_codec.node);

  /* Compute CCITT-32 table (for CRC checks) */
  for (uns i = 0; i < 256; i++)
    {
      uns crc = i << 24;
      for (uns j = 0; j < 8; j++)
	crc = (crc & 0x80000000) ? (crc << 1) ^ 0x4c11db7 : (crc << 1);
      ogg_crc_tab[i]= crc & 0xffffffff;
    }
}

/*** ERROR/MESSAGE HANDLING ***/

#define TRACE(f, l, ...) ogg_msg((f), L_DEBUG | ((l) << 8), __VA_ARGS__)
#define STRACE(s, l, ...) ogg_stream_msg((s), L_DEBUG | ((l) << 8), __VA_ARGS__)

void NONRET
ogg_throw(struct ogg_file *f, uns code)
{
  if (f->throw_buf)
    longjmp(*f->throw_buf, code);
  die("OGG parser: Uncatched exception");
}

static void
ogg_msg_default(struct ogg_file *f UNUSED, uns type, byte *m)
{
  log(type & 0x7f, "OGG parser: %s", m);
}

void NONRET
ogg_prefix_error(struct ogg_file *f, uns code, void (*prefix)(struct ogg_file *f), char *msg, ...)
{
  if (f->msg_level)
    {
      va_list args;
      va_start(args, msg);
      if (prefix)
        {
	  prefix(f);
	  bb_vprintf_at(&f->msg_buf, strlen(f->msg_buf.ptr), msg, args);
	}
      else
	bb_vprintf(&f->msg_buf, msg, args);
      f->msg(f, L_ERROR_R, f->msg_buf.ptr);
      va_end(args);
    }
  ogg_throw(f, code);
}

void
ogg_prefix_msg(struct ogg_file *f, uns type, void (*prefix)(struct ogg_file *f), char *msg, ...)
{
  if (type < f->msg_level)
    {
      va_list args;
      va_start(args, msg);
      if (prefix)
        {
	  prefix(f);
	  bb_vprintf_at(&f->msg_buf, strlen(f->msg_buf.ptr), msg, args);
	}
      else
	bb_vprintf(&f->msg_buf, msg, args);
      f->msg(f, type, f->msg_buf.ptr);
      va_end(args);
    }
}

static void
ogg_page_msg_prefix(struct ogg_file *f)
{
  bb_printf(&f->msg_buf, "Page 0x%llx: ", (unsigned long long int)f->page.pos);
}

void
ogg_stream_msg_prefix(struct ogg_file *f)
{
  bb_printf(&f->msg_buf, "Stream 0x%08x: ", f->page.serial);
}

void
ogg_packet_msg_prefix(struct ogg_file *f)
{
  ogg_stream_msg_prefix(f);
}

/*** BITSTREAMS ***/

#define HASH_PREFIX(x) ogg_stream_hash_##x
#define HASH_NODE struct ogg_stream
#define HASH_KEY_ATOMIC serial
#define HASH_ATOMIC_TYPE uns
#define HASH_TABLE_DYNAMIC
#define HASH_WANT_CLEANUP
#define HASH_WANT_FIND
#define HASH_WANT_NEW
#include "ucw/hashtable.h"

static void
ogg_stream_cleanup(struct ogg_stream *s)
{
  if (s->flags & OGGS_CLEANED)
    return;
  if (s->cleanup)
    s->cleanup(s);
  bb_done(&s->buf);
  mp_delete(s->pool);
  s->flags |= OGGS_CLEANED;
}

static void
ogg_stream_packet_first(struct ogg_stream *s, byte *packet, uns len)
{
  struct ogg_file *f = s->file;
  CLIST_FOR_EACH(struct ogg_codec *, c, ogg_codecs)
    if (c->detect && c->detect(packet, len))
      {
	s->codec = c;
	f->stream_types |= c->type;
	if (c->type & f->forbidden_stream_types)
	  ogg_error(f, "Found forbidden stream type 0x%x", c->type & f->forbidden_stream_types);
	if (c->type & f->skip_stream_types)
	  {
	    ogg_stream_msg(s, L_DEBUG, "Skipping stream type 0x%x", c->type & f->skip_stream_types);
	    ogg_throw(f, OGG_ERR_STREAM_FATAL);
	  }
	if (!c->start)
	  ogg_stream_error(s, "No support for %s codec", c->name);
        STRACE(s, 0, "Detected %s codec", c->name);
	if (f->before_stream_start)
	  f->before_stream_start(s);
        c->start(s, packet, len);
	return;
      }
  f->stream_types |= OGGS_TYPE_UNDEF;
  if (OGGS_TYPE_UNDEF & f->forbidden_stream_types)
    ogg_error(f, "Cannot detect stream format");
  else if (OGGS_TYPE_UNDEF & f->skip_stream_types)
    {
      ogg_stream_msg(s, L_WARN_R, "Cannot detect stream format");
      ogg_throw(f, OGG_ERR_STREAM_FATAL);
    }
  else
    ogg_stream_error(s, "Cannot detect stream format");
}

/*** PAGES ***/

#define ogg_page_error(f, ...) ogg_prefix_error((f), OGG_ERR_PAGE_FORMAT, ogg_page_msg_prefix, __VA_ARGS__)

uns
ogg_read_page(struct ogg_file *f)
{
  struct ogg_page *p = &f->page;
  struct fastbuf *fb = f->fb;
  p->pos = btell(f->fb);
  uns c = bread(fb, p->hdr, 27);
  if (!c)
    return 0;
  if (c < 27)
    goto err;
  p->seg_count = p->hdr[26];
  if (bread(fb, p->seg, p->seg_count) != p->seg_count)
    goto err;
  p->seg_sum = 0;
  for (uns i = 0; i < p->seg_count; i++)
    p->seg_sum += p->seg[i];
  p->data = p->seg + p->seg_count;
  if (bread(f->fb, p->data, p->seg_sum) != p->seg_sum)
    goto err;
  return 1;
err:
  ogg_prefix_error(f, OGG_ERR_PAGE_TRUNC, ogg_page_msg_prefix, "Truncated page");
}

static uns
ogg_check_page_crc(struct ogg_file *f)
{
  struct ogg_page *p = &f->page;
  uns page_crc = get_u32_le(p->hdr + 22), crc = 0;
  bzero(p->hdr + 22, 4);
  for (uns i = 0; i < 27; i++)
    crc = (crc << 8) ^ ogg_crc_tab[((crc >> 24) & 0xff) ^ p->hdr[i]];
  for (uns i = 0; i < p->seg_count; i++)
    crc = (crc << 8) ^ ogg_crc_tab[((crc >> 24) & 0xff) ^ p->seg[i]];
  for (uns i = 0; i < p->seg_sum; i++)
    crc = (crc << 8) ^ ogg_crc_tab[((crc >> 24) & 0xff) ^ p->data[i]];
  put_u32_le(p->hdr + 22, page_crc);
  if (crc == page_crc)
    return 1;
  return 0;
}

static void
ogg_check_page(struct ogg_file *f)
{
  if (!ogg_check_page_crc(f))
    ogg_prefix_error(f, OGG_ERR_PAGE_FORMAT, ogg_page_msg_prefix, "CRC error");
}

void
ogg_decode_page_header(struct ogg_file *f)
{
  struct ogg_page *p = &f->page;
  if (memcmp(p->hdr, "OggS", 4))
    ogg_page_error(f, "Invalid magic number");
  if (p->hdr[4] != 0)
    ogg_page_error(f, "Unsupported page version (0x%x)", p->hdr[4]);
  p->flags = p->hdr[5];
  if (p->flags & OGGP_ZERO)
    ogg_page_error(f, "Invalid flags (0x%x)", p->flags);
  p->granule = get_u64_le(p->hdr + 6);
  p->serial = get_u32_le(p->hdr + 14);
  p->page_num = get_u32_le(p->hdr + 18);
  ogg_prefix_msg(f, 0x900 + L_DEBUG, ogg_page_msg_prefix, "g=%lld s=0x%x pg=0x%x len=%d f=%02x",
    (long long)p->granule, p->serial, p->page_num, p->seg_sum, p->flags);
}

static void
ogg_decode_packet(struct ogg_stream *s)
{
  STRACE(s, 10, "Got packet (len=%d)", s->buf_len);
  if (s->packet)
    OGG_TRY(s->file)
      s->packet(s, s->buf.ptr, s->buf_len);
    OGG_CATCH
      if (try_code == OGG_ERR_FATAL || try_code == OGG_ERR_STREAM_FATAL)
        OGG_THROW_AGAIN;
    OGG_TRY_END;
}

static void
ogg_decode_page_data(struct ogg_file *f)
{
  OGG_TRY(f)
    {
      struct ogg_page *p = &f->page;
      struct ogg_stream *s;
      if (!(f->stream = s = ogg_stream_hash_find(f->streams_table, p->serial)))
        {
    	  TRACE(f, 0, "Found new stream 0x%08x", p->serial);
    	  if (f->streams_table->hash_count >= ogg_max_streams)
    	    ogg_error(f, "Too many streams");
    	  f->stream = s = ogg_stream_hash_new(f->streams_table, p->serial);
    	  bzero(s, sizeof(*s));
    	  s->serial = p->serial;
    	  s->file = f;
    	  s->packet = f->stream_packet;
    	  s->end = f->stream_end;
    	  s->cleanup = f->stream_cleanup;
    	  bb_init(&s->buf);
    	  s->pool = mp_new(8192);
    	  clist_add_tail(&f->streams, &s->node);
    	  if (!(p->flags & OGGP_FIRST))
    	    ogg_stream_error(s, "Missing 'first' flag in the first page");
    	}
      if (s->flags & OGGS_IGNORE)
	break;
      if (s->flags & OGGS_CLOSED)
    	ogg_stream_error(s, "Unexpected new page of already ended stream");
      if (s->page_num != p->page_num)
	ogg_stream_error(s, "Discontinuous stream (found id 0x%x, expected 0x%x)", p->page_num, s->page_num);
      if (!(s->flags & OGGS_CONTINUED) ^ !(p->flags & OGGP_CONTINUED))
    	ogg_stream_msg(s, L_WARN_R, "Invalid 'continued' flag in page 0x%x", s->page_num);
      s->page_num++;
      byte *data = p->data;
      uns data_len = 0;
      for (uns i = 0; i < p->seg_count; i++)
        {
    	  data_len += p->seg[i];
    	  if (p->seg[i] != 255)
  	    {
    	      if (data_len > ogg_max_packet_size - s->buf_len)
    		ogg_stream_error(s, "Buffer overflow");
    	      bb_grow(&s->buf, s->buf_len + data_len);
    	      memcpy(s->buf.ptr + s->buf_len, data, data_len);
    	      s->buf_len += data_len;
	      ogg_decode_packet(s);
    	      if (s->flags & OGGS_IGNORE)
		break;
    	      s->buf_len = 0;
    	      s->packet_num++;
    	      data += data_len;
    	      data_len = 0;
    	      s->flags &= ~OGGS_CONTINUED;
    	    }
    	}
      if (data_len)
        {
    	  bb_grow(&s->buf, s->buf_len + data_len);
    	  memcpy(s->buf.ptr + s->buf_len, data, data_len);
    	  s->buf_len += data_len;
    	  s->flags |= OGGS_CONTINUED;
    	}
      if (p->flags & OGGP_LAST)
        {
    	  if (s->flags & OGGS_CONTINUED)
    	    ogg_stream_error(s, "Unterminated packed");
    	  STRACE(s, 0, "Stream ended");
    	  s->flags |= OGGS_CLOSED;
    	  if (s->end)
    	    s->end(s);
    	}
    }
  OGG_CATCH
    {
      if (try_code == OGG_ERR_FATAL)
        OGG_THROW_AGAIN;
      else if (try_code == OGG_ERR_STREAM_FATAL)
	try_file->stream->flags |= OGGS_IGNORE | OGGS_ERROR;
    }
  OGG_TRY_END;
}

void
ogg_decode_page(struct ogg_file *f)
{
  ogg_decode_page_header(f);
  ogg_check_page(f);
  ogg_decode_page_data(f);
}

uns
ogg_scan_page(struct ogg_file *f)
{
  if (!ogg_read_page(f))
    return 0;
  ogg_decode_page(f);
  return 1;
}

void
ogg_seek_page(struct ogg_file *f)
{
  struct fastbuf *fb = f->fb;
  struct ogg_page *p = &f->page;

  static const byte ogg_seek_prefix[] = "OggS\0"; /* The automaton contains no backwards edges */
  uns state = 0, size = ogg_max_seek_size, checks = ogg_max_seek_checks;
  int c;
  memcpy(p->hdr, ogg_seek_prefix, sizeof(ogg_seek_prefix));

  while ((c = bgetc(fb)) >= 0)
    {
      if (!size)
        {
	  bungetc(fb);
          ogg_prefix_error(f, OGG_ERR_SEEK, NULL, "Seek failed, no page found in selected interval");
	}
      if (c != ogg_seek_prefix[state])
        {
	  state = 0;
	  size--;
	}
      else if (++state == ARRAY_SIZE(ogg_seek_prefix))
        { /* This can be very slow in special cases */
	  if (bread(fb, p->hdr + ARRAY_SIZE(ogg_seek_prefix), 27 - ARRAY_SIZE(ogg_seek_prefix)) != 27 - ARRAY_SIZE(ogg_seek_prefix))
	    break;
	  p->pos = btell(f->fb) - 27;
	  p->seg_count = p->hdr[26];
	  if (bread(fb, p->seg, p->seg_count) != p->seg_count)
	    goto next;
          p->seg_sum = 0;
	  for (uns i = 0; i < p->seg_count; i++)
	    p->seg_sum += p->seg[i];
	  if (bread(fb, p->data, p->seg_sum) != p->seg_sum)
	    goto next;
	  if (ogg_check_page_crc(f))
	    return;
next:
	  TRACE(f, 9, "Seek check failed at 0x%llx", (unsigned long long int)p->pos);
	  if (!--checks)
	    ogg_prefix_error(f, OGG_ERR_SEEK, NULL, "Seek failed, too many checks");
	  bsetpos(fb, p->pos + ARRAY_SIZE(ogg_seek_prefix));
	  size -= ARRAY_SIZE(ogg_seek_prefix);
	  state = 0;
	
	}
    }
  ogg_prefix_error(f, OGG_ERR_SEEK, NULL, "Seek failed, no page found before EOF");
}

void
ogg_seek_last_page(struct ogg_file *f)
{
  struct fastbuf *fb = f->fb;
  ucw_off_t size = bfilesize(fb);
  bsetpos(fb, (size > ogg_max_seek_size) ? size - ogg_max_seek_size : 0);
  ogg_seek_page(f);
  OGG_TRY(f)
    while (ogg_read_page(f));
  OGG_CATCH
    ogg_throw(f, OGG_ERR_SEEK);
  OGG_TRY_END;
}

void
ogg_scan(struct ogg_file *f)
{
  while (ogg_scan_page(f));
}

/*** INIT/CLEANUP ***/

void
ogg_init(struct ogg_file *f)
{
  bzero(f, sizeof(*f));
  f->pool = mp_new(4096);
  bb_init(&f->msg_buf);
  f->streams_table = mp_alloc_zero(f->pool, sizeof(*f->streams_table));
  clist_init(&f->streams);
  ogg_stream_hash_init(f->streams_table);
  f->msg_level = ogg_trace ? (ogg_trace << 8) : 0x100;
  f->stream_packet = ogg_stream_packet_first;
  f->msg = ogg_msg_default;
  f->page.hdr = mp_alloc(f->pool, OGG_MAX_PAGE_SIZE);
  f->page.seg = f->page.hdr + 27;
}

void
ogg_cleanup(struct ogg_file *f)
{
  CLIST_FOR_EACH(struct ogg_stream *, s, f->streams)
    ogg_stream_cleanup(s);
  ogg_stream_hash_cleanup(f->streams_table);
  bb_done(&f->msg_buf);
  mp_delete(f->pool);
  if (f->fb)
    bclose(f->fb);
}
