/*
 *	OGG Parser - Main Definitions
 *
 * 	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 * 	References:
 * 	- http://www.xiph.org/ogg/doc/
 * 	- http://en.wikipedia.org/wiki/Ogg
 * 	- rfc3533: The Ogg Encapsulation Format Version 0
 * 	- rfc3534: The application/ogg Media Type
 */

#ifndef _SHERLOCK_GATHER_FORMAT_OGG_OGG_H
#define _SHERLOCK_GATHER_FORMAT_OGG_OGG_H

#include "ucw/bbuf.h"
#include "ucw/clists.h"

#include <setjmp.h>

struct mempool;
struct fastbuf;

struct ogg_file;
struct ogg_page;
struct ogg_stream;
struct ogg_codec;

/* Global/configuraion variables */
extern clist ogg_codecs;		/* List of known stream codecs (struct ogg_codec) */
extern uns ogg_trace;			/* Tracing */
extern uns ogg_warnings;		/* Log warnings */
extern uns ogg_max_warnings;		/* Maximum number of warnings before we reject the file as corrupted */
extern uns ogg_max_streams;		/* Maximum number of streams in a single OGG file
					 * (an overflow throws OGG_ERR_FATAL) */
extern uns ogg_max_packet_size;		/* Maximum allowed packet size */
extern uns ogg_max_seek_size;		/* Seek operations search only this interval for a page start */
extern uns ogg_max_seek_checks;		/* Maximum number of CRC checks when seeking a page
					 * (the algorithm checks each found 'OggS' substring) */

/* OGG page flags as defined in the specification */
enum ogg_page_flags {
  OGGP_CONTINUED =	0x01,		/* Continued packet */
  OGGP_FIRST =		0x02,		/* First page of logical stream */
  OGGP_LAST =		0x04,		/* Last page of logical stream */
  OGGP_ZERO =		0xf8,		/* These flags must be unset */
};

/* OGG page */
struct ogg_page {
  /* Filled by ogg_read_page() */
  byte *hdr;				/* 27 bytes long page header */
  byte *seg;				/* Page segment table (= hdr + 27) */
  byte *data;				/* Page data (= seg + seg_count) */
  ucw_off_t pos;				/* Page start offset in the source fastbuf */
  uns seg_count;			/* Number of segments */
  uns seg_sum;				/* Total length of all segments (length of data buffer) */

  /* Filled by ogg_decode_page_header() */
  u64 granule;				/* Decoded granule position (codec-dependant meaning) */
  uns serial;				/* Stream ID */
  uns page_num;				/* Page sequential number in the stream */
  enum ogg_page_flags flags;		/* OGGP_x flags */
};

/* Internal stream flags */
enum ogg_stream_flags {
  OGGS_CLOSED =		0x00000001,	/* The stream is closed with OGGP_LAST page */
  OGGS_CONTINUED =	0x00000002,	/* Last decoded page contained an unclosed packet */
  OGGS_CLEANED =	0x00000004,	/* The stream is cleaned up */
  OGGS_ERROR =		0x00000008,	/* The stream is undecodable (OGG_ERR_STREAM_FATAL occured) */
  OGGS_IGNORE = 	0x00000010,	/* Ignore all remaining pages belonging to this stream */
};

/* Stream content type */
enum ogg_stream_type {
  OGGS_TYPE_UNDEF =	0x00000001,	/* Unknown codec */
  OGGS_TYPE_AUDIO =	0x00000002,	/* Audio content (Vorbis, ...) */
  OGGS_TYPE_VIDEO =	0x00000004,	/* Video content (Theora, ...) */
  OGGS_TYPE_TEXT =	0x00000008,	/* Text content (Writ, ...) */
};

/* Stream codec definition */
struct ogg_codec {
  cnode node;				/* Node in ogg_codecs clist */
  byte *name;				/* Codec name */
  enum ogg_stream_type type;		/* Content type */
  uns (*detect)(byte *packet, uns len);	/* Codec detector (receives the first packet in the stream); nonzero means match */
  void (*start)(struct ogg_stream *s, byte *packet, uns len);
  					/* Start decoding a stream for which we detected this codec.
					 * The function should setup needed callbacks in struct ogg_stream. */
};

/* Media mapping definition */
struct ogg_mapping {
  cnode node;
  byte *name;
};

/* Minimum and maximum possible page size (hdr + seg + data) */
#define OGG_MIN_PAGE_SIZE 27
#define OGG_MAX_PAGE_SIZE (27 + 255 + 255 * 255)

/* OGG stream decoder */
struct ogg_stream {
  cnode node;				/* Node in file->streams */
  struct ogg_file *file;		/* Pointer to the incident file structure */
  struct mempool *pool;			/* Memory pool that lives from the creation time until stream cleanup */
  struct ogg_codec *codec;		/* Detected codec or NULL if unknown */
  enum ogg_stream_flags flags;		/* OGGS_x flags */
  uns serial;				/* Stream ID (unique in OGG file) */
  uns page_num;				/* Number of decoded pages */
  uns packet_num;			/* Number of decoded packets */
  uns buf_len;
  bb_t buf;
  void (*packet)(struct ogg_stream *s, byte *packet, uns len);
  void (*end)(struct ogg_stream *s);
  void (*cleanup)(struct ogg_stream *s);
  void *user;
};

struct ogg_stream_hash_table;

struct ogg_file {
  struct mempool *pool;
  struct fastbuf *fb;
  clist streams;
  struct ogg_stream *stream;
  struct ogg_stream_hash_table *streams_table;
  jmp_buf *throw_buf;
  bb_t msg_buf;
  uns msg_level;
  uns max_streams;
  uns stream_types;
  uns forbidden_stream_types;
  uns skip_stream_types;
  struct ogg_page page;
  void (*msg)(struct ogg_file *f, uns type, byte *msg);
  void (*term)(struct ogg_file *f);
  void (*stream_packet)(struct ogg_stream *s, byte *packet, uns len);
  void (*stream_end)(struct ogg_stream *s);
  void (*stream_cleanup)(struct ogg_stream *s);
  void (*before_stream_start)(struct ogg_stream *s);
};

void ogg_init(struct ogg_file *f);
void ogg_init_fastbuf(struct ogg_file *f, struct fastbuf *fb);
void ogg_init_file(struct ogg_file *f, byte *fn);
void ogg_cleanup(struct ogg_file *f);

enum ogg_error_code {
  OGG_ERR_FATAL = 1,		/* Fatal error in physical stream */
  OGG_ERR_SEEK,			/* Seek error */
  OGG_ERR_PAGE_FORMAT,		/* Invalid page format */
  OGG_ERR_PAGE_TRUNC,		/* Truncated page */
  OGG_ERR_STREAM_FATAL,		/* Fatal error in logical stream */
  OGG_ERR_STREAM_PACKET,	/* Undecodable stream packet */
  OGG_ERR_USER,			/* First user error... usually the same meaning as OGG_ERR_STREAM_PACKET */
};

#define OGG_TRY(f) do { \
  struct ogg_file * volatile try_file = (f); \
  jmp_buf * volatile try_saved = try_file->throw_buf, try_buf; \
  try_file->throw_buf = &try_buf; \
  int try_code; try_code = setjmp(try_buf); \
  if (likely(!try_code)) { do {
#define OGG_CATCH } while (0); try_file->throw_buf = try_saved; } else { try_file->throw_buf = try_saved;
#define OGG_TRY_END } } while (0)
#define OGG_THROW_AGAIN ogg_throw(try_file, try_code)

void ogg_throw(struct ogg_file *f, enum ogg_error_code code) NONRET;

void ogg_prefix_error(struct ogg_file *f, enum ogg_error_code code, void (*prefix)(struct ogg_file *f), char *msg, ...) NONRET;
void ogg_prefix_msg(struct ogg_file *f, uns type, void (*prefix)(struct ogg_file *f), char *msg, ...);
void ogg_stream_msg_prefix(struct ogg_file *f);
void ogg_packet_msg_prefix(struct ogg_file *f);

#define ogg_error(f, ...) ogg_prefix_error((f), OGG_ERR_FATAL, NULL, __VA_ARGS__)
#define ogg_msg(f, t, ...) ogg_prefix_msg((f), (t), NULL, __VA_ARGS__)
#define ogg_stream_error(s, ...) ogg_prefix_error((s)->file, OGG_ERR_STREAM_FATAL, ogg_stream_msg_prefix, __VA_ARGS__)
#define ogg_stream_msg(s, t, ...) ogg_prefix_msg((s)->file, (t), ogg_stream_msg_prefix, __VA_ARGS__)
#define ogg_packet_error(s, ...) ogg_prefix_error((s)->file, OGG_ERR_STREAM_PACKET, ogg_stream_msg_prefix, __VA_ARGS__)
#define ogg_packet_msg(s, t, ...) ogg_prefix_msg((s)->file, (t), ogg_stream_msg_prefix, __VA_ARGS__)

/*** PAGES ***/

/* Reads single page from the source fastbuf (can return 0 on EOF or throw OGG_ERR_PAGE_TRUNC) */
uns ogg_read_page(struct ogg_file *f);

/* Tries to find and read a page after fastbuf's current position (can throw OGG_ERR_SEEK) */
void ogg_seek_page(struct ogg_file *f);

/* Tries to find and read the last page in fastbuf (can throw OGG_ERR_SEEK) */
void ogg_seek_last_page(struct ogg_file *f);

/* Decode page and send packets to their streams (can throw OGG_ERR_* except OGG_ERR_{SEEK,PAGE_TRUNC}) */
void ogg_decode_page(struct ogg_file *f);

/* Same as ogg_read_page + ogg_decode_page */
uns ogg_scan_page(struct ogg_file *f);

/* Repet ogg_scan_page until EOF */
void ogg_scan(struct ogg_file *f);

/* Decode page header (can throw OGG_ERR_FORMAT) */
void ogg_decode_page_header(struct ogg_file *f);

#endif
