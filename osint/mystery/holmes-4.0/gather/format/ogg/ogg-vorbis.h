/*
 *	OGG Parser - Vorbis Codec
 *
 * 	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 * 	References:
 * 	- http://en.wikipedia.org/wiki/Vorbis
 * 	- http://xiph.org/vorbis/doc/
 */

#ifndef _SHERLOCK_GATHER_FORMAT_OGG_OGG_VORBIS_H
#define _SHERLOCK_GATHER_FORMAT_OGG_OGG_VORBIS_H

#include "gather/format/ogg/ogg.h"

extern struct ogg_codec ogg_vorbis_codec;

struct ogg_vorbis_comment {
  byte *name;				/* Comment name (case insensitive ASCII) */
  byte *value;				/* Comment value (UTF-8) */
};

enum ogg_vorbis_stream_flags {
  OGGS_VORBIS_ID_HEADER_VALID		= 0x00000001,	/* Identification header parsed successfully */
  OGGS_VORBIS_COMMENT_HEADER_VALID	= 0x00000002,	/* Comment header parsed successfully */
};

struct ogg_vorbis_stream {
  struct ogg_stream *stream;

  enum ogg_vorbis_stream_flags flags;	/* OGGS_VORBIS_x */

  /* Identification header (if OGG_VORBIS_ID_HEADER_VALID is set) */
  uns audio_sample_rate;		/* Samples per second */
  int bitrate_maximum;			/* Maximum bitrate hint (present if > 0) */
  int bitrate_nominal;			/* Nominal bitrate hint (present if > 0) */
  int bitrate_minimum;			/* Minimum bitrate hint (present if > 0) */
  uns audio_channels;			/* Number of audio channels */
  uns blocksize_0;			/* Number of samples in small packets */
  uns blocksize_0_log;
  uns blocksize_1;			/* Number of samples in large packets */
  uns blocksize_1_log;

  /* Comment header (if OGG_VORBIS_COMMENT_HEADER_VALID is set) */
  byte *vendor;				/* Vendor string (UTF-8) */
  uns comment_count;			/* Number of user comments */
  struct ogg_vorbis_comment *comments;	/* Array of user comments */

  /* Timing */
  u64 start_sample;			/* Start sample (~0 if unknown) */
  u64 end_sample;			/* End sample + 1 (~0 if unknown) */
};

/* Scan entire OGG Vorbis physical stream */
void ogg_vorbis_scan_file(struct ogg_file *f);

/* Scan head of OGG Vorbis physical stream (headers and time_start) */
void ogg_vorbis_scan_head(struct ogg_file *f);

/* Scan tail of OGG Vorbis physical stream (time_end) */
void ogg_vorbis_scan_tail(struct ogg_file *f);

#endif
