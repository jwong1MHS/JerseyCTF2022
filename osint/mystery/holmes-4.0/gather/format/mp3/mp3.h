/*
 *	MP3 Analyser
 *
 *	(c) 2006--2007 Martin Mares <mj@ucw.cz>
 */

#ifndef _MP3_MP3_H
#define _MP3_MP3_H

#include "ucw/config.h"
#include "bitstream.h"

#include <stdarg.h>

/*** The context structure ***/

struct mp3_file {
	struct fastbuf *fb;		// fastbuf stream with the MP3
	int limit;			// position where we want to stop (-1=none)
	void (*warn)(struct mp3_file *f, char *msg, va_list args);

	byte *back_buf, *back_end, *back_p, *back_e;	// for backward reading
	int back_e_pos;
};

void mp3_init(struct mp3_file *f);
void mp3_cleanup(struct mp3_file *f);
void mp3_warn(struct mp3_file *f, char *msg, ...);

/*** Frame headers ***/

struct mp3_header {
	enum frame_type {
		FT_MP3,
		FT_ID3v1,
		FT_ID3v2,
		FT_APE,
		FT_LYRICS
	} type;
	int unsynced_bytes_skipped;

	byte hdr[32];			// raw header (4 bytes for MP3, 10 for ID3v2, 32 for APEv2)
	int hdr_pos;
	int hdr_len;

	byte mpeg;			// 0=MPEG2.5, 1=?, 2=MPEG2, 3=MPEG1
	byte layer;			// 1-3, 4=?
	int bitrate;			// bit/s
	int samplerate;			// sample/s
	byte protected;			// 0=CRC16 follows header
	byte padding;			// in bytes
	byte private;			// flag: private (application specific)
	byte channels;			// 0=stereo, 1=joint, 2=dual, 3=mono
	byte mode_ext;			// mode extension (joint stereo only)
	byte copyright;			// flag: copyrighted
	byte original;			// flag: copy of original media
	byte emphasis;			// 0=none, 1=50/15us, 2=?, 3=CCITT J.17

	// calculated
	int bytes_per_frame;
	int samples_per_frame;
	int mpeg2;			// MPEG2 or MPEG2.5
};

int mp3_read_header(struct mp3_file *f, struct mp3_header *h);
void mp3_print_header(struct mp3_header *h);

int mp3_scan_tag_last(struct mp3_file *f, struct mp3_header *h);	// Starts at h->hdr_pos
int mp3_scan_tag_back(struct mp3_file *f, struct mp3_header *h);	// Find previous tag

/*** Frame interior ***/

struct mp3_frame {
	struct mp3_header *hdr;
	struct bit_stream bs;		// Bit stream containing frame data
};

void mp3_init_frame(struct mp3_frame *f, struct mp3_file *fi, struct mp3_header *h, byte *frame);

/*** Side info ***/

struct mp3_side_chan {
	int part23len;
	int bigvals;
	int globgain;
	int sfc;
	int wsf;
	int blktype;
	int swpt;
	int tabsel[3];
	int sbg[3];
	int r1;
	int r2;
	int preflag;
	int scalefac;
	int count1tab;
};

struct mp3_side_info {
	int stereo;			// Stereo enabled
	int granules;			// Number of granules (2 for MPEG1, 1 for MPEG2)
	int bytes_per_sinfo;

	int main_data_begin;
	int private_bits;
	int scfsi[2];
	struct mp3_side_chan chan[2][2];	// Per-channel info [granule][channel]

	int all_parts_23;		// Total size (in bits) of all parts 2 and 3
};

int mp3_parse_side_info(struct mp3_frame *f, struct mp3_side_info *s);
void mp3_print_side_info(struct mp3_side_info *s);

/*** VBR headers ***/

struct mp3_vbr_fraunhofer {
	byte *raw;
	int version;			// Tag version, semantics unknown
	int enc_delay;
	int quality;
	unsigned int frames;
	unsigned int bytes;
	int toc_entries;
	int toc_scale;
	int toc_entry_bytes;
	int toc_entry_frames;
	byte *toc;			// NULL if doesn't fit in the frame
};

struct mp3_vbr_xing {
	byte *raw;
	int vbr;			// It's a VBR file (0 for LAME-tagged CBR)
	unsigned int flags;
	unsigned int frames;
	unsigned int bytes;
	byte *toc;
	unsigned int quality;

	/* LAME extensions */
	int lame;			// The extensions are present
	byte lame_ver[10];		// Version string of the encoder
	int lame_tag_ver;		// Version of this tag
	int vbr_method;			// 0..15 (0=undef, 1=CBR, 8=2-pass CBR, others are VBR modes)
	int lowpass;			// Lowpass filter frequency / 100Hz
	unsigned int peak;		// Peak amplitude (as a 32-bit float)
	int radio_replay_gain;		// Replay gain flags
	int audiophile_replay_gain;
	int enc_flags;			// Encoder flags (4 bits)
	int ath_type;			// Absolute threshold of hearing type (0-15)
	int specified_bitrate;		// VBR: minimal bitrate, ABR: average bitrate
	int enc_delay;			// Number of samples added at the start ...
	int enc_padding;		// ... and at the end
	int noise_shaping;		// Noise shaping mode (0-3)
	int stereo_mode;		// Stereo mode (0-7)
	int unwise_settings;		// Encoder reports unwise settings :)
	int source_sfreq;		// Source sample freq (0=<32k, 1=44.1k, 2=48k, 3=>48k)
	int mp3_gain;			// MP3 gain adjustment done
	int surround;			// Surround encoding (0-7, 0=none)
	int preset;			// Preset used (0-2047)
	unsigned int music_bytes;	// Real size of the compressed data without tags
	unsigned int music_crc;		// CRC-16 of the music data
	unsigned int tag_crc;		// CRC-16 of this header
};

int mp3_parse_vbr(struct mp3_frame *f, struct mp3_vbr_fraunhofer *v);
int mp3_parse_xing(struct mp3_frame *f, struct mp3_vbr_xing *v);
void mp3_print_vbr(struct mp3_vbr_fraunhofer *v);
void mp3_print_xing(struct mp3_vbr_xing *v);

#endif
