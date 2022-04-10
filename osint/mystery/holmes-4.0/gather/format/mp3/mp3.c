/*
 *	Dissecting MP3 File Structure
 *
 *	(c) 2006--2007 Martin Mares <mj@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/fastbuf.h"
#include "ucw/unaligned.h"
#include "mp3.h"

#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#define MSG printf

/*** INITIALIZATION ***/

static void
mp3_default_warn(struct mp3_file *f, char *msg, va_list args)
{
	fprintf(stderr, "WARNING: %s: ", f->fb->name);
	vfprintf(stderr, msg, args);
	fputc('\n', stderr);
}

void
mp3_init(struct mp3_file *f)
{
	bzero(f, sizeof(*f));
	f->limit = -1;
	f->warn = mp3_default_warn;
}

void
mp3_cleanup(struct mp3_file *f)
{
	xfree(f->back_buf);
}

void
mp3_warn(struct mp3_file *f, char *msg, ...)
{
	va_list args;
	va_start(args, msg);
	if (f->warn)
		f->warn(f, msg, args);
	va_end(args);
}

/*** FRAME HEADERS ***/

static int
validate_header(struct mp3_header *h)
{
	byte *hdr = h->hdr;
	return hdr[0] == 0xff &&		// sync
		(hdr[1] & 0xe0) == 0xe0 &&
		(hdr[2] >> 4) != 0x0f &&	// bitrate
		((hdr[2] >> 2) & 3) != 3 &&	// sample rate
		(hdr[3] & 3) != 3;		// emphasis
}

static int
decode_header(struct mp3_file *f, struct mp3_header *h)
{
	byte *hdr = h->hdr;
	char *err;

	// See http://www.dv.co.yu/mpgscript/mpeghdr.htm
	//MSG("%x: %02x %02x %02x %02x\n", h->hdr_pos, hdr[0], hdr[1], hdr[2], hdr[3]);

	h->mpeg = (hdr[1] >> 3) & 3;
	h->layer = 4-((hdr[1] >> 1) & 3);
	h->protected = hdr[1] & 1;
	h->mpeg2 = (h->mpeg == 2 || h->mpeg == 0);	// MPEG 2 or 2.5

	unsigned type;
	if (h->mpeg == 3 && h->layer)
		type = h->layer-1;
	else if (h->mpeg2 && h->layer == 1)
		type = 3;
	else if (h->mpeg2 && (h->layer == 2 || h->layer == 3))
		type = 4;
	else {
		err = "Unknown MPEG version";
		goto error;
	}
	static const int br[5][16] = {
		{ 0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448, -1 },		// MPEG1 layer 1
		{ 0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, -1 },		// MPEG1 layer 2
		{ 0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, -1 },		// MPEG1 layer 3
		{ 0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256, -1 },		// MPEG2 layer 1
		{ 0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, -1 }			// MPEG2 layer 2 and 3
	};
	h->bitrate = br[type][hdr[2] >> 4] * 1000;
	if (!h->bitrate) {
		err = "Free-format frames not supported";
		goto error;
	}
	if (h->bitrate < 0) {
		err = "Unknown bitrate code";
		goto error;
	}

	static const int sr[4][4] = {
		{ 11025, 12000, 8000, -1 },	// MPEG2.5
		{ -1, -1, -1, -1 },
		{ 22050, 24000, 16000, -1 },	// MPEG2
		{ 44100, 48000, 32000, -1 }	// MPEG1
	};
	h->samplerate = sr[h->mpeg][(hdr[2] >> 2) & 3];
	if (h->samplerate < 0) {
		err = "Unknown samplerate code";
		goto error;
	}

	h->padding = (hdr[2] >> 1) & 1;
	h->private = hdr[2] & 1;
	h->channels = (hdr[3] >> 6) & 3;
	h->mode_ext = (hdr[3] >> 4) & 3;
	h->copyright = (hdr[3] >> 3) & 1;
	h->original = (hdr[3] >> 2) & 1;
	h->emphasis = hdr[3] & 3;

	if (h->layer == 1) {
		h->samples_per_frame = 384;
		h->padding *= 4;
	} else {
		h->samples_per_frame = 1152;
	}
	h->bytes_per_frame = (h->bitrate/8) * h->samples_per_frame / (h->samplerate << h->mpeg2) + h->padding - 4;

	return 1;

 error:
	mp3_warn(f, "Cannot parse frame header at %x (%02x %02x %02x %02x): %s",
		h->hdr_pos, hdr[0], hdr[1], hdr[2], hdr[3], err);
	return 0;
}

static int
verify_id3v2(struct mp3_file *f, byte *hdr, struct mp3_header *h)
{
	/* Verify ID3v2 header or footer */
	if (hdr[3] > 4)
		mp3_warn(f, "Cannot parse ID3v2 tag: unsupported version %02x", hdr[3]);
	else if (hdr[5] & 0x0f)
		mp3_warn(f, "Cannot parse ID3v2 tag: unknown flags %02x", hdr[5]);
	else if ((hdr[6] & 0x80) || (hdr[7] & 0x80) | (hdr[8] & 0x80) | (hdr[9] & 0x80))
		mp3_warn(f, "Cannot parse ID3v2 tag: incorrect length encoding");
	else {
		h->bytes_per_frame = (hdr[6] << 21) | (hdr[7] << 14) | (hdr[8] << 7) | hdr[9];
		h->hdr_len = 10;
		return 1;
	}
	return 0;
}

int
mp3_read_header(struct mp3_file *f, struct mp3_header *h)
{
	struct fastbuf *fb = f->fb;
	byte *hdr = h->hdr;
	unsigned int cnt = 0;
 reset:
	bzero(hdr, 4);
	for (;;) {
		if (cnt >= 4) {
			h->unsynced_bytes_skipped = cnt - 4;
			h->hdr_pos = btell(fb) - 4;
			if (validate_header(h)) {
				h->type = FT_MP3;
				h->hdr_len = 4;
				if (decode_header(f, h))
					return 1;
			} else if (hdr[0] == 'T' && hdr[1] == 'A' && hdr[2] == 'G') {
				h->type = FT_ID3v1;
				h->bytes_per_frame = 124;
				h->hdr_len = 4;
				return 1;
			} else if (hdr[0] == 'I' && hdr[1] == 'D' && hdr[2] == '3') {
				h->type = FT_ID3v2;
				int n = bread(fb, hdr+4, 6);
				cnt += n;
				if (n < 6)
					mp3_warn(f, "Cannot parse ID3v2 tag: truncated header");
				else if (verify_id3v2(f, hdr, h)) {
					if (hdr[5] & 0x10)	// footer present
						h->bytes_per_frame += 10;
					return 1;
				}
				goto reset;
			} else if (!memcmp(hdr, "APET", 4)) {
				int n = bread(fb, hdr, 4);
				cnt += n;
				if (n < 4 || memcmp(hdr, "AGEX", 4))
					continue;
				n = bread(fb, hdr+8, 24);
				cnt += n;
				if (n < 24)
					mp3_warn(f, "Cannot parse APE tag: truncated header");
				else {
					h->type = FT_APE;
					h->hdr_len = 32;
					h->bytes_per_frame = get_u32_le(hdr+12);
					return 1;
				}
				goto reset;
			} else if (!memcmp(hdr, "LYRI", 4)) {		// So far not decoded in forward mode
				h->type = FT_LYRICS;
				h->hdr_len = 4;
				h->bytes_per_frame = 0;
				return 1;
			}
		}
		hdr[0] = hdr[1];
		hdr[1] = hdr[2];
		hdr[2] = hdr[3];
		if (f->limit >= 0 && btell(fb) >= f->limit) {
			h->unsynced_bytes_skipped = cnt;
			return 0;
		}
		int c = bgetc(fb);
		if (c < 0) {
			h->unsynced_bytes_skipped = cnt;
			return 0;
		}
		hdr[3] = c;
		cnt++;
	}
}

void
mp3_print_header(struct mp3_header *h)
{
	MSG("%x: ", h->hdr_pos);
	switch (h->type) {
	case FT_MP3:
		MSG("MPEG %s layer %d prot=%d brate=%d srate=%d pad=%d priv=%d chan=%s mode=%d copy=%d orig=%d emph=%d bpf=%d spf=%d\n",
			((char *[]){ "2.5", "??", "2", "1" }) [h->mpeg],
			h->layer,
			h->protected,
			h->bitrate,
			h->samplerate,
			h->padding,
			h->private,
			((char *[]){ "stereo", "joint", "dual", "mono" }) [h->channels],
			h->mode_ext,
			h->copyright,
			h->original,
			h->emphasis,
			h->bytes_per_frame,
			h->samples_per_frame);
		break;
	case FT_ID3v1:
		MSG("ID3v1 tag: %d bytes\n", h->hdr_len + h->bytes_per_frame);
		break;
	case FT_ID3v2:
		MSG("ID3v2.%d.%d tag: %d bytes\n", h->hdr[3], h->hdr[4], h->hdr_len + h->bytes_per_frame);
		break;
	case FT_APE:
		MSG("APE tag: %d bytes, version: %d, flags: %08x\n", h->hdr_len + h->bytes_per_frame, get_u32_le(h->hdr+8), get_u32_le(h->hdr+20));
		break;
	case FT_LYRICS:
		if (h->hdr_len == 4)
			MSG("LYRICS tag: ignored during forward search\n");	// FIXME
		else
			MSG("LYRICS tag: %d bytes\n", h->bytes_per_frame);
		break;
	}
}

/*** BACKWARD SCANNING FOR TAGS ***/

#define STBSIZE 1024
#define STBOVERLAP 64

int
mp3_scan_tag_back(struct mp3_file *f, struct mp3_header *h)
{
	struct fastbuf *fb = f->fb;
	int cnt = 0;

	/*
	 *  We keep a buffer of STBSIZE bytes for backward scanning:
	 *
	 *	f->back_buf	start of the buffer
	 *	f->back_end	end of the buffer
	 *	f->back_e	start of real data (or end as we are reading backwards)
	 *	f->back_p	current byte
	 *	f->back_e_pos	file position corresponding to back_e
	 *
	 *  When switching buffers, we keep at least STBOVERLAP bytes of old data
	 *  to avoid splitting headers.
	 */

	if (h->hdr_pos >= f->back_e_pos && h->hdr_pos < f->back_e_pos + (f->back_end - f->back_e))
		f->back_p = f->back_e + (h->hdr_pos - f->back_e_pos);
	else {
		f->back_e_pos = h->hdr_pos;
		f->back_p = f->back_e = f->back_end;
	}

	while (cnt < 1024) {
		if (f->back_p == f->back_e) {
			if (!f->back_e_pos)
				return 0;
			int overlap = MIN(f->back_end - f->back_p, STBOVERLAP);
			memmove(f->back_end - overlap, f->back_p, overlap);
			f->back_p = f->back_end - overlap;
			int load = MIN(f->back_p - f->back_buf, f->back_e_pos);
			f->back_e_pos -= load;
			bsetpos(fb, f->back_e_pos);
			if ((int)bread(fb, f->back_p - load, load) != load) {
				mp3_warn(f, "Read error when reading backwards");
				return 0;
			}
			f->back_e = f->back_p - load;
		}
		byte *p = --f->back_p;
		h->hdr_pos = f->back_e_pos + (p - f->back_e);
		cnt++;
		if (cnt >= 10 && (!memcmp(p, "ID3", 3) || !memcmp(p, "3DI", 3)) && verify_id3v2(f, p, h)) {
			h->type = FT_ID3v2;
			h->unsynced_bytes_skipped = cnt - 10;
			if (p[0] == 'I') {
				// The tag had no footer
				if (p[5] & 0x10)
					mp3_warn(f, "ID3v2 claimed footer present, but none found");
				memcpy(h->hdr, p, 10);
				return 1;
			} else {
				// This is the footer
				if (h->bytes_per_frame >= 10 && h->hdr_pos >= h->bytes_per_frame) {
					h->hdr_pos -= h->bytes_per_frame;
					bsetpos(fb, h->hdr_pos);
					bread(fb, h->hdr, 10);
					if (!memcmp(h->hdr, "ID3", 3) && verify_id3v2(f, h->hdr, h) && (p[5] & 0x10))
						return 1;	// Really a ID3v2 block
				}
				mp3_warn(f, "ID3v2 footer found, but the header is missing");
			}
		}
		if (cnt >= 15 && !memcmp(p+6, "LYRICS200", 9)) {
			h->type = FT_LYRICS;
			int d = 0;
			for (int i=0; i<6; i++)
				if (p[i] >= '0' && p[i] <= '9')
					d = 10*d + p[i] - '0';
				else
					goto badlyrics;
			if (h->hdr_pos >= d) {
				h->hdr_pos -= d;
				bsetpos(fb, h->hdr_pos);
				bread(fb, h->hdr, 11);
				if (!memcmp(h->hdr, "LYRICSBEGIN", 11)) {
					h->hdr_len = 11;
					h->bytes_per_frame = d - 11;
					h->unsynced_bytes_skipped = cnt - 15;
					return 1;
				}
			}
		badlyrics:
			mp3_warn(f, "Malformed LYRICSv2 tag found");
		}
		if (cnt >= 11 && !memcmp(p, "LYRICSBEGIN", 11))
			mp3_warn(f, "Unsupported LYRICS tag found");
		if (cnt >= 32 && !memcmp(p, "APETAGEX", 8)) {
			u32 flags = get_u32_le(p+20) & 0xe0000000;
			h->type = FT_APE;
			h->hdr_len = 32;
			h->bytes_per_frame = get_u32_le(p+12);
			h->unsynced_bytes_skipped = cnt - 32;
			if (flags == 0x80000000) {
				// Footer
				if (h->hdr_pos >= h->bytes_per_frame) {
					h->hdr_pos -= h->bytes_per_frame;
					bsetpos(fb, h->hdr_pos);
					bread(fb, h->hdr, 32);
					if (!memcmp(h->hdr, "APETAGEX", 8) && (get_u32_le(h->hdr+20) & 0xe0000000) == 0xa0000000)
						return 1;
				}
			} else if (!flags || flags == 0xe0000000) {
				// Header of a tag without footer
				return 1;
			}
			mp3_warn(f, "Malformed APE tag found");
		}
	}
	return 0;
}

int
mp3_scan_tag_last(struct mp3_file *f, struct mp3_header *h)
{
	struct fastbuf *fb = f->fb;

	if (!f->back_buf) {
		f->back_buf = xmalloc(STBSIZE);
		f->back_end = f->back_buf + STBSIZE;
	}
	f->back_p = f->back_e = f->back_buf;
	f->back_e_pos = h->hdr_pos;
	if (f->back_e_pos >= 128) {
		/* ID3v1 tags are at the very end of the file */
		bsetpos(fb, f->back_e_pos - 128);
		bread(fb, h->hdr, 4);
		if (!memcmp(h->hdr, "TAG", 3)) {
			h->type = FT_ID3v1;
			h->bytes_per_frame = 124;
			h->hdr_pos -= 128;
			h->hdr_len = 4;
			h->unsynced_bytes_skipped = 0;
			return 1;
		}
	}
	return mp3_scan_tag_back(f, h);
}

/*** FRAME BODY ***/

static void
mp3_frame_warn(struct bit_stream *b, char *msg, va_list args)
{
	struct mp3_file *fi = b->priv;
	fi->warn(fi, msg, args);
}

void
mp3_init_frame(struct mp3_frame *f, struct mp3_file *fi, struct mp3_header *h, byte *frame)
{
	f->hdr = h;
	bs_init(&f->bs, frame, h->bytes_per_frame);
	if (fi->warn) {
		f->bs.warn = mp3_frame_warn;
		f->bs.err = mp3_frame_warn;
		f->bs.priv = fi;
	}
}

/*** SIDE INFO ***/

int
mp3_parse_side_info(struct mp3_frame *f, struct mp3_side_info *s)
{
	struct mp3_header *h = f->hdr;
	struct bit_stream *b = &f->bs;

	if (h->mpeg == 1 || h->layer != 3)		// Only MPEG1/2 layer 3 supported
		return 0;
	s->stereo = (h->channels != 3);
	s->bytes_per_sinfo = h->mpeg2 ? (s->stereo ? 17 : 9) : (s->stereo ? 32 : 17);
	if (h->bytes_per_frame < s->bytes_per_sinfo)
		return 0;

	s->main_data_begin = bs_get_bits(b, h->mpeg2 ? 8 : 9);
	s->private_bits = bs_get_bits(b, h->mpeg2 ? (s->stereo ? 2 : 1) : (s->stereo ? 3 : 5));
	if (h->mpeg2) {
		s->granules = 1;
    		s->scfsi[0] = s->scfsi[1] = 0;
	} else {
		s->granules = 2;
		s->scfsi[0] = bs_get_bits(b, 4);
		s->scfsi[1] = (s->stereo ? bs_get_bits(b, 4) : 0);
	}
	s->all_parts_23 = 0;
	for (int g=0; g < s->granules; g++)
		for (int c=0; c<=s->stereo; c++) {
			struct mp3_side_chan *sc = &s->chan[g][c];
			sc->part23len = bs_get_bits(b, 12);
			sc->bigvals = bs_get_bits(b, 9);
			sc->globgain = bs_get_bits(b, 8);
			sc->sfc = bs_get_bits(b, h->mpeg2 ? 9 : 4);
			sc->wsf = bs_get_bits(b, 1);
			s->all_parts_23 += sc->part23len;
			if (sc->wsf) {
				sc->blktype = bs_get_bits(b, 2);
				sc->swpt = bs_get_bits(b, 1);
				sc->tabsel[0] = bs_get_bits(b, 5);
				sc->tabsel[1] = bs_get_bits(b, 5);
				for (int k=0; k<3; k++)
					sc->sbg[k] = bs_get_bits(b, 3);
			} else {
				for (int k=0; k<3; k++)
					sc->tabsel[k] = bs_get_bits(b, 5);
				sc->r1 = bs_get_bits(b, 4);
				sc->r2 = bs_get_bits(b, 3);
			}
			sc->preflag = (h->mpeg2 ? 0 : bs_get_bits(b, 1));
			sc->scalefac = bs_get_bits(b, 1);
			sc->count1tab = bs_get_bits(b, 1);
		}

	int rem_bits = s->bytes_per_sinfo*8 - bs_tell_bit_pos(b);
	if (rem_bits)
		bs_warn(b, "MP3 side info: %d bits remain", rem_bits);

	return 1;
}

void
mp3_print_side_info(struct mp3_side_info *s)
{
	MSG("\tmain_data at -%d, priv=%02x, scfsi=%x/%x, ssize=%d\n",
	       s->main_data_begin, s->private_bits, s->scfsi[0], s->scfsi[1], s->bytes_per_sinfo);
	for (int g=0; g < s->granules; g++)
		for (int c=0; c <= s->stereo; c++) {
			struct mp3_side_chan *sc = &s->chan[g][c];
			MSG("\tGranule %d channel %d: part23len=%d, bigvals=%d, globgain=%d, sfc=%d, wsf=%d\n",
			       g, c, sc->part23len, sc->bigvals, sc->globgain, sc->sfc, sc->wsf);
			if (sc->wsf)
				MSG("\t\tSplit block: type=%d, swpt=%d, tabsel=%x,%x sbg=%d,%d,%d\n",
				       sc->blktype, sc->swpt, sc->tabsel[0], sc->tabsel[1], sc->sbg[0], sc->sbg[1], sc->sbg[2]);
			else
				MSG("\t\tNormal block: tabsel=%x,%x,%x r1=%d r2=%d\n",
				       sc->tabsel[0], sc->tabsel[1], sc->tabsel[2], sc->r1, sc->r2);
			MSG("\t\tpreflag=%d, scalefac=%d, count1tab=%d\n", sc->preflag, sc->scalefac, sc->count1tab);
		}
	MSG("\tExpect ancillary data at bit position %d (byte %#x)\n", s->all_parts_23, s->all_parts_23/8);
}

/*** VBR HEADERS ***/

int
mp3_parse_vbr(struct mp3_frame *f, struct mp3_vbr_fraunhofer *v)
{
	byte *frame = f->bs.pos;
	int size = f->bs.remains;

	/* Check the Fraunhofer VBR header: always at position 32, preceded by 0's */
	if (size < 32 + 26 || memcmp(frame+32, "VBRI", 4))
		return 0;
	for (int i=0; i<32; i++)
		if (frame[i])
			return 0;
	byte *x = v->raw = frame + 32;

	v->version = get_u16_be(x+4);
	v->enc_delay = get_u16_be(x+6);
	v->quality = get_u16_be(x+8);
	v->bytes = get_u32_be(x+10);
	v->frames = get_u32_be(x+14);
	v->toc_entries = get_u16_be(x+18);
	v->toc_scale = get_u16_be(x+20);
	v->toc_entry_bytes = get_u16_be(x+22);
	v->toc_entry_frames = get_u16_be(x+24);
	if (size < 32 + 26 + v->toc_entry_bytes * v->toc_entries)
		v->toc = NULL;
	else
		v->toc = x + 26;
	return 1;
}

void
mp3_print_vbr(struct mp3_vbr_fraunhofer *v)
{
	MSG("Fraunhofer VBR header: version=%d\n", v->version);
	MSG("\tframes=%u, bytes=%u, quality=%d, enc_dly=%d\n", v->frames, v->bytes, v->quality, v->enc_delay);
	if (v->toc_entries) {
		if (v->toc)
			MSG("\tTOC: entries=%d, scale=%d, bpe=%d, fpe=%d\n", v->toc_entries, v->toc_scale, v->toc_entry_bytes, v->toc_entry_frames);
		else
			MSG("\tTOC: INVALID\n");
	}
}

int
mp3_parse_xing(struct mp3_frame *f, struct mp3_vbr_xing *v)
{
	byte *frame = f->bs.pos;
	int size = f->bs.remains;
	struct mp3_header *h = f->hdr;

	/* First of all, check that the frame is otherwise empty */
	int stereo = (h->channels != 3);
	int bytes_per_sinfo = h->mpeg2 ? (stereo ? 17 : 9) : (stereo ? 32 : 17);
	if (bytes_per_sinfo + 4 + 4 > size)
		return 0;
	for (int i=0; i<bytes_per_sinfo; i++)
		if (frame[i])
			return 0;
	
	/* Try to find the XING header ("Info" is used by LAME in CBR modes) */
	byte *x = frame + bytes_per_sinfo;
	byte *e = frame + size;
	int vbr;
	if (!memcmp(x, "Xing", 4))
		vbr = 1;
	else if (!memcmp(x, "Info", 4))
		vbr = 0;
	else
		return 0;
	bzero(v, sizeof(struct mp3_vbr_xing));
	v->raw = x;
	v->vbr = vbr;
	x += 4;

	/* Verify XING flags and size */
	v->flags = get_u32_be(x);
	x += 4;
	if (v->flags & 1) {
		if (x+4 > e)
			return 0;
		v->frames = get_u32_be(x);
		x += 4;
	}
	if (v->flags & 2) {
		if (x+4 > e)
			return 0;
		v->bytes = get_u32_be(x);
		x += 4;
	}
	if (v->flags & 4) {
		if (x+100 > e)
			return 0;
		v->toc = x;
		x += 100;
	}
	if (v->flags & 8) {
		if (x+4 > e)
			return 0;
		v->quality = get_u32_be(x);
		x += 4;
	}
	if (v->flags & ~15U)
		return 0;

	/* Check for LAME extensions */
	if (x + (0xc0 - 0x9c) <= e && !memcmp(x, "LAME", 4)) {
		/* XXX: Probably should check the CRC as well */
		v->lame = 1;
		memcpy(v->lame_ver, x, 9);
		v->lame_ver[9] = 0;
		for (int i=8; i>=0 && v->lame_ver[i] == ' '; i--)
			v->lame_ver[i] = 0;

		x -= 0x9c;				// To align with the position in the spec
		v->lame_tag_ver = x[0xa5] >> 4;
		v->vbr_method = x[0xa5] & 0x0f;
		v->lowpass = x[0xa6];
		v->peak = get_u32_be(x+0xa7);		// In reality, this is a 32-bit float
		v->radio_replay_gain = get_u16_be(x+0xab);
		v->audiophile_replay_gain = get_u16_be(x+0xad);
		v->enc_flags = x[0xaf] >> 4;
		v->ath_type = x[0xaf] & 0x0f;
		v->specified_bitrate = x[0xb0];
		v->enc_delay = get_u16_be(x+0xb1) >> 4;
		v->enc_padding = get_u16_be(x+0xb2) & 0xfff;
		v->noise_shaping = x[0xb4] & 3;
		v->stereo_mode = (x[0xb4] >> 2) & 7;
		v->unwise_settings = (x[0xb4] >> 5) & 1;
		v->source_sfreq = x[0xb4] >> 6;
		v->mp3_gain = (signed char) x[0xb5];
		v->surround = (x[0xb6] >> 11) & 7;
		v->preset = get_u16_be(x+0xb6) & 0x7ff;
		v->music_bytes = get_u32_be(x+0xb8);
		v->music_crc = get_u16_be(x+0xbc);
		v->tag_crc = get_u16_be(x+0xbe);
	}

	return 1;
}

void
mp3_print_xing(struct mp3_vbr_xing *v)
{
	MSG("XING %cBR header: flags=%x", (v->vbr ? 'V' : 'C'), v->flags);
	if (v->flags & 1)
		MSG(", frames=%u", v->frames);
	if (v->flags & 2)
		MSG(", bytes=%u", v->bytes);
	if (v->flags & 4)
		MSG(", TOC");
	if (v->flags & 8)
		MSG(", q=%d", v->quality);
	MSG("\n");

	if (!v->lame)
		return;
	MSG("LAME header: encver=%s, tagver=%d\n", v->lame_ver, v->lame_tag_ver);
	MSG("\tBitrate: %s %d\n",
		((char*[]){ "?BR0", "CBR", "ABR", "VBR1", "VBR2", "VBR3", "VBR4", "?BR7",
		 	    "CBR-2pass", "ABR 2-pass", "?BR10", "?BR11", "?BR12", "?BR13", "?BR14", "?BR15" }) [v->vbr_method],
		v->specified_bitrate);
	MSG("\tlowpass=%d00, peak=%08x, rrg=%04x, arg=%04x, gain=%d\n",
		v->lowpass, v->peak, v->radio_replay_gain, v->audiophile_replay_gain, v->mp3_gain);
	MSG("\tenc_flags=%x, ath_type=%d, enc_dly=%d, enc_pad=%d, noise=%d, stereo=%d, unwise=%d, ",
		v->enc_flags, v->ath_type, v->enc_delay, v->enc_padding, v->noise_shaping, v->stereo_mode, v->unwise_settings);
	MSG("srcfreq=%d, surround=%d, preset=%d\n",
		v->source_sfreq, v->surround, v->preset);
	MSG("\tmusic_bytes=%u, music_crc=%04x, tag_crc=%04x\n",
		v->music_bytes, v->music_crc, v->tag_crc);
}
