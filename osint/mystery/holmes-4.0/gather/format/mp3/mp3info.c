/*
 *	A Simple Utility for Analysing MP3 Files
 *
 *	(c) 2006--2007 Martin Mares <mj@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/bbuf.h"
#include "sherlock/object.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <fcntl.h>

#include "mp3.h"
#include "id3.h"

static void
usage(void)
{
	fprintf(stderr, "\
Usage: mp3info [<options>] [<files>]\n\
\n\
Options:\n\
-v\tShow all MP3 frames encountered\n\
");
	exit(1);
}

static int verbose;

static struct mempool *tag_pool;

static int
read_body(struct mp3_frame *fr, struct mp3_file *f, struct mp3_header *h, bb_t *buf)
{
	bb_grow(buf, h->bytes_per_frame);
	int n = bread(f->fb, buf->ptr, h->bytes_per_frame);
	if (n < h->bytes_per_frame) {
		/* Heuristics for padding truncated final frames */
		if (n < 32)
			return 0;
		bzero(buf->ptr + n, h->bytes_per_frame - n);
		if (verbose)
			mp3_warn(f, "Last frame was found truncated");
	}
	mp3_init_frame(fr, f, h, buf->ptr);
	return 1;
}

static int
scan_frame(struct mp3_frame *fr)
{
	struct mp3_header *h = fr->hdr;
	struct mp3_vbr_fraunhofer vf;
	if (mp3_parse_vbr(fr, &vf)) {
		if (verbose)
			mp3_print_vbr(&vf);
		printf("\tEstimated time: %.2f\n", (double)h->samples_per_frame * vf.frames / h->samplerate);
		return 1;
	}

	struct mp3_vbr_xing vx;
	if (mp3_parse_xing(fr, &vx)) {
		if (verbose)
			mp3_print_xing(&vx);
		if (vx.frames) {
			printf("\tEstimated time: %.2f\n", (double)h->samples_per_frame * vx.frames / h->samplerate);
			return 1;
		}
	}

	return 0;
}

static void
scan_tag(struct mp3_frame *fr)
{
	mp_flush(tag_pool);
	switch (fr->hdr->type) {
	case FT_ID3v1: {
		struct odes *o = id3v1_parse(fr, tag_pool);
		printf("ID3v1 tag\n");
		obj_dump_indented(o, 1);
		break;
		}
	case FT_ID3v2: {
		struct id3v2_tag_set *ts = id3v2_new_set(tag_pool);
		id3v2_parse(ts, fr);
		id3v2_dump(ts);
		break;
		}
	default: ;
	}
}

static void
scan(struct mp3_file *f)
{
	struct mp3_header h;
	struct mp3_frame fr;
	int limit = 1048576;
	bb_t frame_buf;

	ucw_off_t len = bfilesize(f->fb);
	if (len >= 0x7fffffff) {
		mp3_warn(f, "File too large");
		return;
	}
	bb_init(&frame_buf);
	tag_pool = mp_new(4096);

	if (len > 0) {
		h.hdr_pos = len;
		int ok = mp3_scan_tag_last(f, &h);
		while (ok > 0) {
			limit = MIN(limit, h.hdr_pos);
			if (h.unsynced_bytes_skipped && verbose)
				printf("%x: %d unsynced bytes skipped\n", h.hdr_pos + h.hdr_len + h.bytes_per_frame, h.unsynced_bytes_skipped);
			if (verbose)
				mp3_print_header(&h);
			if (read_body(&fr, f, &h, &frame_buf))
				scan_tag(&fr);
			ok = mp3_scan_tag_back(f, &h);
		}
		bsetpos(f->fb, 0);
	}

	f->limit = limit;
	int frames = 0;
	while (mp3_read_header(f, &h)) {
		if (h.unsynced_bytes_skipped && verbose)
			printf("%x: %d unsynced bytes skipped\n", h.hdr_pos - h.unsynced_bytes_skipped, h.unsynced_bytes_skipped);
		if (verbose)
			mp3_print_header(&h);
		if (!read_body(&fr, f, &h, &frame_buf))
			break;
		if (h.type == FT_MP3) {
			if (frames++ > 256)
				break;
			if (scan_frame(&fr))
				break;
		} else
			scan_tag(&fr);
	}

	mp_delete(tag_pool);
	bb_done(&frame_buf);
}

int
main(int argc, char **argv)
{
	int opt;
	while ((opt = getopt(argc, argv, "v")) >= 0)
		switch (opt) {
		case 'v':
			verbose++;
			break;
		default:
			usage();
		}

	if (optind < argc) {
		while (optind < argc) {
			struct mp3_file f;
			mp3_init(&f);
			printf("%s\n", argv[optind]);
			f.fb = bopen_try(argv[optind], O_RDONLY, 65536);
			if (f.fb) {
				scan(&f);
				bclose(f.fb);
			} else
				fprintf(stderr, "%s: Cannot open: %m\n", argv[optind]);
			optind++;
		}
	} else {
		struct mp3_file f;
		mp3_init(&f);
		f.fb = bfdopen_shared(0, 65536);
		f.fb->name = "stdin";
		scan(&f);
		bclose(f.fb);
	}
	return 0;
}
