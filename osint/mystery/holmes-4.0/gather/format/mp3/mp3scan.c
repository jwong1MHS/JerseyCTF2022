/*
 *	A Simple Utility for Analysing MP3 Files
 *
 *	(c) 2006--2007 Martin Mares <mj@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/fastbuf.h"
#include "ucw/bbuf.h"
#include "ucw/mempool.h"
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
Usage: mp3scan [<options>] [<files>]\n\
\n\
Options:\n\
-v\tShow all MP3 frames encountered\n\
-t\tShow all MP3 tags encountered\n\
\n\
Output format (tab-separated columns):\n\
1\tnumber of frames\n\
2\tnumber of unsychronized bytes\n\
3\ttag types encountered (1,2,X/L/V)\n\
4\tbitrate: average(min-max)\n\
5\tsample rate: average(min-max)\n\
6\tplaying time in seconds\n\
7\tquoted file name\n\
");
	exit(1);
}

static int verbose;
static int show_tags;

static struct mempool *tag_pool;

static void
print_quoted(char *n)
{
	putchar('"');
	while (*n) {
		if (*n == '"')
			putchar('\\');
		putchar(*n++);
	}
	putchar('"');
}

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

static void
show_body(struct mp3_frame *fr)
{
	struct mp3_side_info si;
	if (mp3_parse_side_info(fr, &si))
		mp3_print_side_info(&si);
	else
		printf("\tUnable to parse side info\n");
}

static int
scan_body(struct mp3_frame *fr)
{
	struct mp3_header *h = fr->hdr;
	struct mp3_vbr_fraunhofer vf;
	struct mp3_vbr_xing vx;
	if (mp3_parse_vbr(fr, &vf)) {
		if (verbose) {
			mp3_print_vbr(&vf);
			printf("\tEstimated time: %.2f\n", (double)h->samples_per_frame * vf.frames / h->samplerate);
		}
		return 'V';
	} else if (mp3_parse_xing(fr, &vx)) {
		if (verbose) {
			mp3_print_xing(&vx);
			if (vx.frames)
				printf("\tEstimated time: %.2f\n", (double)h->samples_per_frame * vx.frames / h->samplerate);
		}
		return (vx.lame ? 'L' : 'X');
	} else if (verbose > 1)
		show_body(fr);
	return 0;
}

static void
show_id3v1(struct mp3_frame *fr)
{
	mp_flush(tag_pool);
	struct odes *o = id3v1_parse(fr, tag_pool);
	obj_dump_indented(o, 1);
}

static void
show_id3v2(struct mp3_frame *fr)
{
	mp_flush(tag_pool);
	struct id3v2_tag_set *ts = id3v2_new_set(tag_pool);
	id3v2_parse(ts, fr);
	id3v2_dump(ts);
}

static void
scan(struct mp3_file *f)
{
	double total_time = 0;
	int tag1 = 0, tag2 = 0, tag_vbr = 0, tag_ape = 0, tag_lyrics = 0, unsynced = 0, frames = 0;
	int sr_max = 0, sr_min = 1000000;
	int br_max = 0, br_min = 1000000;
	long long int sr_sum = 0, br_sum = 0;
	struct mp3_header h;
	struct mp3_frame fr;
	bb_t frame_buf;
	bb_init(&frame_buf);
	tag_pool = mp_new(4096);

	for (;;) {
		int ok = mp3_read_header(f, &h);
		if (h.unsynced_bytes_skipped && verbose)
			printf("%x: %d unsynced bytes\n", (int)btell(f->fb) - h.hdr_len - h.unsynced_bytes_skipped, h.unsynced_bytes_skipped);
		unsynced += h.unsynced_bytes_skipped;
		if (!ok)
			break;
		if (verbose || show_tags && h.type != FT_MP3)
			mp3_print_header(&h);
		if (!read_body(&fr, f, &h, &frame_buf))
			break;
		switch (h.type) {
		case FT_MP3: ;
			int vbr = scan_body(&fr);
			if (vbr)
				tag_vbr = vbr;
			else {
				frames++;
				if (h.samplerate < sr_min)
					sr_min = h.samplerate;
				if (h.samplerate > sr_max)
					sr_max = h.samplerate;
				sr_sum += h.samplerate;
				if (h.bitrate < br_min)
					br_min = h.bitrate;
				if (h.bitrate > br_max)
					br_max = h.bitrate;
				br_sum += h.bitrate;
				total_time += (double)h.samples_per_frame / h.samplerate;
			}
			h.bytes_per_frame = 0;
			break;
		case FT_ID3v1:
			if (show_tags)
				show_id3v1(&fr);
			tag1++;
			break;
		case FT_ID3v2:
			if (show_tags)
				show_id3v2(&fr);
			tag2++;
			break;
		case FT_APE:
			tag_ape++;
			break;
		case FT_LYRICS:
			tag_lyrics++;
			break;
		}
	}
	printf("%d\t%d\t%c%c%c%c%c", frames, unsynced, (tag1 ? '1' : '-'), (tag2 ? '2' : '-'), (tag_vbr ? : '-'), (tag_ape ? 'A' : '-'), (tag_lyrics ? 'l' : '-'));
	if (frames) {
		printf("\t%d", (int)(br_sum / frames));
		if (br_min != br_max)
			printf("(%d-%d)", br_min, br_max);
		printf("\t%d", (int)(sr_sum / frames));
		if (sr_min != sr_max)
			printf("(%d-%d)", sr_min, sr_max);
	} else {
		printf("\t-\t-");
	}
	printf("\t%.2f\t", total_time);
	print_quoted(f->fb->name);
	putchar('\n');

	mp_delete(tag_pool);
	bb_done(&frame_buf);
}

int
main(int argc, char **argv)
{
	int opt;
	while ((opt = getopt(argc, argv, "tv")) >= 0)
		switch (opt) {
		case 't':
			show_tags++;
			break;
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
