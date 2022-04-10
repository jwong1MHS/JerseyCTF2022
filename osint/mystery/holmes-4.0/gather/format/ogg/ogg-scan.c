/*
 *	OGG Parser - A Simple Scanner
 *
 * 	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "gather/format/ogg/ogg.h"
#include "gather/format/ogg/ogg-vorbis.h"

#include <stdlib.h>
#include <stdio.h>
#include <setjmp.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

static void NONRET
usage(void)
{
  fputs("\
Usage: ogg-scan [options] <file> ...\n\
\n\
Options:\n\
-v, --verbose=<level>   Set verbosity level (default=0)\n\
-f, --fast              Parse only header and tail\n\
" CF_USAGE, stderr);
  exit(1);
}

static char *shortopts = "v:f" CF_SHORT_OPTS;
static struct option longopts[] = {
  CF_LONG_OPTS
  { "verbose",			1, 0, 'v' },
  { "fast",			0, 0, 'f' },
  { NULL,                       0, 0, 0 }
};

static uns verbose_level;
static uns fast;

static uns
scan(struct ogg_file *f)
{
  OGG_TRY(f)
    {
      if (fast)
        {
	  ogg_vorbis_scan_head(f);
	  ogg_vorbis_scan_tail(f);
	}
      else
	ogg_vorbis_scan_file(f);
    }
  OGG_CATCH
    {
      if (try_code == OGG_ERR_FATAL)
        return 0;
    }
  OGG_TRY_END;
  return 1;
}

int
main(int argc, char **argv)
{
  int opt;
  log_init(argv[0]);
  cf_def_file = NULL;
  while ((opt = cf_getopt(argc, argv, shortopts, longopts, NULL)) >= 0)
    switch (opt)
      {
	case 'v':
	  verbose_level = atol(optarg);
	  break;
	case 'f':
	  fast++;
	  break;
	default:
	  usage();
      }
  if (optind == argc)
    usage();
 
  struct ogg_file f;
  struct fastbuf *out = bfdopen_shared(1, 4096);
  while (optind < argc)
    {
      log(L_INFO, "Scanning %s", argv[optind]);
      ogg_init(&f);
      f.fb = bopen(argv[optind++], O_RDONLY, 8192);
      f.msg_level = verbose_level << 8;
      uns success = scan(&f);
      if (f.stream_types & OGGS_TYPE_VIDEO)
	bputsn(out, "Detected video content");
      else if (f.stream_types & OGGS_TYPE_UNDEF)
	bputsn(out, "Found unknown content");
      else if (!success)
	bputsn(out, "Undecodable content");
      else
        CLIST_FOR_EACH(struct ogg_stream *, s, f.streams)
	  if (!(s->flags & OGGS_ERROR))
	    if (s->codec == &ogg_vorbis_codec)
	      {
	        struct ogg_vorbis_stream *v = s->user;
		if (!(v->flags & OGGS_VORBIS_ID_HEADER_VALID))
		  continue;
	        bprintf(out, "Vorbis stream:\n");
	        bprintf(out, "  Channels = %u\n", v->audio_channels);
	        bprintf(out, "  Sample rate = %u\n", v->audio_sample_rate);
	        if (v->bitrate_maximum > 0)
	          bprintf(out, "  Bitrate maximum = %d\n", v->bitrate_maximum);
	        if (v->bitrate_nominal > 0)
	          bprintf(out, "  Bitrate nominal = %d\n", v->bitrate_nominal);
	        if (v->bitrate_minimum > 0)
	          bprintf(out, "  Bitrate minimum = %d\n", v->bitrate_minimum);
		bprintf(out, "  Block size = %u/%u\n", v->blocksize_0, v->blocksize_1);
		if (v->flags & OGGS_VORBIS_COMMENT_HEADER_VALID)
		  {
		    if (v->vendor)
	              bprintf(out, "  Vendor = `%s'\n", v->vendor);
	            for (uns i = 0; i < v->comment_count; i++)
		      bprintf(out, "  Comment `%s' = `%s'\n", v->comments[i].name, v->comments[i].value);
		    if (~v->start_sample && ~v->end_sample)
		      bprintf(out, "  Length = %llu samples = %llu seconds\n", (long long)(v->end_sample - v->start_sample),
		          (long long)((v->end_sample - v->start_sample) / v->audio_sample_rate));
		  }
	      }
      bflush(out);
      ogg_cleanup(&f);
    }
  bclose(out);
  return 0;
}
