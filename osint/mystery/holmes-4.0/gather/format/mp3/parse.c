/*
 *	Sherlock Gatherer: MP3 Parser
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "ucw/fastbuf.h"
#include "ucw/bbuf.h"
#include "ucw/mempool.h"
#include "ucw/conf.h"
#include "gather/gather.h"
#include "gather/format/audio.h"
#include "gather/format/mp3/mp3.h"
#include "gather/format/mp3/id3.h"

#include <stdlib.h>
#include <errno.h>

static uns mp3_trace;
static uns mp3_warnings;
static uns mp3_max_warnings = ~0U;
static uns mp3_parse_frames;

static uns num_warnings;
enum {
  PRIOR_EST,
  PRIOR_ID3v2,
  PRIOR_VBR,
  PRIOR_XING,
  PRIOR_SUM,
};

#define TRACE(x...) do { if (mp3_trace) log(L_DEBUG, x); } while (0)

static struct cf_section mp3_config = {
  CF_ITEMS{
    CF_UNS("Trace", &mp3_trace),
    CF_UNS("Warnings", &mp3_warnings),
    CF_UNS("MaxWarnings", &mp3_max_warnings),
    CF_UNS("ParseFrames", &mp3_parse_frames),
    CF_END
  }
};

static void CONSTRUCTOR
mp3_init_config(void)
{
  cf_declare_section("MP3", &mp3_config, 0);
}

static void
warn(struct mp3_file *f UNUSED, char *msg, va_list args)
{
  if (++num_warnings <= mp3_max_warnings && mp3_warnings)
    vmsg(L_WARN_R, msg, args);
}

static int
read_body(struct mp3_frame *fr, struct mp3_file *f, struct mp3_header *h, bb_t *buf)
{
  bb_grow(buf, h->bytes_per_frame);
  int n = bread(f->fb, buf->ptr, h->bytes_per_frame);
  if (n < h->bytes_per_frame)
    {
      /* Heuristics for padding truncated final frames */
      if (n < 32)
  	return 0;
      bzero(buf->ptr + n, h->bytes_per_frame - n);
    }
  mp3_init_frame(fr, f, h, buf->ptr);
  return 1;
}

static int
scan_frame(struct mp3_frame *fr)
{
  struct mp3_header *h = fr->hdr;
  struct mp3_vbr_fraunhofer vf;
  struct mp3_vbr_xing vx;
  if (mp3_parse_vbr(fr, &vf))
    {
      if (h->samplerate)
	audio_set_value(&audio_len, 0.5 + (double)h->samples_per_frame * vf.frames / h->samplerate, PRIOR_VBR);
      return 1;
    }
  if (mp3_parse_xing(fr, &vx))
    {
      if (vx.frames)
        {
          if (h->samplerate)
	    audio_set_value(&audio_len, 0.5 + (double)h->samples_per_frame * vx.frames / h->samplerate, PRIOR_XING);
	  return 1;
	}
    }
  return 0;
}

static void
add_tag(struct odes *o)
{
  for (struct oattr *t = obj_find_attr(o, 'T' + OBJ_ATTR_SON); t; t = t->same)
    {
      byte *id = obj_find_aval(t->son, 'F');
      if (!id)
	continue;
      uns attr = 'X', src_attr = 'T', type = WT_TEXT;
      if (strlen(id) == 4)
	switch (ID3_ID(id[0], id[1], id[2], id[3]))
	  {
	    case ID3_ID('T','I','T','1'):	/* Content group description */
	    case ID3_ID('T','I','T','2'):	/* Title/Songname/Content description */
	    case ID3_ID('T','I','T','3'):	/* Subtitle/Description refinement */
	      attr = 'N'; /* MT_TITLE */
	      break;
	    case ID3_ID('T','A','L','B'):	/* Album/Movie/Show title */
	    case ID3_ID('T','O','A','L'):	/* Original album/movie/show title */
	      attr = 'A'; /* MT_AUDIO_ALBUM */
	      break;
	    case ID3_ID('T','P','E','1'):	/* Lead artist/Lead performer/Soloist/Performing group */
	    case ID3_ID('T','P','E','2'):	/* Band/Orchestra/Accompaniment */
	    case ID3_ID('T','P','E','3'):	/* Conductor */
	    case ID3_ID('T','P','E','4'):	/* Interpreted, remixed, or otherwise modified by */
	    case ID3_ID('T','O','P','E'):	/* Original artist/performer */
	      attr = 'I'; /* MT_AUDIO_AUTHOR */
	      break;
	    case ID3_ID('T','C','O','N'):	/* Genre */
	      src_attr = 'G';
	      attr = 'G'; /* MT_AUDIO_GENRE */
	      break;
	    case ID3_ID('T','L','E','N'):	/* Length in ms */
	      {
	        src_attr = 0;
	        byte *a = obj_find_aval(t->son, 'T');
		if (a && *a)
		  {
	            char *end;
		    errno = 0;
	            uns l = strtoul(a, &end, 10);
	            if (errno != ERANGE && !*end && l)
		      audio_set_value(&audio_len, l / 1000, PRIOR_ID3v2);
		  }
	        break;
	      }
	    case ID3_ID('T','Y','E','R'):	/* Year of recording */
	      attr = 'D';
	      break;
	    case ID3_ID('T','R','C','K'):	/* Track number */
	      attr = 'T';
	      break;
	  }
      /* Copy values */
      if (src_attr)
        for (struct oattr *a = obj_find_attr(t->son, src_attr); a; a = a->same)
	  if (attr == 'X')
	    audio_add_text(type, a->val);
          else if (attr == 'M')
	    audio_add_meta(type, a->val);
          else
	    audio_add_attr(attr, a->val);
    }
#if 0 // Store raw tags for debug purposes
  if (o->attrs)
    obj_add_attr_clone(audio_obj, o->attrs);
#endif
}

static void
scan_tag(struct mp3_frame *fr)
{
  switch (fr->hdr->type)
    {
      case FT_ID3v1:
  	{
	  TRACE("Found ID3v1 tag");
  	  add_tag(id3v1_parse(fr, gthis->pool));
  	  break;
	}
      case FT_ID3v2:
	{
	  TRACE("Found ID3v2 tag");
	  struct id3v2_tag_set *ts = id3v2_new_set(gthis->pool);
	  id3v2_parse(ts, fr);
	  add_tag(id3v2_set_to_obj(ts));
	  break;
	}
      default: ;
    }
}

int
mp3_parse(char **args UNUSED)
{
  double total_time = 0;
  int frames = 0, tags = 0;
  int sr_max = 0, sr_min = 1000000;
  int br_max = 0, br_min = 1000000;
  long long int sr_sum = 0, br_sum = 0;
  struct mp3_file f;
  struct mp3_header h;
  struct mp3_frame fr;
  bb_t frame_buf;
  int all = 0;
  int mono = 0, stereo = 0;

  num_warnings = 0;
  audio_start();
  bb_init(&frame_buf);
  mp3_init(&f);
  f.fb = fbmem_clone_read(gthis->contents);
  f.warn = warn;

  TRACE("Starting MP scan");
  while (mp3_read_header(&f, &h))
    {
      if (h.unsynced_bytes_skipped)
	mp3_warn(&f, "%x: %d unsynced bytes skipped\n", h.hdr_pos - h.unsynced_bytes_skipped, h.unsynced_bytes_skipped);
      if (!read_body(&fr, &f, &h, &frame_buf))
        {
	  all = 1;
	  break;
	}
      if (h.type == FT_MP3)
        {
	  if (scan_frame(&fr))
	    {
	      tags++;
	      if (!mp3_parse_frames)
	        {
		  TRACE("Terminating the scan");
	          break;
		}
	    }
	  else
	    {
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
	      if (h.channels == 3)
		mono++;
	      else
		stereo++;
	    }
	}
      else
	scan_tag(&fr);
      if (num_warnings > mp3_max_warnings)
	break;
    }
  bb_done(&frame_buf);
  bclose(f.fb);
  mp3_cleanup(&f);

  if (num_warnings > mp3_max_warnings)
    {
      audio_cancel();
      gerror(2200, "Too many warnings while parsing MP3 file");
    }
  if (!frames && all)
    {
      audio_cancel();
      gerror(2200, "No MP3 frames found");
    }
  audio_set_value(&audio_channels, (mono > stereo) ? 1 : (mono < stereo) ? 2 : -1, PRIOR_SUM);
  if (!gthis->truncated && all)
    {
      /* We have parsed the entire file */
      if (frames)
        {
          audio_set_value(&audio_bitrate, br_sum / frames, PRIOR_SUM);
          audio_set_value(&audio_srate, sr_sum / frames, PRIOR_SUM);
        }
      audio_set_value(&audio_len, 0.5 + total_time, PRIOR_SUM);
    }
  else
    {
      /* We have only parsed a part of MP3 */
      if (frames)
        {
          audio_set_value(&audio_bitrate, br_sum / frames, PRIOR_EST);
          audio_set_value(&audio_srate, sr_sum / frames, PRIOR_EST);
	}
      uns file_size = gthis->truncated ? (uns)gthis->expected_size : (uns)gthis->orig_size;
      uns parsed_size = btell(gthis->contents);
      if (file_size && audio_bitrate.value > 0)
        {
	  TRACE("Estimating audio length from incomplete file");
	  audio_set_value(&audio_len, 0.5 + total_time + (double)(file_size - parsed_size) / audio_bitrate.value * 8, PRIOR_EST);
	}
    }
  audio_end();
  return 1;
}
