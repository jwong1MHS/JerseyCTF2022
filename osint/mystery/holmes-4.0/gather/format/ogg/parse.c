/*
 *	Sherlock Gatherer: OGG Parser
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "ucw/fastbuf.h"
#include "gather/gather.h"
#include "gather/format/audio.h"
#include "gather/format/ogg/ogg.h"
#include "gather/format/ogg/ogg-vorbis.h"

static uns num_warnings;

static void
my_msg(struct ogg_file *ogg, uns type, byte *m)
{
  switch (type & 0xff)
    {
      case 'W':
      case 'E':
      case 'w':
      case 'e':
	if (++num_warnings >= ogg_max_warnings)
	  {
	    log(L_ERROR_R, "Ogg: Too many warnings, terminating");
	    ogg_throw(ogg, OGG_ERR_FATAL);
	  }
	if (ogg_warnings)
	  log(type & 0xff, m);
	break;
      case 'D':
	if (ogg_trace)
	  log(L_DEBUG, "%s", m);
	break;
      default:
	/* Ignore debug messages */
	break;
    }
}

static int
scan(struct ogg_file *ogg)
{
  OGG_TRY(ogg)
    ogg_vorbis_scan_file(ogg);
  OGG_CATCH
    if (try_code == OGG_ERR_FATAL)
      return 0;
  OGG_TRY_END;
  return 1;
}

static void
estimate_rates(struct ogg_vorbis_stream *v)
{
  if (v->audio_sample_rate > 0)
    audio_set_value(&audio_srate, v->audio_sample_rate, 0);
  if (v->audio_channels > 0)
    audio_set_value(&audio_channels, v->audio_channels == 1 ? 1 : 2, 0);
  if (v->bitrate_nominal > 0)
    audio_set_value(&audio_bitrate, v->bitrate_nominal, 0);
  else if (v->bitrate_minimum > 0 && v->bitrate_maximum > 0)
    audio_set_value(&audio_bitrate, (v->bitrate_minimum + v->bitrate_maximum) / 2, 0);
  else if (v->bitrate_minimum > 0)
    audio_set_value(&audio_bitrate, v->bitrate_minimum, 0);
  else if (v->bitrate_maximum > 0)
    audio_set_value(&audio_bitrate, v->bitrate_maximum, 0);
}

static void
estimate_length(struct ogg_vorbis_stream *v)
{
  u64 len = v->end_sample;
  if (~len && ~v->start_sample)
    len -= v->start_sample;
  if (!gthis->truncated)
    {
      if (~len && audio_srate.value > 0)
        audio_set_value(&audio_len, len / audio_srate.value, 0);
    }
  else
    {
      uns file_size = gthis->truncated ? (uns)gthis->expected_size : (uns)gthis->orig_size;
      uns parsed_size = btell(gthis->contents);
      if (file_size && audio_bitrate.value > 0 && ~len && audio_srate.value > 0)
	audio_set_value(&audio_len, 0.5 + (double)len / audio_srate.value + (double)(file_size - parsed_size) / audio_bitrate.value * 8, 0);
    }
}

int
ogg_parse(char **args UNUSED)
{
  struct ogg_file ogg;
  audio_start();
  ogg_init(&ogg);
  ogg.fb = fbmem_clone_read(gthis->contents);
  ogg.msg = my_msg;
  if (!scan(&ogg))
    goto err;
  CLIST_FOR_EACH(struct ogg_stream *, s, ogg.streams)
    {
      if (s->codec != &ogg_vorbis_codec)
        continue;
      struct ogg_vorbis_stream *v = s->user;
      if (!(v->flags & OGGS_VORBIS_ID_HEADER_VALID))
	continue;

      estimate_rates(v);
      estimate_length(v);

      if (v->flags & OGGS_VORBIS_COMMENT_HEADER_VALID)
        {
	  for (uns i = 0; i < v->comment_count; i++)
	    {
	      struct ogg_vorbis_comment *c = v->comments + i;
	      if (!strcasecmp(c->name, "title"))
	        audio_add_attr('N', c->value);
	      else if (!strcasecmp(c->name, "album"))
		audio_add_attr('A', c->value);
	      else if (!strcasecmp(c->name, "artist") || !strcasecmp(c->name, "performer"))
		audio_add_attr('I', c->value);
	      else if (!strcasecmp(c->name, "genre"))
		audio_add_attr('G', c->value);
	      else if (!strcasecmp(c->name, "tracknumber"))
		audio_add_attr('T', c->value);
	      else if (!strcasecmp(c->name, "date"))
		audio_add_attr('D', c->value);
	      else
		audio_add_text(WT_TEXT, c->value);
	    }
	}
      ogg_cleanup(&ogg);
      audio_end();
      set_content_type("audio/x-vorbis");
      return 1;
    }
err:
  ogg_cleanup(&ogg);
  audio_cancel();
  gerror(2200, "Undecodable OGG");
}
