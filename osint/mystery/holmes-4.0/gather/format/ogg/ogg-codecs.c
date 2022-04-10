/*
 *	OGG Parser - Detecting of Various Codecs
 *
 * 	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "ucw/lib.h"
#include "gather/format/ogg/ogg.h"
#include "gather/format/ogg/ogg-codecs.h"

#include <string.h>

#define STRACE(s, l, ...) ogg_stream_msg((s), L_DEBUG | ((l) << 8), __VA_ARGS__)

static uns
ogg_speex_detect(byte *packet, uns len)
{
  return (len >= 8) && !memcmp(packet, "Speex   ", 8);
}

struct ogg_codec ogg_speex_codec = {
  .name = "Speex",
  .type = OGGS_TYPE_AUDIO,
  .detect = ogg_speex_detect,
};

static uns
ogg_flac_detect(byte *packet, uns len)
{
  return (len >= 4) && !memcmp(packet, "fLaC", 4);
}

struct ogg_codec ogg_flac_codec = {
  .name = "FLAC",
  .type = OGGS_TYPE_AUDIO,
  .detect = ogg_flac_detect,
};

static uns
ogg_theora_detect(byte *packet, uns len)
{
  return (len >= 7) && !memcmp(packet, "\x80theora", 7);
}

struct ogg_codec ogg_theora_codec = {
  .name = "Theora",
  .type = OGGS_TYPE_VIDEO,
  .detect = ogg_theora_detect,
};

static uns
ogg_writ_detect(byte *packet, uns len)
{
  return (len >= 5) && !memcmp(packet, "\0writ", 5);
}

struct ogg_codec ogg_writ_codec = {
  .name = "Writ",
  .type = OGGS_TYPE_TEXT,
  .detect = ogg_writ_detect,
};
