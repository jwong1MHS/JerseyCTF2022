/*
 *	OGG Parser - Detectors of Various Codecs
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 *	References:
 *	- http://www.speex.org/docs/manual/speex-manual/node8.html
 *	- http://flac.sourceforge.net/documentation_format_overview.html
 *	- http://theora.org/doc/Theora_I_spec.pdf
 *	- http://wiki.xiph.org/OggWrit
 */

#ifndef _SHERLOCK_GATHER_FORMAT_OGG_OGG_CODECS_H
#define _SHERLOCK_GATHER_FORMAT_OGG_OGG_CODECS_H

#include "gather/format/ogg/ogg.h"

/* Audio codecs */
extern struct ogg_codec ogg_speex_codec;
extern struct ogg_codec ogg_flac_codec;

/* Video codecs */
extern struct ogg_codec ogg_theora_codec;

/* Text codecs */
extern struct ogg_codec ogg_writ_codec;

#endif
