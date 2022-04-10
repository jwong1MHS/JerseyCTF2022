/*
 *	Parsing of ID3v1 Tags
 *
 *	(c) 2007 Martin Mares <mj@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "sherlock/object.h"
#include "charset/charconv.h"
#include "mp3.h"
#include "id3.h"
#include "bitstream.h"

#include <stdio.h>
#include <string.h>

static uns id3v1_charset = CONV_CHARSET_WIN1250;	// FIXME: Make configurable; non-UTF-8

static void
id3v1_attr(struct odes *o, uns attr, byte *a, uns len)
{
	if (!a[0])
		return;
	struct conv_context cc;
	conv_init(&cc);
	conv_set_charset(&cc, id3v1_charset, CONV_CHARSET_UTF8);
	struct mempool *pool = o->pool;
	byte *p = mp_start(pool, 1);
	uns nonspc = 0;
	while (len--) {
		uns u = *a++;
		if (!u)
			break;
		u = conv_in_to_ucs(&cc, u);
		if (u < 0x20 || u >= 0x7f && u < 0xa0 || u == UNI_REPLACEMENT)
			continue;
		if (u != ' ')
			nonspc++;
		p = utf8_put(mp_spread(pool, p, 4), u);
	}
	if (nonspc) {
		*p++ = 0;
		obj_add_attr_ref(o, attr, mp_end(pool, p));
	} else
		mp_end(pool, mp_ptr(pool));
}

struct odes *
id3v1_parse(struct mp3_frame *fr, struct mempool *pool)
{
	ASSERT(fr->hdr->hdr_len == 4 && fr->hdr->bytes_per_frame == 124 && fr->bs.remains == 124);
	byte tag[128];
	memcpy(tag, fr->hdr->hdr, 4);		// Reconstruct the tag
	memcpy(tag+4, fr->bs.pos, 124);
	struct odes *o = obj_new(pool);
	id3v1_attr(o, 'T', tag+3, 30);		// Title
	id3v1_attr(o, 'A', tag+33, 30);		// Artist
	id3v1_attr(o, 'B', tag+63, 30);		// Album
	id3v1_attr(o, 'Y', tag+93, 4);		// Year
	id3v1_attr(o, 'C', tag+97, 30);		// Comment
	byte *genre = id3_genre_tab(tag[127]);	// Genre
	if (genre)
		obj_add_attr_ref(o, 'G', genre);
	if (!tag[125] && tag[126])		// ID3v1.1 extension: number of tracks
		obj_set_attr_num(o, 'N', tag[126]);
	return o;
}
