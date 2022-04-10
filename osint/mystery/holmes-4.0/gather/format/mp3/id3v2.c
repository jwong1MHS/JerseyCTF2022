/*
 *	Parsing of ID3v2 Tags
 *
 *	(c) 2007 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "ucw/lib.h"
#include "ucw/mempool.h"
#include "ucw/unaligned.h"
#include "ucw/unicode.h"
#include "ucw/chartype.h"
#include "sherlock/object.h"
#include "charset/charconv.h"
#include "mp3.h"
#include "id3.h"
#include "bitstream.h"

#include <stdio.h>
#include <string.h>

static uns id3v2_charset = CONV_CHARSET_WIN1250;	// FIXME: Make configurable

/*** General parsing functions ***/

static uns
read_sync_safe_int(struct bit_stream *s, byte *x)
{
	if ((x[0] | x[1] | x[2] | x[3]) & 0x80)
		bs_error(s, "ID3v2: Invalid synch-safe integer");
	return ((x[0] << 21) | (x[1] << 14) | (x[2] << 7) | x[3]);
}

uns
id3v2_parse_byte(struct id3v2_item *it)
{
	struct bit_stream *s = &it->tag->bs;
	return *bs_get(s, 1, NULL);
}

byte *
id3v2_parse_fixed_string(struct id3v2_item *it, int len)
{
	struct bit_stream *s = &it->tag->bs;
	byte *buf = mp_alloc_fast_noalign(it->tag->set->pool, len+1);
	byte *orig = bs_get(s, len, NULL);
	for (int i=0; i<len; i++)
		if (orig[i] >= 0x20 && orig[i] <= 0x7e)
			buf[i] = orig[i];
		else
			buf[i] = '?';
	buf[len] = 0;
	return buf;
}

static byte *
parse_utf8(struct bit_stream *s, struct mempool *pool)
{
	byte *p = mp_start(pool, 1);
	while (s->remains) {
		byte *u = bs_get(s, 1, NULL);
		uns uu = *u;
		if (!uu)
			break;
		if (uu >= 0x80 && uu < 0xc0 || uu >= 0xfe)
			bs_error(s, "ID3v2: Malformed UTF-8 sequence");
		bs_get(s, utf8_encoding_len(uu) - 1, "ID3v2: Truncated UTF-8 sequence");
		utf8_32_get(u, &uu);
		uu = unicode_sanitize_char(uu);
		p = utf8_put(mp_spread(pool, p, 4), uu);
	}
	*p++ = 0;
	return mp_end(pool, p);
}

static byte *
parse_utf16(struct bit_stream *s, int big_endian_p, struct mempool *pool)
{
	byte *p = mp_start(pool, 1);
	while (s->remains) {
		byte *u = bs_get(s, 2, "ID3v2: Truncated UTF-16 sequence");
		uns uu = (big_endian_p ? get_u16_be(u) : get_u16_le(u));
		if (!uu)
			break;
		uu = unicode_sanitize_char(uu);
		p = utf8_put(mp_spread(pool, p, 4), uu);
	}
	*p++ = 0;
	return mp_end(pool, p);
}

static byte *
parse_latin1(struct bit_stream *s, struct mempool *pool)
{
	// The specs say that the string should be in iso-8859-1, but reality disagrees,
	// so we have made the charset configurable.
	if (id3v2_charset == CONV_CHARSET_UTF8)
		return parse_utf8(s, pool);
	struct conv_context cc;
	conv_init(&cc);
	conv_set_charset(&cc, id3v2_charset, CONV_CHARSET_UTF8);
	byte *p = mp_start(pool, 1);
	while (s->remains) {
		uns u = *bs_get(s, 1, NULL);
		if (!u)
			break;
		u = conv_in_to_ucs(&cc, u);
		u = unicode_sanitize_char(u);
		p = utf8_put(mp_spread(pool, p, 4), u);
	}
	*p++ = 0;
	return mp_end(pool, p);
}

byte *
id3v2_parse_string(struct id3v2_item *it, int encoding)
{
	struct bit_stream *s = &it->tag->bs;
	struct mempool *pool = it->tag->set->pool;
	byte *bom;

	if (encoding < 0)
		encoding = *bs_get(s, 1, "ID3v2: Missing string encoding in item %c%c%c%c", ID3_NAME_CC(it->id));
	switch (encoding) {
		case 0:				// ISO-8859-1
			return parse_latin1(s, pool);
		case 1:				// UTF-16 with BOM
			if (!s->remains)	// Illegal, but common.
				return mp_strdup(pool, "");
			bom = bs_get(s, 2, "ID3v2: Missing UTF-16 BOM");
			if (bom[0] == 0xff && bom[1] == 0xfe)
				return parse_utf16(s, 0, pool);
			else if (bom[0] == 0xfe && bom[1] == 0xff)
				return parse_utf16(s, 1, pool);
			else if (!bom[0] && !bom[1])	// Also illegal but common
				return mp_strdup(pool, "");
			else
				bs_error(s, "ID3v2: Invalid UTF-16 BOM");
		case 2:				// UTF-16-BE without BOM
			return parse_utf16(s, 1, pool);
		case 3:				// UTF-8
			return parse_utf8(s, pool);
		default:
			bs_error(s, "ID3v2: Unrecognized string encoding %02x in item %c%c%c%c", encoding, ID3_NAME_CC(it->id));
			return NULL;
	}
}

static void
id3v2_unsync_bitstream(struct bit_stream *s)
{
	byte *r = s->pos;
	byte *w = s->pos;
	byte *e = s->pos + s->remains;
	while (r < e) {
		if ((*w++ = *r++) == 0xff && r < e && *r == 0x00)
			r++;
	}
	s->remains = w - s->pos;
}

/*** Automatic upgrading of v2.2 tags ***/

static void
id3v2_upgrade_v2(struct id3v2_item *it)
{
	u32 v2_to_v3[][2] = {
		{ ID3_ID(' ','B','U','F'), ID3_ID('R','B','U','F') },
		{ ID3_ID(' ','C','N','T'), ID3_ID('P','C','N','T') },
		{ ID3_ID(' ','C','O','M'), ID3_ID('C','O','M','M') },
		{ ID3_ID(' ','E','T','C'), ID3_ID('E','T','C','O') },
		{ ID3_ID(' ','G','E','O'), ID3_ID('G','E','O','B') },
		{ ID3_ID(' ','M','L','L'), ID3_ID('M','L','L','T') },
		{ ID3_ID(' ','P','O','P'), ID3_ID('P','O','P','M') },
		{ ID3_ID(' ','R','E','V'), ID3_ID('R','V','R','B') },
		{ ID3_ID(' ','S','L','T'), ID3_ID('S','Y','L','T') },
		{ ID3_ID(' ','S','T','C'), ID3_ID('S','Y','T','C') },
		{ ID3_ID(' ','T','A','L'), ID3_ID('T','A','L','B') },
		{ ID3_ID(' ','T','B','P'), ID3_ID('T','B','P','M') },
		{ ID3_ID(' ','T','C','M'), ID3_ID('T','C','O','M') },
		{ ID3_ID(' ','T','C','O'), ID3_ID('T','C','O','N') },
		{ ID3_ID(' ','T','C','R'), ID3_ID('T','C','P','O') },
		{ ID3_ID(' ','T','D','Y'), ID3_ID('T','D','L','Y') },
		{ ID3_ID(' ','T','E','N'), ID3_ID('T','E','N','C') },
		{ ID3_ID(' ','T','F','T'), ID3_ID('T','F','L','T') },
		{ ID3_ID(' ','T','K','E'), ID3_ID('T','K','E','Y') },
		{ ID3_ID(' ','T','L','A'), ID3_ID('T','L','A','N') },
		{ ID3_ID(' ','T','L','E'), ID3_ID('T','L','E','N') },
		{ ID3_ID(' ','T','M','T'), ID3_ID('T','M','E','D') },
		{ ID3_ID(' ','T','O','A'), ID3_ID('T','O','P','E') },
		{ ID3_ID(' ','T','O','F'), ID3_ID('T','O','F','N') },
		{ ID3_ID(' ','T','O','L'), ID3_ID('T','O','L','Y') },
		{ ID3_ID(' ','T','O','T'), ID3_ID('T','O','A','L') },
		{ ID3_ID(' ','T','P','1'), ID3_ID('T','P','E','1') },
		{ ID3_ID(' ','T','P','2'), ID3_ID('T','P','E','2') },
		{ ID3_ID(' ','T','P','3'), ID3_ID('T','P','E','3') },
		{ ID3_ID(' ','T','P','4'), ID3_ID('T','P','E','4') },
		{ ID3_ID(' ','T','P','A'), ID3_ID('T','P','O','S') },
		{ ID3_ID(' ','T','P','B'), ID3_ID('T','P','U','B') },
		{ ID3_ID(' ','T','R','C'), ID3_ID('T','S','R','C') },
		{ ID3_ID(' ','T','R','K'), ID3_ID('T','R','C','K') },
		{ ID3_ID(' ','T','S','I'), ID3_ID('T','S','I','Z') },
		{ ID3_ID(' ','T','S','S'), ID3_ID('T','S','S','E') },
		{ ID3_ID(' ','T','T','1'), ID3_ID('T','I','T','1') },
		{ ID3_ID(' ','T','T','2'), ID3_ID('T','I','T','2') },
		{ ID3_ID(' ','T','T','3'), ID3_ID('T','I','T','3') },
		{ ID3_ID(' ','T','X','T'), ID3_ID('T','E','X','T') },
		{ ID3_ID(' ','T','X','X'), ID3_ID('T','X','X','X') },
		{ ID3_ID(' ','U','F','I'), ID3_ID('U','F','I','D') },
		{ ID3_ID(' ','U','L','T'), ID3_ID('U','S','L','T') },
		{ ID3_ID(' ','W','A','F'), ID3_ID('W','O','A','F') },
		{ ID3_ID(' ','W','A','R'), ID3_ID('W','O','A','R') },
		{ ID3_ID(' ','W','A','S'), ID3_ID('W','O','A','S') },
		{ ID3_ID(' ','W','C','M'), ID3_ID('W','C','O','M') },
		{ ID3_ID(' ','W','C','P'), ID3_ID('W','C','O','P') },
		{ ID3_ID(' ','W','P','B'), ID3_ID('W','P','U','B') },
		{ ID3_ID(' ','W','X','X'), ID3_ID('W','X','X','X') },
	};
	/*
	 *  XXX: We don't translate these fields yet:
	 *
	 *  TYE, TDA, TIM, TRD, TOR
	 *  IPL MCI RVA EQU PIC CRM CRA LNK
	 */
	for (uns i=0; i<ARRAY_SIZE(v2_to_v3); i++)
		if (v2_to_v3[i][0] == it->id) {
			it->id = v2_to_v3[i][1];
			return;
		}
}

/*** Parsing of basic structure of tags ***/

struct id3v2_tag_set *
id3v2_new_set(struct mempool *pool)
{
	struct id3v2_tag_set *ts = mp_alloc_zero(pool, sizeof(*ts));
	ts->pool = pool;
	clist_init(&ts->tags);
	return ts;
}

static void
id3v2_parse_v2(struct id3v2_tag *t)
{
	struct bit_stream *s = &t->bs;

	if (t->hdr_flags & 0x7f)
		bs_error(s, "Unsupported ID3v2.2 header flags %02x", t->hdr_flags);

	if (t->hdr_flags & 0x80)
		id3v2_unsync_bitstream(s);

	while (s->remains) {
		byte *th = bs_get(s, 1, NULL);
		if (!th[0]) {
			DBG("ID3: Found padding, stopping the scan");
			break;
		}
		bs_get(s, 5, "ID3v2.2 frame header truncated");
		struct id3v2_item *it = mp_alloc(t->set->pool, sizeof(*it));
		it->tag = t;
		it->id = ID3_ID(' ',th[0],th[1],th[2]);
		it->length = (th[3] << 16) | (th[4] << 8) | th[5];
		it->flags = 0;
		DBG("ID3: Frame `%.3s', len=%d", th, it->length);
		it->data = bs_get(s, it->length, "ID3v2.2 frame truncated");
		id3v2_upgrade_v2(it);
		clist_add_tail(&t->items, &it->n);
	}
}

static void
id3v2_parse_v3(struct id3v2_tag *t)
{
	struct bit_stream *s = &t->bs;

	if (t->hdr_flags & 0x3f)
		bs_error(s, "Unsupported ID3v2.3 header flags %02x", t->hdr_flags);

	if (t->hdr_flags & 0x80)
		id3v2_unsync_bitstream(s);

	if (t->hdr_flags & 0x40) {	// Extended header present
		byte *ehl = bs_get(s, 4, "ID3v2 extended header missing");
		uns ext_len = get_u32_be(ehl);
		DBG("ID3: Extended header of size %d", ext_len);
		struct bit_stream eh_bs;
		bs_subrange(s, &eh_bs, ext_len, "ID3v2 extended header truncated");
		// There is nothing useful in the extended header, so we ignore it for now.
	}

	while (s->remains) {
		byte *th = bs_get(s, 1, NULL);
		if (!th[0]) {
			DBG("ID3: Found padding, stopping the scan");
			break;
		}
		bs_get(s, 9, "ID3v2.3 frame header truncated");
		struct id3v2_item *it = mp_alloc(t->set->pool, sizeof(*it));
		it->tag = t;
		it->id = get_u32_be(th);
		it->length = get_u32_be(th+4);
		it->flags = get_u16_be(th+8);
		DBG("ID3: Frame `%.4s', len=%d, flags=%04x", th, it->length, it->flags);
		struct bit_stream it_bs;
		bs_subrange(s, &it_bs, it->length, "ID3v2.3 frame truncated");
		if (it->flags & 0x001f)
			bs_error(s, "ID3v2.3 unsupported frame flags %04x", it->flags);
		if (it->flags & 0x0020) {		// Grouping identity
			uns grp = *bs_get(&it_bs, 1, "ID3v2.4 grouping indentity truncated");
			bs_warn(&it_bs, "ID3v2.3 grouping identity ignored: %02x", grp);
		}
		if (it->flags & 0x0040) {
			bs_warn(&it_bs, "ID3v2.3 encrypted frame ignored");
			continue;
		}
		if (it->flags & 0x0080) {
			bs_warn(&it_bs, "ID3v2.3 compressed frame ignored");
			continue;
		}
		it->data = it_bs.pos;
		it->length = it_bs.remains;
		clist_add_tail(&t->items, &it->n);
	}
}

static void
id3v2_parse_v4(struct id3v2_tag *t)
{
	struct bit_stream *s = &t->bs;

	if (t->hdr_flags & 0x2f)
		bs_error(s, "Unsupported ID3v2.4 header flags %02x", t->hdr_flags);

	// CAVEAT: Flag 0x80 indicated unsynchronization in previous versions and the standard
	// still refers to it by this name. However, in v2.4 it's just a note that all
	// frames have been unsychronized properly, so we needn't care.

	if (t->hdr_flags & 0x10) {	// Footer present
		byte *foot = bs_pop(s, 10, "ID3v2.4 footer missing");
		if (memcmp(foot, "3DI", 3))
			bs_error(s, "ID3v2 footer malformed (%02x%02x%02x)", foot[0], foot[1], foot[2]);
		// We ignore the rest of the footer
	}

	if (t->hdr_flags & 0x40) {	// Extended header present
		byte *ehl = bs_get(s, 4, "ID3v2.4 extended header missing");
		uns ext_len = read_sync_safe_int(s, ehl);
		DBG("ID3: Extended header of size %d", ext_len);
		struct bit_stream eh_bs;
		bs_subrange(s, &eh_bs, ext_len, "ID3v2 extended header truncated");
		// There is nothing useful in the extended header, so we ignore it for now.
	}

	while (s->remains) {
		byte *th = bs_get(s, 1, NULL);
		if (!th[0]) {
			DBG("ID3: Found padding, stopping the scan");
			break;
		}
		bs_get(s, 9, "ID3v2.4 frame header truncated");
		struct id3v2_item *it = mp_alloc(t->set->pool, sizeof(*it));
		it->tag = t;
		it->id = get_u32_be(th);
		it->length = read_sync_safe_int(s, th+4);
		it->flags = get_u16_be(th+8);
		DBG("ID3: Frame `%.4s', len=%d, flags=%04x", th, it->length, it->flags);
		struct bit_stream it_bs;
		bs_subrange(s, &it_bs, it->length, "ID3v2 frame truncated");
		if (it->flags & 0x80b0) {
			bs_warn(s, "ID3v2.4 unsupported frame flags %04x", it->flags);
			continue;
		}
		if (it->flags & 0x0001) {
			bs_warn(&it_bs, "ID3v2.4 data length indicated frame");
			bs_get(&it_bs, 4, "ID3v2.4 data length indicator truncated");
		}
		if (it->flags & 0x0040) {		// Grouping identity
			uns grp = *bs_get(&it_bs, 1, "ID3v2.4 grouping indentity truncated");
			bs_warn(&it_bs, "ID3v2.4 grouping identity ignored: %02x", grp);
		}
		if (it->flags & 0x0002)			// Unsynchronization used
			id3v2_unsync_bitstream(&it_bs);
		if (it->flags & 0x0004) {
			bs_warn(&it_bs, "ID3v2.4 encrypted frame ignored");
			continue;
		}
		if (it->flags & 0x0008) {
			bs_warn(&it_bs, "ID3v2.4 compressed frame ignored");
			continue;
		}
		it->data = it_bs.pos;
		it->length = it_bs.remains;
		clist_add_tail(&t->items, &it->n);
	}
}

static struct id3v2_tag *
id3v2_do_parse(struct id3v2_tag_set *ts, struct mp3_frame *fr)
{
	struct bit_stream *s = &fr->bs;
	byte *hdr = fr->hdr->hdr;

	struct id3v2_tag *t = mp_alloc_zero(ts->pool, sizeof(*t));
	t->set = ts;
	t->version = hdr[3];
	t->revision = hdr[4];
	t->hdr_flags = hdr[5];
	clist_init(&t->items);
	uns tag_len = read_sync_safe_int(s, hdr+6);

	DBG("ID3: Decoding ID3v2.%d.%d tag, flags=%02x", t->version, t->revision, t->hdr_flags);
#if 0	// XXX: Some tags include the header size in the length, but it is wrong to do so!
	if (tag_len < 10)
		bs_error(&t->bs, "Invalid ID3v2 tag length (%d)", tag_len);
	tag_len -= 10;
#endif

	bs_subrange(s, &t->bs, tag_len, "ID3v2 tag truncated");
	switch (t->version) {
		case 2:
			id3v2_parse_v2(t);
			break;
		case 3:
			id3v2_parse_v3(t);
			break;
		case 4:
			id3v2_parse_v4(t);
			break;
		default:
			bs_error(s, "ID3v2.%d.%d tags are not supported", t->version, t->revision);
	}
	clist_add_tail(&ts->tags, &t->n);
	return t;
}

struct id3v2_tag *
id3v2_parse(struct id3v2_tag_set *ts, struct mp3_frame *fr)
{
	if (fr->bs.err_jmp)
		return id3v2_do_parse(ts, fr);
	else {
		jmp_buf jb;
		fr->bs.err_jmp = &jb;
		if (!setjmp(jb)) {
			struct id3v2_tag *t = id3v2_do_parse(ts, fr);
			fr->bs.err_jmp = NULL;
			return t;
		}
		fr->bs.err_jmp = NULL;
		return NULL;
	}
}

/*** Searching for items ***/

struct id3v2_item *
id3v2_find_item(struct id3v2_tag_set *ts, u32 id)
{
	// FIXME: So far, we use a fixed order.
	CLIST_FOR_EACH(struct id3v2_tag *, t, ts->tags)
		CLIST_FOR_EACH(struct id3v2_item *, it, t->items)
			if (it->id == id) {
				id3v2_seek_item(it);
				return it;
			}
	return NULL;
}

void
id3v2_seek_item(struct id3v2_item *it)
{
	struct bit_stream *s = &it->tag->bs;
	// One day, we should do decryption etc. here
	bs_reset(s, it->data, it->length);
}

/*** Parsing and objectification of items ***/

static void
single_attr(struct odes *o, uns attr, byte *str)
{
	// We assume correct UTF-8, but there might be various control chars embedded
	for (byte *s=str; *s; s++)
		if (*s < 0x20)
			*s = ' ';
	obj_add_attr_ref(o, attr, str);
}

static void
multi_attr(struct odes *o, uns attr, byte *str)
{
	// The same as above, but for multi-line strings, which translate to multi-valued attributes
	uns nl;
	do {
		byte *s = str;
		while (*s && *s != '\n') {
			if (*s < 0x20)
				*s = ' ';
			s++;
		}
		nl = *s;
		*s++ = 0;
		obj_add_attr_ref(o, attr, str);
		str = s;
	} while (nl);
}

static void
genre_attr(struct odes *o, uns attr, byte *str)
{
	byte *prev = NULL;
	while (str[0] == '(') {
		if (str[1] == '(') {
			str++;
			break;
		}
		else if (!strncasecmp(str, "(RX)", 4)) {
			obj_add_attr_ref(o, attr, prev = "Remix");
			str += 4;
		}
		else if (!strncasecmp(str, "(CR)", 4)) {
			obj_add_attr_ref(o, attr, prev = "Cover");
			str += 4;
		}
		else  {
			byte *p = str, *tab;
			uns x = 0;
			while (x < 255 && Cdigit(*++p))
				x = x * 10 + *p - '0';
			if (*p++ != ')')
				break;
			tab = id3_genre_tab(x);
			if (tab) /* Ignore unknown numeric genres */
				obj_add_attr_ref(o, attr, prev = tab);
			str = p;
		}
	}
	if (*str && (!prev || strcasecmp(prev, str)))
		obj_add_attr_ref(o, attr, str);
}

static struct odes *
id3v2_item_to_obj_ll(struct id3v2_item *it)
{
	struct odes *o = obj_new(it->tag->set->pool);
	int enc;

	obj_set_attr(o, 'F', (char []){ ID3_NAME_CC(it->id), 0 });
	id3v2_seek_item(it);
	switch (it->id) {
	case ID3_ID('C','O','M','M'):
	case ID3_ID('U','S','L','T'):
		enc = id3v2_parse_byte(it);
		single_attr(o, 'L', id3v2_parse_fixed_string(it, 3));	// language
		single_attr(o, 'D', id3v2_parse_string(it, enc));	// description
		multi_attr(o, 'T', id3v2_parse_string(it, enc));
		break;
	case ID3_ID('T','X','X','X'):
		enc = id3v2_parse_byte(it);
		single_attr(o, 'D', id3v2_parse_string(it, enc));	// description
		single_attr(o, 'T', id3v2_parse_string(it, enc));
		break;
	case ID3_ID('W','X','X','X'):
		enc = id3v2_parse_byte(it);
		single_attr(o, 'D', id3v2_parse_string(it, enc));	// description
		single_attr(o, 'U', id3v2_parse_string(it, 0));		// URL
		break;
	case ID3_ID('P','R','I','V'):
		single_attr(o, 'O', id3v2_parse_string(it, 0));		// owner
		break;
	case ID3_ID('T','C','O','N'):
		genre_attr(o, 'G', id3v2_parse_string(it, -1));		// genre
		break;
	default:
		if ((it->id & 0xff000000) == ID3_ID('T',0,0,0) ||	// Textual items
		    (it->id & 0xffff0000) == ID3_ID(' ','T',0,0))
			single_attr(o, 'T', id3v2_parse_string(it, -1));
		else if ((it->id & 0xff000000) == ID3_ID('W',0,0,0) ||	// Links
		         (it->id & 0xffff0000) == ID3_ID(' ','W',0,0))
			single_attr(o, 'U', id3v2_parse_string(it, 0));
	}
	return o;
}

struct odes *
id3v2_item_to_obj(struct id3v2_item *it)
{
	// If error reporting is disabled or errors aren't fatal, save us by an err_jmp
	struct bit_stream *s = &it->tag->bs;
	jmp_buf jb, *old_jb = s->err_jmp;
	s->err_jmp = &jb;

	struct odes *o;
	if (!setjmp(jb))
		o = id3v2_item_to_obj_ll(it);
	else
		o = NULL;

	s->err_jmp = old_jb;
	return o;
}

static struct oattr *
id3v2_find_item_obj_parent(struct odes *o, u32 id)
{
	for (struct oattr *a = o->attrs; a; a=a->next)
		if (a->attr == 'T' + OBJ_ATTR_SON) {
			byte *x = obj_find_aval(a->son, 'F');
			ASSERT(x);
			if (ID3_ID(x[0],x[1],x[2],x[3]) == id)
				return a;
		}
	return NULL;
}

struct odes *
id3v2_find_item_obj(struct odes *o, u32 id)
{
	struct oattr *a = id3v2_find_item_obj_parent(o, id);
	return a ? a->son : NULL;
}

struct odes *
id3v2_set_to_obj(struct id3v2_tag_set *set)
{
	// FIXME: So far, we use a fixed order.
	struct odes *o = obj_new(set->pool);
	CLIST_FOR_EACH(struct id3v2_tag *, t, set->tags) {
		CLIST_FOR_EACH(struct id3v2_item *, it, t->items) {
				struct odes *ito = id3v2_item_to_obj(it);
				// FIXME: With some frames, we might want to replace the original contents instead of appending to it.
				if (ito)
					obj_add_son_ref(o, 'T' + OBJ_ATTR_SON, ito);
		}
	}
	return o;
}

/*** Dumping ***/

static void
id3v2_dump_err(struct bit_stream *s UNUSED, char *msg, va_list args)
{
	printf("\t\t### ");
	vprintf(msg, args);
	putchar('\n');
}

void
id3v2_dump_item(struct id3v2_item *it)
{
	struct bit_stream *s = &it->tag->bs;
	struct bit_stream old_bs = *s;
	jmp_buf jb;

	mp_push(it->tag->set->pool);
	bs_init(s, NULL, 0);
	s->warn = id3v2_dump_err;
	s->err = id3v2_dump_err;
	s->err_jmp = &jb;
	if (!setjmp(jb)) {
		printf("\tItem %c%c%c%c (len=%d, flg=%02x)\n", ID3_NAME_CC(it->id), it->length, it->flags);
		struct odes *o = id3v2_item_to_obj_ll(it);
		obj_dump_indented(o, 2);
	}

	*s = old_bs;
	mp_pop(it->tag->set->pool);
}

void
id3v2_dump(struct id3v2_tag_set *set)
{
	CLIST_FOR_EACH(struct id3v2_tag *, t, set->tags) {
		printf("ID3v2 tag (v2.%d.%d) hf=%04x\n", t->version, t->revision, t->hdr_flags);
		CLIST_FOR_EACH(struct id3v2_item *, it, t->items)
			id3v2_dump_item(it);
	}
}
