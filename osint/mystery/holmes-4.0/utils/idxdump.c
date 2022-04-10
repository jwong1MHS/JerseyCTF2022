/*
 *	Sherlock Utilities -- Index Dumper
 *
 *	(c) 2001--2007 Robert Spalek <robert@ucw.cz>
 *	(c) 2002--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Pavel Charvat <phar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/chartype.h"
#include "ucw/conf.h"
#include "ucw/url.h"
#include "ucw/unicode.h"
#include "ucw/string.h"
#include "ucw/stkstring.h"
#include "ucw/binsearch.h"
#include "ucw/bbuf.h"
#include "sherlock/object.h"
#include "sherlock/lizard-fb.h"
#include "sherlock/tagged-text.h"
#include "charset/charconv.h"
#include "charset/unicat.h"
#include "charset/fb-charconv.h"
#include "indexer/indexer.h"
#include "indexer/lexicon.h"
#include "indexer/params.h"
#include "indexer/graph.h"
#include "utils/dumpconfig.h"
#include "lang/lang.h"

#ifdef CONFIG_IMAGES_SIM
#include "images/images.h"
#include "images/signature.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <alloca.h>

static int term_charset_id;
static struct lizard_buffer *liz_buf;
static struct fastbuf *output;
static int verbose;				/* Expand object attribute names?  */
static int bare;				/* Avoid headings */
static int raw;					/* Avoid various conversions */
static int annotate;				/* Annotate with URL's */

/* Maximal input and output line length.  */
#define	BUFSIZE		2048
#define	LINE_LEN	512
/* Special reserved value */
#define	PASS_POSITION	0xc001cafe

static u64
xtol64(byte *c)
{
	u64 x = 0;
	int n = 0;
	if (*c == '+')
	{
		byte *err = cf_parse_u64(c, &x);
		if (err)
			die(err);
	}
	else
		while (*c)
		{
			if (++n > 16)
				die("Number too long");
			if (!Cxdigit(*c))
				die("Invalid hexadecimal number");
			x = (x << 4) | Cxvalue(*c);
			c++;
		}
	return x;
}

/* String sets */

struct strset_item {
  char s[1];
};

#define HASH_PREFIX(x) strset_hash_##x
#define HASH_NODE struct strset_item
#define HASH_KEY_ENDSTRING s
#define HASH_WANT_LOOKUP
#define HASH_WANT_FIND
#define HASH_AUTO_POOL 1024
#define HASH_TABLE_DYNAMIC
#include "ucw/hashtable.h"

struct strset {
	struct strset_hash_table hash;
};

static void
strset_init(struct strset *set)
{
	bzero(set, sizeof(*set));
	strset_hash_init(&set->hash);
}

static void
strset_commit(struct strset *set UNUSED)
{
}

static void
strset_append(struct strset *set, byte *c)
{
	while (*c)
	{
		while (Cspace(*c) || *c == ',')
			c++;
		if (!*c)
			break;
		byte *start = c, *end = c;
		uns state = 0; // 1 - escaped, 2 - double quotes
		while (*c && (state || !(Cspace(*c) || *c == ',')))
		{
			if (*c == '\"' && !(state & 5))
				state ^= 2;
			else
			{
				if ((state & 1) || *c == '\\')
					state ^= 1;
				*end++ = *c;
			}
			c++;
		}
		if (state)
			die("Unterminated string");
		state = *c;
		*end = 0;
		str_unesc(start, start);
		strset_hash_lookup(&set->hash, start);
		if (!state)
			break;
		c++;
	}
}

static inline uns
strset_find(struct strset *set, byte *value)
{
	return !set->hash.hash_count || strset_hash_find(&set->hash, value);
}

/* Integer sets */

struct u64set_item {
	u64 start, end;
};

#define GBUF_PREFIX(x) u64set_buf_##x
#define GBUF_TYPE struct u64set_item
#include "ucw/gbuf.h"

struct u64set {
	uns count;
	u64set_buf_t list;
};

static void
u64set_init(struct u64set *set)
{
	set->count = 0;
	u64set_buf_init(&set->list);
}

#define ASORT_PREFIX(x) u64set_##x
#define ASORT_KEY_TYPE u64
#define ASORT_ELT(i) list[i].start
#define ASORT_SWAP(i, j) do{ struct u64set_item s = list[i]; list[i] = list[j]; list[j] = s; }while(0)
#define ASORT_EXTRA_ARGS , struct u64set_item *list
#include "ucw/sorter/array-simple.h"

static inline void
u64set_commit(struct u64set *set)
{
	if (!set->count)
		return;
	/* Sort and join overlapping intervals */
	struct u64set_item *list = set->list.ptr;
	uns i, j;
	u64set_sort(set->count, list);
	for (j = 0, i = 1; i < set->count; i++)
	{
		if (list[i].start > list[j].end + 1)
			list[++j] = list[i];
		else if (list[i].end > list[j].end)
			list[j].end = list[i].end;
	}
	set->count = j + 1;
}

static void
u64set_append(struct u64set *set, byte *c)
{
	while (*c)
	{
		while (Cspace(*c) || *c == ',')
			c++;
		if (!*c)
			break;
		u64set_buf_grow(&set->list, ++set->count);
		struct u64set_item *item = &set->list.ptr[set->count - 1];
		byte *start = c++;
		while (*c && !Cspace(*c) && *c != ',' && *c != '-')
			c++;
		uns state = *c;
		*c++ = 0;
		item->start = xtol64(start);
		if (state == '-')
		{
			start = c;
			while (*c && !Cspace(*c) && *c != ',')
				c++;
			state = *c;
			*c++ = 0;
			item->end = xtol64(start);
			if (item->start > item->end)
				die("Invalid integer range");
		}
		else
			item->end = item->start;
		if (!state)
			break;
		if (!Cspace(state) && state != ',')
			die("Invalid integer set format");
	}
}

static inline uns
u64set_find(struct u64set *set, u64 value)
{
	if (!set->count)
		return 1;
#define SET_U64_LT(ary, i, x) ((ary)[i].end < (x))
	uns i = BIN_SEARCH_FIRST_GE_CMP(set->list.ptr, set->count, value, SET_U64_LT);
#undef SET_U64_LT
	return i < set->count && set->list.ptr[i].start <= value;
}

/* Zoek filters */

#define ZOEK_STRSET (void(*)(void*)) &strset_init, (void(*)(void*,byte*)) &strset_append, (void(*)(void *)) &strset_commit
#define ZOEK_U64SET (void(*)(void*)) &u64set_init, (void(*)(void*,byte*)) &u64set_append, (void(*)(void *)) &u64set_commit

struct zoek_def {
	byte *name;
	void *set;
	void (*init)(void *set);
	void (*append)(void *set, byte *c);
	void (*commit)(void *set);
};

static void
zoek_init(struct zoek_def *defs)
{
	for (; defs->name; defs++)
		defs->init(defs->set);
}

static void
zoek_commit(struct zoek_def *defs)
{
	for (; defs->name; defs++)
		defs->commit(defs->set);
}

static void
zoek_append(struct zoek_def *defs, byte *c)
{
	for (byte *s = c; *s; s++)
		if (*s == '\"' || *s == '\\')
			break;
		else if (*s == '=')
		{
			*s = 0;
			while (strcasecmp(defs->name, c))
			{
				defs++;
				if (!defs->name)
					die("Invalid zoek variable, try idxdump --help");
			}
			c = s + 1;
		}
	defs->append(defs->set, c);
}

static byte *
get_annot(int id)
{
	static struct fastbuf *annot_fb;
	static struct fastbuf *annot_index_fb;
	static int annot_last_id = -1;
	static byte annot_buf[MAX_URL_SIZE];
	if (!annotate)
		return "";
	if (unlikely(!annot_fb))
		annot_fb = index_bopen("url-list", O_RDONLY, 0);
	if (annot_last_id + 1 != id) {
		if (unlikely(!annot_index_fb))
			annot_index_fb = index_bopen("url-index", O_RDONLY, 0);
		bsetpos(annot_index_fb, (ucw_off_t)id * BYTES_PER_O);
		bsetpos(annot_fb, bgeto(annot_index_fb));
	}
	if (!bgets(annot_fb, annot_buf, sizeof(annot_buf)))
		die("Unable to annotate oid 0x%x: out of url-list", id);
	annot_last_id = id;
	return annot_buf;
}

static void
dump_card_attr(u64 id, void *tmp)
{
	struct card_attr *a = tmp;
	byte *attrs = "EADMF123";
	byte at[9], tf[32], mh[9];

	if (!a)
	{
		if (!verbose && !bare)
			bprintf(output, "ID       Card     SiteID   MrgHash    Area Wgt %s Age Type\n", attrs);
		return;
	}
	str_format_flags(at, attrs, a->flags);
#ifdef CONFIG_FILETYPE
	byte *x = tf + sprintf(tf, "%02x %s", a->type_flags, custom_file_type_names[CA_GET_FILE_TYPE(a)]);
#ifdef CONFIG_LANG
	if (!(a->type_flags & 0x80))
	  sprintf(x, ", %s", lang_code_to_name(CA_GET_FILE_LANG(a)));
#endif
	if (a->type_flags & 0x80)
		sprintf(x, " other: %d/%d", CA_GET_FILE_TYPE(a), CA_GET_FILE_INFO(a));
#else
	tf[0] = 0;
#endif
	for (int i=0; i<SHERLOCK_MERGING_HASH_SIZE; i++)
		sprintf(mh+2*i, "%02x", a->merging_hash[i]);
	mh[2*SHERLOCK_MERGING_HASH_SIZE] = 0;
	if (verbose)
	{
		bprintf(output, "Attribute %x:\n", (u32) id);
		bprintf(output, "Card:\t%x\n", a->card);
#ifdef CONFIG_SITES
		bprintf(output, "Site:\t%x\n", a->site_id);
#endif
#ifdef CONFIG_MERGING_HASHES
		bprintf(output, "MrgHash:\t%s\n", mh);
#endif
#ifdef CONFIG_AREAS
		bprintf(output, "Area:\t%d\n", a->area);
#endif
		bprintf(output, "Weight:\t%d\n", a->weight);
		bprintf(output, "Flags:\t%s\n", at);
#ifdef CONFIG_LASTMOD
		bprintf(output, "Age:\t%d\n", a->age);
#endif
#ifdef CONFIG_FILETYPE
		bprintf(output, "Type:\t%s\n", tf);
#endif
#define INT_ATTR(id,keywd,gf,pf) bprintf(output, "Custom " #id ":\t%x\n", gf(a));
#define SMALL_SET_ATTR(id,keywd,gf,pf) bprintf(output, "Custom " #id ":\t%x\n", gf(a));
#define LATE_INT_ATTR INT_ATTR
#define LATE_SMALL_SET_ATTR SMALL_SET_ATTR
  EXTENDED_ATTRS
#undef INT_ATTR
#undef SMALL_SET_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR
		if (annotate)
			bprintf(output, "URL:\t%s\n", get_annot(id));
		bputc(output, '\n');
	}
	else
	{
		bprintf(output, "%8x %8x %8x %-8s %6d %3d %s %3d %-20s %s\n",
			(u32) id, a->card,
#ifdef CONFIG_SITES
		       a->site_id,
#else
		       0,
#endif
		       mh,
#ifdef CONFIG_AREAS
		       a->area,
#else
		       0,
#endif
		       a->weight, at,
#ifdef CONFIG_LASTMOD
		       a->age,
#else
		       0,
#endif
		       tf,
		       get_annot(id)
		       );
	}
}

static void
dump_footprint(struct fastbuf *output, byte *fp)
{
	u32 x[4];
	memcpy(x, fp, 16);
	bprintf(output, "%08x%08x:%08x%08x", x[0], x[1], x[2], x[3]);
}

static void
dump_md5(struct fastbuf *output, byte *md5)
{
	for (uns i=0; i<16; i++)
		bprintf(output, "%02x", md5[i]);
}

static void
dump_note(u64 id, void *tmp)
{
	struct card_note *n = tmp;
	byte *attrs = "GOIPRAT*";
	byte at[9];

	if (!n)
	{
		if (!bare)
		{
			byte header[SITE_HASH_SIZE*2+1], format[10];
			sprintf(format, "%%-%d.%ds", SITE_HASH_SIZE*2, SITE_HASH_SIZE*2);
			sprintf(header, format, "SiteHash");
			bprintf(output, "ID         OID      Area CBonus USize  Scn Ook Dyn Mrg %s %s Footprint\n", attrs, header);
		}
		return;
	}
	str_format_flags(at, attrs, n->flags);
	bprintf(output, "%8x %8x %6d %6d %6d %3d %3d %3d %3d %s ", (u32) id,
#ifdef CONFIG_INDEXER_STORE_OID
	       n->oid,
#else
	       0,
#endif
#ifdef CONFIG_AREAS
	       n->area,
#else
	       0,
#endif
	       n->card_bonus,
	       n->useful_size,
	       n->weight_scanner,
	       0,
#ifdef CONFIG_WEIGHTS
	       n->weight_dynamic,
#else
	       0,
#endif
	       n->weight_merged,
	       at);
	for (int i=0; i<SITE_HASH_SIZE; i++)
		bprintf(output, "%02x", n->site_hash[i]);
	bputc(output, ' ');
	dump_footprint(output, n->footprint);
	bprintf(output, " %s\n", get_annot(id));
}

static struct u64set zoek_card_info_orig_card;
static struct zoek_def card_info_zoeks[] = {
	{ "OrigCard",	&zoek_card_info_orig_card,	ZOEK_U64SET },
	{ NULL }
};

static void
dump_card_info(u64 id, void *tmp)
{
	struct card_info *info = tmp;
	if (!info)
	{
		dump_card_attr(0, NULL);
		dump_note(0, NULL);
		if (!bare)
			bprintf(output, "         OrigCard\n");
		return;
	}
	if (!u64set_find(&zoek_card_info_orig_card, info->orig_card))
		return;
	dump_card_attr(id, &info->attr);
	dump_note(id, &info->note);
	bprintf(output, "         %8x\n", info->orig_card);
}

static u64
dump_admin_export_header(struct fastbuf *f)
{
	struct odes *o = obj_new(cf_pool);
	if (obj_read(f, o) <= 0)
		die("Corrupted export header");
	if (verbose)
	{
		bprintf(output, "Params:\n");
		bput_object(output, o);
		bputc(output, '\n');
	}
	return btell(f);
}

static void
dump_admin_export(u64 id, void *tmp)
{
	struct admin_export *exp = tmp;
	byte *note_flags = "GOIPRAT*", *flags = "EADMF123", note_flags_buf[9], flags_buf[9];
	if (!exp)
	{
		if (!bare)
			bprintf(output, "ID       Footprint                         Idx Id       " \
			    "%s %s Scn Ook Dyn Mrg Wei TF Lng Typ\n", flags, note_flags);
		return;
	}
	str_format_flags(flags_buf, flags, exp->flags);
	str_format_flags(note_flags_buf, note_flags, exp->note_flags);
	bprintf(output, "%08x ", (u32)id);
	dump_footprint(output, exp->footprint);
	bprintf(output, " %3u %08x %s %s %3d %3d %3d %3d %3d %02x %3d %3d\n",
	    exp->subindex + 1, exp->id, flags_buf, note_flags_buf,
	    exp->weight_scanner, exp->weight_oook, exp->weight_dynamic,
	    exp->weight_merged, exp->weight, exp->file_flags, exp->file_lang, exp->file_type);
}

static void
dump_checksum(u64 id, void *tmp)
{
	struct csum *c = tmp;
	if (!tmp)
	{
		if (!verbose && !bare)
			bprintf(output, "Pos      MD5                              CardID\n");
		return;
	}
	if (verbose)
	{
		bprintf(output, "Checksum at %x:\n", (u32) id);
		bprintf(output, "MD5:\t");
		dump_md5(output, c->md5);
		bprintf(output, "\n");
		bprintf(output, "Card:\t%08x\n\n", c->cardid);
	}
	else
	{
		bprintf(output, "%08x ", (u32) id);
		dump_md5(output, c->md5);
		bprintf(output, " %08x\n", c->cardid);
	}
}

static struct strset zoek_fingerprint_fp;
static struct u64set zoek_fingerprint_card_id;
static struct zoek_def fingerprint_zoeks[] = {
	{ "FP",		&zoek_fingerprint_fp,		ZOEK_STRSET },
	{ "CardID",	&zoek_fingerprint_card_id,	ZOEK_U64SET },
	{ NULL }
};

static void
dump_fingerprint(u64 id, void *tmp)
{
	struct card_print *c = tmp;
	int i;
	if (!c)
	{
		if (!verbose && !bare)
			bprintf(output, "ID       Fingerprint              CardID\n");
		return;
	}
	byte fp[25];
	for (i=0; i<12; i++)
		sprintf(fp + i * 2, "%02x", c->fp.hash[i]);
	fp[24] = 0;
	if (!strset_find(&zoek_fingerprint_fp, fp) || !u64set_find(&zoek_fingerprint_card_id, c->cardid))
		return;
	if (verbose)
	{
		bprintf(output, "Fingerprint at %x:\n", (u32) id);
		bprintf(output, "Hash:\t");
		bprintf(output, "%s", fp);
		bprintf(output, "\n");
		bprintf(output, "Card:\t%08x\n", c->cardid);
		if (annotate && c->cardid < FIRST_ID_SKEL)
			bprintf(output, "URL: %s\n", get_annot(c->cardid));
		bputc(output, '\n');
	}
	else
	{
		bprintf(output, "%08x ", (u32) id);
		bprintf(output, "%s", fp);
		bprintf(output, " %08x", c->cardid);
		if (annotate && c->cardid < FIRST_ID_SKEL)
			bprintf(output, " %s", get_annot(c->cardid));
		bputc(output, '\n');
	}
}

static struct u64set zoek_signature_card_id;
static struct zoek_def signature_zoeks[] = {
	{ "CardID",	&zoek_signature_card_id,	ZOEK_U64SET },
	{ NULL }
};

static void
dump_signatures(u64 id, void *tmp)
{
	uns *c = tmp;
	uns i;
	if (!tmp)
	{
		if (!verbose && !bare)
			bprintf(output, "Pos      CardID   Signatures\n");
		return;
	}
	if (!u64set_find(&zoek_signature_card_id, *c))
		return;
	if (verbose)
	{
		bprintf(output, "Checksum at %x:\n", (u32) id);
		bprintf(output, "Card:\t%08x\n", *c++);
		bprintf(output, "Signatures:\t");
		for (i=0; i<matcher_signatures; i++)
			bprintf(output, "%08x ", c[i]);
		bprintf(output, "\n\n");
	}
	else
	{
		bprintf(output, "%08x ", (u32) id);
		bprintf(output, "%08x", *c++);
		for (i=0; i<matcher_signatures; i++)
			bprintf(output, " %08x", c[i]);
		bprintf(output, "\n");
	}
}

static void
dump_card(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	u32 id;
	byte *buf;
	int buf_len;
	uns buf_type;

	if (!f)
		return;
	id = start >> CARD_POS_SHIFT;
	ASSERT(!(start & ((1 << CARD_POS_SHIFT) - 1)));
	if (raw > 1)
	{
		buf_len = bgetl(f);
		buf_type = bgetc(f) + BUCKET_TYPE_PLAIN;
		if (buf_type < BUCKET_TYPE_PLAIN || buf_type > BUCKET_TYPE_V33_LIZARD)
			die("Cannot parse card %08x: bucket_type=%08x", id, buf_type);
		uns sep = bgetc(f);
		ASSERT(!sep);
		buf_len -= LIZARD_COMPRESS_HEADER;
		bput_attr_format(output, '#', "## %08x %06d %08x", id, buf_len, buf_type);
		if (buf_type == BUCKET_TYPE_V33_LIZARD)
		{
			uns orig_len = bgetl(f);
			uns adler = bgetl(f);
			bput_attr_format(output, '#', "## orig_len=%d adler=%08x", orig_len, adler);
			buf_len -= 8;
		}
	}
	else
	{
		buf_len = lizard_bread(liz_buf, f, &buf, &buf_type);
		if (buf_len < 0)
			die("Cannot parse card %08x: %m", id);
	}
	if (raw > 1)
		bbcopy(f, output, buf_len);
	else if (raw)
		bwrite(output, buf, buf_len);
	else
	{
		get_attr_set_type(buf_type);
		put_attr_set_type(BUCKET_TYPE_V30);
		bput_attr_format(output, '#', "## %08x %06d %08x", id, buf_len, buf_type);
		byte *buf_end = buf + buf_len;
		while (1)							/* Dump all attributes.  */
		{
			struct parsed_attr attr;
			int i = get_attr(&buf, buf_end, &attr);
			if (i < 0)
				break;
			if (!i)
			{
				bput_attr_separator(output);
				continue;
			}
			if (!attr.len)
				bput_attr(output, attr.attr, "", 0);
			byte *attr_end = attr.val + attr.len;
			while (attr.len)					/* Wrap into short lines.  */
			{
				uns print = attr.len;
				if (print > LINE_LEN)				/* Do not use line_len, this is for 2nd wrapping done by objdump.  */
				{
					byte *start = attr.val + LINE_LEN, *c = start;
					while (c < attr_end && !Cspace(*c)
					&& (!Cctrl(*c) || c < start + 64)
					&& ((*c >= 0x80 && *c < 0xc0) || c < start + 128))
						c++;
					print = c - attr.val;
				}
				bput_attr(output, attr.attr, attr.val, print);	/* Do not recode, done by objdump.  */
				attr.val += print;
				attr.len -= print;
			}
		}
	}
	while (btell(f) & ((1 << CARD_POS_SHIFT) - 1))
		if (bgetc(f) < 0)
			break;
	bputc(output, '\n');
}

static byte *wt_names[8] = { WORD_TYPE_USER_NAMES };
static byte *mt_names[16] = { META_TYPE_USER_NAMES };

static uns
dump_type_name(struct fastbuf *f, struct fastbuf *output, uns meta)
{
	byte **names = meta ? mt_names : wt_names;
	int c = bgetc(f);
	uns shift = 1;
	ASSERT(c >= 0);
	if (c >= '0' && c <= '3')
	{
		bputc(output, ' ');
		bputc(output, c);
		c = bgetc(f);
		shift++;
	}
	if ((c & 0xf0) != 0x90)
		bprintf(output, " [error=%x]", c);
	bprintf(output, " [%s] ", names[c & 0x0f]);
	return shift;
}

static void
dump_single_label(struct parsed_attr *pa)
{
	byte *p = pa->val, *end = pa->val + pa->len;
	bprintf(output, "\t%c", pa->attr);
	if (raw || pa->attr == 'H')
		bwrite(output, p, pa->len);
	else while (p < end)
	{
		uns c;
		GET_TAGGED_CHAR(p, c);
		if (c < 0x80000000)
			bput_utf8(output, c);
		else if (c < 0x80001000)
			bprintf(output, " [%s] ", mt_names[c & 0xf]);
		else
			bprintf(output, " [???] ");
	}
	bputc(output, '\n');
}

static const char label_flags[] = "BUMO";

static struct u64set zoek_labels_merged_id;
static struct u64set zoek_labels_url_id;
static struct u64set zoek_labels_redir_id;
static struct zoek_def labels_zoeks[] = {
	{ "MergedID",	&zoek_labels_merged_id,	ZOEK_U64SET },
	{ "URLID",	&zoek_labels_url_id,	ZOEK_U64SET },
	{ "RedirID",	&zoek_labels_redir_id,	ZOEK_U64SET },
	{ NULL }
};

static void
dump_labels(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	struct lab label;
	byte fl[9];

	if (!f)
		return;
	breadb(f, &label, sizeof(struct lab));
	uns filtered = !(
		u64set_find(&zoek_labels_merged_id, label.merged_id) && 
		u64set_find(&zoek_labels_url_id, label.url_id) && 
		u64set_find(&zoek_labels_redir_id, label.redir_id)); 
	if (!filtered)
	{
		bprintf(output, "Label at %08llx:\n", (long long) start);
		bprintf(output, "Merged ID:\t%08x\n", label.merged_id); // Stage2 ID
		bprintf(output, "URL ID:\t\t%08x\n", label.url_id);
		bprintf(output, "Redirect ID:\t%08x\n", label.redir_id);
		bprintf(output, "Count:\t%d\n", label.count);
		bprintf(output, "Flags:\t%s\n", str_format_flags(fl, label_flags, label.flags));
		bprintf(output, "Labels:\n");
	}
	ucw_off_t stop = btell(f) + label.count;
	get_attr_set_type(BUCKET_TYPE_V33);
	while (btell(f) < stop)
	{
		struct parsed_attr pa;
		if (bget_attr(f, &pa) <= 0)
			die("Broken label file");
		if (!filtered)
			dump_single_label(&pa);
	}
	if (!filtered)
		bprintf(output, "\n");
}

static struct u64set zoek_labels_id_orig_id;
static struct zoek_def labels_id_zoeks[] = {
	{ "OrigID",	&zoek_labels_id_orig_id,	ZOEK_U64SET },
	{ NULL }
};

static void
dump_labels_id(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;

	if (!f)
		return;
	u32 orig_id = bgetl(f);
	uns filtered = !u64set_find(&zoek_labels_id_orig_id, orig_id);
	byte fl[9];
	str_format_flags(fl, label_flags, bgetc(f));
	if (!filtered)
		bprintf(output, "Label block at %08llx for URL %08x (%s)\n", (long long) start, orig_id, fl);
	get_attr_set_type(BUCKET_TYPE_V33);
	while (1)
	{
		struct parsed_attr pa;
		int c = bget_attr(f, &pa);
		if (c < 0)
			die("Broken label file");
		if (!c)
			break;
		if (!filtered)
			dump_single_label(&pa);
	}
	if (!filtered)
		bprintf(output, "\n");
}

static char *word_classes[] = { "cplx", "ignr", "word", "garb", "ctxt", "brek", "????", "????" };

static struct strset zoek_lex_word;
static struct zoek_def lex_zoeks[] = {
	{ "Word",	&zoek_lex_word,	ZOEK_STRSET },
	{ NULL }
};

static void
dump_lex_entry(struct lex_entry *l, uns id, struct fastbuf *f)
{
	ucw_off_t ref_pos = GET_O(l->ref_pos);
	uns chlen = GET_U16(l->ch_len) << 12;
	id |= l->class;
#ifdef CONFIG_CONTEXTS
	uns ctxt = l->ctxt;
#else
	uns ctxt = 0;
#endif
#ifdef CONFIG_SPELL
	uns freq = l->freq;
#else
	uns freq = 0;
#endif
	byte word[l->length+1];
	bread(f, word, l->length);
	word[l->length] = 0;
	if (!strset_find(&zoek_lex_word, word))
		return;

	if (verbose)
	{
		bprintf(output, "Word ID %x:\n", id);
		bprintf(output, "References:\t%08llx + %07x\n", (long long) ref_pos, chlen);
		bprintf(output, "Class:\t\t%s\n", (l->class > 7 ? "????" : word_classes[l->class]));
		bprintf(output, "Frequency:\t%d\n", freq);
		bprintf(output, "Context:\t%04x\n", ctxt);
		bprintf(output, "Length:\t\t%d\n", l->length);
		bprintf(output, "Word:\t\t");
	}
	else
	{
		bprintf(output, "%8x %8llx+%07x %s %3d %04x %3d ", id, (long long) ref_pos, chlen,
		       (l->class > 7 ? "????" : word_classes[l->class]), freq, ctxt, l->length);
	}
	bwrite(output, word, l->length);
	bprintf(output, verbose ? "\n\n" : "\n");
}

static void
dump_lexicon(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	uns wordid;

	if (!f)
	{
		if (!verbose && !bare)
			bprintf(output, "ID       RefPos+Len       Flgs Frq Ctxt Len Word\n");
		return;
	}
	if (!start)
	{
		u32 wc = bgetl(f);
		u32 cc = bgetl(f);
		bprintf(output, "# Word count:\t%d\n", wc);
		bprintf(output, "# Complex count:\t%d\n", cc);
		wordid = 8;
		for (uns i=0; i<wc+cc; i++)
		{
			struct lex_entry l;
			breadb(f, &l, sizeof(l));
			dump_lex_entry(&l, wordid, f);
			wordid += 8;
		}
	}
	else
		bprintf(output, "# Garbage found after end of lexicon!\n");
}

static void
dump_lex_words(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	static uns wordid;

	if (!f)
	{
		if (!verbose && !bare)
			bprintf(output, "ID       RefPos+Len       Flgs Frq Ctxt Len Word\n");
		return;
	}
	if (!start)
	{
		u32 wc = bgetl(f);
		bprintf(output, "# Word count:\t%d\n", wc);
		wordid = 8;
	}
	else
	{
		struct lex_entry l;
		breadb(f, &l, sizeof(l));
		dump_lex_entry(&l, wordid, f);
		wordid += 8;
	}
}

static void
dump_lex_temp(u64 pos, void *tmp)
{
	struct fastbuf *f = tmp;
	static uns wid;

	if (pos == PASS_POSITION)		// start of a new block
	{
		wid = ~0U;
		return;
	}
	if (!f)
	{
		if (!verbose && !bare)
			bprintf(output, "Pos      ID       Count      Flgs Ctxt Len Word\n");
		return;
	}
	if (pos < wid)				// requested former word
	{
		wid = 0;
		bsetpos(f, 0);
	}
	if (!wid)				// beginning of the file
	{
		u32 wc = bgetl(f);
		bprintf(output, "# Word count:\t%d\n", wc);
	}

	u32 id, count;
	uns class, context, length;
	while (1)
	{
		id = bgetl(f);
		if (id == ~0U)			// eof
			return;
		count = bgetl(f);
		class = id & 7;
		context = bget_context(f);
		length = bgetc(f);
		if (wid++ < pos)
			bskip(f, length);
		else
			break;
	}

	byte word[length+1];
	bread(f, word, length);
	word[length] = 0;
	if (!strset_find(&zoek_lex_word, word))
		return;

	if (verbose)
	{
		bprintf(output, "Record %x:\n", (uns) pos);
		bprintf(output, "Word ID %x:\n", id >> 3);
		bprintf(output, "Count:\t\t%d\n", count);
		bprintf(output, "Word class:\t\t%s\n", word_classes[class]);
		bprintf(output, "Ctxt class:\t\t%d\n", context);
		bprintf(output, "Length:\t\t%d\n", length);
		bprintf(output, "Word:\t\t");
	}
	else
	{
		bprintf(output, "%8x %8x %10d %s %04x %3d ", (uns) pos, id >> 3, count, word_classes[class], context, length);
	}
	bwrite(output, word, length);
	bprintf(output, verbose ? "\n\n" : "\n");
}

static void
dump_lex_stats(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;

	if (!f || start)
		return;

	enum wtype { TOTAL, BARE, ACCENTED, DIGITS, MIXED, NTYPES };
	byte *tnames[] = { "Total", "Bare", "Accented", "Numbers", "Mixed" };
	uns wcnt[MAX_WORD_BYTES+1][NTYPES];
	uns wlen[MAX_WORD_BYTES+1][NTYPES];
	uns cnt = bgetl(f);
	bzero(wcnt, sizeof(wcnt));
	bzero(wlen, sizeof(wlen));
	while (cnt--)
	{
		bgetl(f);
		bgetl(f);
		bget_context(f);
		uns len = bgetc(f);
		uns reclen = 9 + len;
		ASSERT(len && len <= MAX_WORD_BYTES);
		byte buf[MAX_WORD_BYTES];
		bread(f, buf, len);
		int alpha = 0;
		int digits = 0;
		int accents = 0;
		for (byte *x=buf; x<buf+len;)
		{
			uns c;
			x = utf8_get(x, &c);
			if (c >= '0' && c <= '9')
				digits++;
			else
				alpha++;
			if (Uunaccent(c) != c)
				accents++;
		}
		enum wtype t;
		if (!digits) {
			if (!accents)
				t = BARE;
			else
				t = ACCENTED;
		} else {
			if (alpha)
				t = MIXED;
			else
				t = DIGITS;
		}
		len = digits+alpha;
		wcnt[len][t]++;
		wlen[len][t] += reclen;
		wcnt[len][TOTAL]++;
		wlen[len][TOTAL] += reclen;
		wcnt[0][t]++;
		wlen[0][t] += reclen;
		wcnt[0][TOTAL]++;
		wlen[0][TOTAL] += reclen;
	}
	bputs(output, "Words:\n");
	bprintf(output, "Len ");
	for (uns j=0; j<NTYPES; j++)
		bprintf(output, " %11s Cnt/Len", tnames[j]);
	bputc(output, '\n');
	for (uns i=0; i<=MAX_WORD_BYTES; i++)
	{
		if (!i)
			bprintf(output, "Sum:");
		else
			bprintf(output, "%3d:", i);
		for (uns j=0; j<NTYPES; j++)
			bprintf(output, " %9d/%9d", wcnt[i][j], wlen[i][j]);
		bputc(output, '\n');
	}
}

static void
dump_graph(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	u32 src, deg;
	byte *vtypes[8] = { "", " [site2]", " [redir]", " [redir+site2]",
		" [frame]", " [frame+site2]", " [img]", " [img+site2]" };

	if (!f)
		return;
	bget_graph_hdr(f, &src, &deg);
	bprintf(output, "Vertex %x (degree %d) at %08llx:\n", src, deg, (long long) start);
	while (deg--)
	{
		u32 x = bgetl(f);
		bprintf(output, "\t<- %x%s\n",
		       x & ~ETYPE_MASK,
		       vtypes[x >> ETYPE_SHIFT]);
	}
}

static void
dump_graph_index(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	if (!f)
		return;
	ucw_off_t ofs = bgeto(f);
	bprintf(output, "%08x -> %010llx\n", (uns) start / BYTES_PER_O, (long long) ofs);
}

static void
dump_graph_vertex(struct fastbuf *b, int argc, char **argv)
{
	struct fastbuf *fb_goes = bopen_try(stk_strcat(b->name, FN_GRAPH_GOES), O_RDONLY, 4);
	struct fastbuf *fb_index = bopen(stk_strcat(b->name, FN_GRAPH_INDEX), O_RDONLY, 4);
	while (argc--)
	{
		uns vertex = xtol64(*argv++);
		if (fb_goes)
		{
			bsetpos(fb_goes, 4*vertex);
			vertex = bgetl(fb_goes);
			if (vertex == 0xffffffff)
				die("No such vertex in %s", fb_goes->name);
		}
		bsetpos(fb_index, BYTES_PER_O * (ucw_off_t)vertex);
		ucw_off_t pos = bgeto(fb_index);
		if (verbose)
			bprintf(output, "# Translated vertex %x at position %llx\n", vertex, (long long)pos);
		bsetpos(b, pos);
		dump_graph(pos, b);
	}
	bclose(fb_index);
	bclose(fb_goes);
}

static struct u64set zoek_merges_class;
static struct zoek_def merges_zoeks[] = {
	{ "Class",	&zoek_merges_class,	ZOEK_U64SET },
	{ NULL }
};

static void
dump_merges(u64 id, void *tmp)
{
	u32 *u = tmp;
	if (!u)
	{
		if (!bare)
		{
			bprintf(output, "CardID      Class");
			if (annotate)
				bprintf(output, "    URL");
			bputc(output, '\n');
		}
		return;
	}
	if (u64set_find(&zoek_merges_class, *u))
	{
		bprintf(output, "%08x -> %08x", (u32) id, *u);
		if (annotate)
			bprintf(output, " %s", get_annot(id));
		bputc(output, '\n');
	}
}

static struct u64set zoek_u32_value;
static struct zoek_def u32_zoeks[] = {
	{ "Value",	&zoek_u32_value,	ZOEK_U64SET },
	{ NULL }
};

static void
dump_u32(u64 id, void *tmp)
{
	u32 *u = tmp;
	if (!u)
		return;
	if (u64set_find(&zoek_u32_value, *u))
		bprintf(output, "%08x -> %08x\n", (u32) id, *u);
}

static void
dump_float(u64 id, void *tmp)
{
	float *u = tmp;
	if (!u)
		return;
	bprintf(output, "%08x -> %e\n", (u32) id, *u);
}

static inline uns
Bgetc(struct fastbuf *f)
{
  uns c = bgetc(f);
  //bprintf(output, "#%02x ", c);
  return c;
}

static inline uns
Bgetw(struct fastbuf *f)
{
  uns w = bgetw(f);
  //bprintf(output, "#%04x ", w);
  return w;
}

static uns
dump_refchain(struct fastbuf *f)
{
	uns rlen = 4;
	u64 pos = btell(f);
	u32 oid = bgetl(f);
	if (!oid)
		return rlen;
	uns len = oid >> 28;
	oid &= (1<<28) - 1;
	if (!len)
		len = bget_utf8_32(f), rlen += utf8_space(len);
	rlen += len;
	if (verbose)
		bprintf(output, "OID:\t%08x\nLength:\t%d\n\t", oid, len);
	else
		bprintf(output, "%8llx %8x %8d ", (long long) pos, oid, len);
	uns last_pos = 0;
	while (len)
	{
		uns pos, type, wt = 0, len1;
		uns c = Bgetc(f);
		if (c < 0x80)
			len1 = 1, type = c >> 6, pos = c & 0x3f;
		else if (c < 0xc0)
			len1 = 2, type = c & 7, pos = ((c & 0x38) << 5) | Bgetc(f);
		else if (c < 0xe0)
			len1 = 3, type = c & 7, pos = ((c & 0x18) << 13) | Bgetw(f);
		else if (c < 0xf0)
			len1 = 2, type = 0x80 | c & 0xf,
			c = Bgetc(f), wt = c >> 6, pos = c & 0x3f;
		else if (c < 0xf8)
			len1 = 3, wt = c & 3, type = 0x80 | (c & 4) << 1,
			pos = Bgetw(f), type |= pos & 7, pos >>= 3;
		else if (c < 0xfc)
			len1 = 4, type = c & 3,
			c = Bgetc(f), type |= (c >> 5) & 4,
			pos = ((c & 0x7f) << 16) | Bgetw(f);
		else if (c < 0xfe)
			len1 = 4, wt = c & 1,
			c = Bgetc(f), wt |= (c >> 6) & 2, type = 0x80 | c & 0xf,
			pos = ((c & 0x70) << 12) | Bgetw(f);
		else
		{
			if (c >= 0xfe)
				bprintf(output, "RFU ");
			len--;
			continue;
		}
		if (!(type & 0x80))
			pos += last_pos;
		if (verbose)
			if (type & 0x80)
				bprintf(output, "(%03x:%s:%d)_%d ", pos, mt_names[type & 0x7f], wt, len1);
			else
				bprintf(output, "(+%02x=%03x:%s)_%d ", pos-last_pos, pos, wt_names[type], len1);
		else
			if (type & 0x80)
				bprintf(output, "(%03x:%x:%d) ", pos, type & 0x7f, wt);
			else
				bprintf(output, "(%03x:%x) ", pos, type);
		last_pos = pos;
		len -= len1;
	}
	bprintf(output, "\n");
	return rlen;
}

static void
dump_refs(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;

	if (!f)
	{
		if (!verbose && !bare)
			bprintf(output, "Pos      OID      Length   ref\n");
		return;
	}
	bprintf(output, "@%llx:\n", (long long) start);
	if (num_slices == 1)
		while (dump_refchain(f) > 4);
	else
	{
		uns slice_mask, slice_id[HARD_MAX_SLICES], slice_len[HARD_MAX_SLICES], nsl;
		slice_mask = bgetc(tmp);
		nsl = 0;
		for (uns i=0; i<HARD_MAX_SLICES; i++)
			if (slice_mask & (1 << i))
				slice_id[nsl++] = i;
		ASSERT(nsl);
		for (uns i=0; i<nsl-1; i++)
			slice_len[i] = bget_utf8_32(tmp);
		for (uns i=0; i<nsl; i++)
		{
			bprintf(output, "Slice %d", slice_id[i]);
			if (i < nsl-1)
				bprintf(output, " (%d bytes)", slice_len[i]);
			bprintf(output, "\n");
			while (dump_refchain(f) > 4);
		}
	}
	bprintf(output, "\n");
}

static void
dump_ref_texts(u64 start UNUSED, void *tmp)
{
	struct fastbuf *f = tmp;
	uns i, l;
	struct fingerprint fp;

	if (!f)
	{
		if (!verbose && !bare)
			bprintf(output, "ID       FingerPrint              Text\n");
		return;
	}
	bprintf(output, "%08x ", bgetl(f));
	breadb(f, &fp, sizeof(fp));
	for (i=0; i<12; i++)
		bprintf(output, "%02x", fp.hash[i]);
	l = bgetw(f);
	l -= dump_type_name(f, output, 1);
	while (l--) {
		i = bgetc(f);
		bputc(output, i);
	}
	bputc(output, '\n');
}

static void
dump_sites(u64 id, void *tmp)
{
	struct site_mapping *m = tmp;
	int i;
	if (!m)
	{
		if (!verbose && !bare)
			bprintf(output, "SiteID   SiteHash\n");
		return;
	}
	if (verbose)
	{
		bprintf(output, "ID:\t%x\n", (u32) id);
		bprintf(output, "Hash:\t");
		for (i=0; i<SITE_HASH_SIZE; i++)
			bprintf(output, "%02x", m->site_hash[i]);
		bprintf(output, "\n");
	}
	else
	{
		bprintf(output, "%08x ", (u32) id);
		for (i=0; i<SITE_HASH_SIZE; i++)
			bprintf(output, "%02x", m->site_hash[i]);
		bprintf(output, "\n");
	}
}

static struct strset zoek_urls_url;
static struct zoek_def urls_zoeks[] = {
	{ "url",	&zoek_urls_url,	ZOEK_STRSET },
	{ NULL }
};

static void
dump_urls(u64 pos, void *tmp)
{
	struct fastbuf *f = tmp;
	byte line[BUFSIZE];
	static uns oid;

	if (pos == PASS_POSITION)		// startup of a new block
	{
		oid = ~0U;
		return;
	}
	if (!f)
		return;
	if (pos < oid)				// requested former line
	{
		oid = 0;
		bsetpos(f, 0);
	}
	while (oid < pos)			// skip lines until the requested
	{
		if (!bgets(f, line, BUFSIZE))
			return;
		oid++;
	}
	if (!bgets(f, line, BUFSIZE))
		return;
	if (strset_find(&zoek_urls_url, line))
		bprintf(output, "%08x %s\n", oid, line);
	oid++;
}

static void
dump_url_index(u64 start, void *tmp)
{
	if (!tmp)
		return;
	u64 list_pos = GET_O(((byte *)tmp));
	bprintf(output, "%08x %-16Lx", (uns)start, (long long)list_pos);
	if (annotate)
		bprintf(output, " %s", get_annot(start));
	bputc(output, '\n');
}

static void
dump_string_index(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	struct fingerprint fp;
	u32 size;
	uns i;

	if (!f)
		return;
	breadb(f, &fp, sizeof(struct fingerprint));
	breadb(f, &size, sizeof(u32));
	if (verbose)
	{
		bprintf(output, "String index entry at %08llx:\n", (long long) start);
		bprintf(output, "Finger:\t");
		for (i=0; i<12; i++)
			bprintf(output, "%02x", fp.hash[i]);
		bprintf(output, "\n");
		bprintf(output, "Size:\t%d\n", size);
	}
	else
	{
		bprintf(output, "S%8llx ", (long long) start);
		for (i=0; i<12; i++)
			bprintf(output, "%02x", fp.hash[i]);
		bprintf(output, " %8d\n", size);
	}
	while (size > 0)
		size -= dump_refchain(f);
	bprintf(output, "\n");
}

static struct strset zoek_string_map_fp;
static struct zoek_def string_map_zoeks[] = {
	{ "FP",		&zoek_string_map_fp,	ZOEK_STRSET },
	{ NULL }
};

static void
dump_string_map(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	struct fingerprint fp;
	ucw_off_t ref_pos;
	uns i;

	if (!f)
	{
		if (!verbose && !bare)
			bprintf(output, "Pos      Fingerprint              RefPos\n");
		return;
	}
	breadb(f, &fp, sizeof(struct fingerprint));
	ref_pos = bgeto(f);
	byte fp_str[25];
	for (i=0; i<12; i++)
		sprintf(fp_str + i * 2, "%02x", fp.hash[i]);
	fp_str[24] = 0;
	if (!strset_find(&zoek_fingerprint_fp, fp_str))
		return;
	if (verbose)
	{
		bprintf(output, "String map entry at %08llx:\n", (long long) start);
		bprintf(output, "Finger:\t%s\n", fp_str);
		bprintf(output, "RefPos:\t%08llx\n\n", (long long) ref_pos);
	}
	else
		bprintf(output, "%8llx %s %08llx\n", (long long) start, fp_str, (long long) ref_pos);
}

static struct u64set zoek_word_index_word_id;
static struct zoek_def word_index_zoeks[] = {
	{ "WordID",	&zoek_word_index_word_id,	ZOEK_U64SET },
	{ NULL }
};

static void
dump_word_index(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	u32 wordid, size;

	if (!f)
		return;
	breadb(f, &wordid, sizeof(u32));
	breadb(f, &size, sizeof(u32));
	if (!u64set_find(&zoek_word_index_word_id, wordid))
	{
		bskip(f, size);
		return;
	}
	if (verbose)
	{
		bprintf(output, "Word index entry at %08llx:\n", (long long) start);
		bprintf(output, "Word:\t%x\n", wordid);
		bprintf(output, "Size:\t%d\n", size);
	}
	else
	{
		bprintf(output, "S%8llx %8x %8d\n", (long long) start, wordid, size);
	}
	while (size > 0)
		size -= dump_refchain(f);
	bprintf(output, "\n");
}

static void
dump_params(u64 start UNUSED, void *tmp)
{
	struct index_params *par = tmp;

	if (!par)
		return;
	bprintf(output, "Index version:\t\t%08x\n", par->version);
	bprintf(output, "Reference time:\t\t%d\n", (uns) par->ref_time);
	bprintf(output, "Lexicon config:\n");
	bprintf(output, "\tmin_len_ign\t%d\n", par->lex_config.min_len_ign);
	bprintf(output, "\tmin_len\t\t%d\n", par->lex_config.min_len);
	bprintf(output, "\tmax_len\t\t%d\n", par->lex_config.max_len);
	bprintf(output, "\tmax_num_len\t%d\n", par->lex_config.max_num_len);
	bprintf(output, "\tmax_mixed_len\t%d\n", par->lex_config.max_mixed_len);
	bprintf(output, "\tmax_ctrl_len\t%d\n", par->lex_config.max_ctrl_len);
	bprintf(output, "\tmax_gap\t\t%d\n", par->lex_config.max_gap);
	bprintf(output, "\tcontext_slots\t%d\n", par->lex_config.context_slots);
	bprintf(output, "Input objects:\t\t%d\n", par->objects_in);
	bprintf(output, "Number of sites:\t%d\n", par->sites);
	bprintf(output, "Srand:\t\t\t%d\n", par->srand);
	bprintf(output, "DB version:\t\t%08x\n", par->database_version);
	bprintf(output, "Type mask:\t\t%04x\n", par->type_mask);
	bprintf(output, "ID mask:\t\t%04x\n", par->id_mask);
	bprintf(output, "Slices:\t\t%d\n", par->num_slices);
	bprintf(output, "Output cards:\t%d\n", par->cards_out);
}

static void
dump_stems_temp(u64 start UNUSED, void *tmp)
{
	struct fastbuf *f = tmp;

	if (!f)
		return;
	uns sid = bgetl(f);
	u32 lm = bgetl(f);
	bprintf(output, "# Stem expansion table for stemmer #%08x langmask %08x\n", sid, lm);
	u32 x, y;
	while ((x = bgetl(f)) != ~0U)
	{
		y = bgetl(f);
		bprintf(output, "%08x %08x\n", x, y);
	}
}

static void
dump_stems(u64 start UNUSED, void *tmp)
{
	struct fastbuf *f = tmp;

	if (!f)
		return;
	uns sid = bgetl(f);
	u32 lm = bgetl(f);
	bprintf(output, "# Stem expansion table for stemmer #%08x langmask %08x\n", sid, lm);
	u32 x;
	while ((x = bgetl(f)) != ~0U)
	{
		if (x & 0x80000000)
			bprintf(output, "%08x\n", x & 0x7fffffff);
		else
			bprintf(output, "\t%08x\n", x);
	}
}

static void
dump_feedback_gath(u64 id, void *tmp)
{
	struct feedback_gatherer *f = tmp;
	byte *attrs = "GOI*****";
	byte at[9];

	if (!f)
	{
		if (!bare)
			bprintf(output, "ID       Footprint                         CardID   Flags    Dyn\n");
		return;
	}
	str_format_flags(at, attrs, f->flags);
	bprintf(output, "%08x ", (uns)id);
	dump_footprint(output, f->footprint);
	bprintf(output, " %08x %s %d\n", f->cardid, at, f->weight);
}

#ifdef CONFIG_IMAGES_SIM
#define IMAGE_CLUSTER_SIZE sizeof(struct image_cluster)

static u64
dump_image_signatures_unsorted_header(struct fastbuf *f)
{
	uns count = bgetl(f);
	if (verbose)
		bprintf(output, "Count: %u\n\n", count);
	return 4;
}

static void
dump_image_signatures(u64 start, void *tmp)
{
	struct fastbuf *f = tmp;
	uns oid;
	struct image_signature sig;
	int len;

	if (!f)
	{
		if (!verbose && !bare)
			bprintf(output, "Pos              OID      Len Fl  Cols  Rows  DF    DH   L   u   v  LH  HL  HH\n");
		return;
	}

	if ((oid = bgetl(f)) == ~0U)
		return;
	if ((len = bpeekc(f)) < 0)
		return;
	breadb(f, &sig, image_signature_size(len));
	if (!verbose)
		bprintf(output, "%-16Lx %-8x %3u %02x %5u %5u %3u %5u %3u %3u %3u %3u %3u %3u\n",
			(long long)start, oid, sig.len, sig.flags, sig.cols, sig.rows, sig.df, sig.dh,
			sig.vec.f[0], sig.vec.f[1], sig.vec.f[2],
			sig.vec.f[3], sig.vec.f[4], sig.vec.f[5]);
	else
	{
		bprintf(output, "Pos=%-16Lx OID=%-8x Len=%-3u Fl=%02x  w=%-5u h=%-5u DF=%-3u DH=%-5u "
			"L=%-3u u=%-3u v=%-3u LH=%-3u HL=%-3u HH=%-3u\n",
			(long long)start, oid, sig.len, sig.flags, sig.cols, sig.rows, sig.df, sig.dh,
			sig.vec.f[0], sig.vec.f[1], sig.vec.f[2],
			sig.vec.f[3], sig.vec.f[4], sig.vec.f[5]);
		for (uns i = 0; i < sig.len; i++)
			bprintf(output, "                     region #%-3u  wa=%-3u wb=%-3u I1=%-3u I2=%-3u I3=%-3u X=%-3u Y=%-3u "
				"L=%-3u u=%-3u v=%-3u LH=%-3u HL=%-3u HH=%-3u\n", i,
				sig.reg[i].wa, sig.reg[i].wb,
				sig.reg[i].h[0], sig.reg[i].h[1], sig.reg[i].h[2],
				sig.reg[i].h[3], sig.reg[i].h[4],
				sig.reg[i].f[0], sig.reg[i].f[1], sig.reg[i].f[2],
				sig.reg[i].f[3], sig.reg[i].f[4], sig.reg[i].f[5]);
		bputc(output, '\n');
	}
}

static uns image_clusters_depth;

static u64
dump_image_clusters_header(struct fastbuf *f)
{
	image_clusters_depth = bgetl(f);
	if (verbose)
		bprintf(output, "Depth=%u\n\n", image_clusters_depth);
	return 4;
}

static void
dump_image_clusters(u64 id, void *tmp)
{
	struct image_cluster *clus = tmp;
	if (!tmp)
	{
		if (!bare)
			bprintf(output, "Id                Pos/Dot    L    u    v   LH   HL   HH\n");
		return;
	}
	bprintf(output, "%-8x ", (uns)id);
	if (id < (1U << (image_clusters_depth - 1)) - 1)
		bprintf(output, "%16d %4d %4d %4d %4d %4d %4d\n", clus->dot,
			clus->vec[0], clus->vec[1], clus->vec[2],
			clus->vec[3], clus->vec[4], clus->vec[5]);
	else
		bprintf(output, "%16Lx\n", (long long)clus->pos);

}
#else
#define IMAGE_CLUSTER_SIZE 0

static u64
dump_image_signatures_unsorted_header(struct fastbuf *f UNUSED)
{
	die("Disabled CONFIG_IMAGES_SIM");
}

static void
dump_image_signatures(u64 id UNUSED, void *tmp UNUSED)
{
	die("Disabled CONFIG_IMAGES_SIM");
}

static u64
dump_image_clusters_header(struct fastbuf *f UNUSED)
{
	die("Disabled CONFIG_IMAGES_SIM");
}

static void
dump_image_clusters(u64 id UNUSED, void *tmp UNUSED)
{
	ASSERT(0);
}
#endif

struct ule {
	uns pos;
	char x[1];
};

#define HASH_NODE struct ule
#define HASH_PREFIX(x) ule_##x
#define HASH_KEY_ENDSTRING x
#define HASH_WANT_FIND
#define HASH_WANT_NEW
#include "ucw/hashtable.h"

static void
lookup_urls(struct fastbuf *b, int argc, char **argv)
{
	byte buf1[MAX_URL_SIZE];
	bb_t line;
	if (!argc)
	{
		fprintf(stderr, "Nothing to do.\n");
		exit(1);
	}
	ule_init();
	for (int i=0; i<argc; i++)
	{
		int err = url_auto_canonicalize(argv[i], buf1);
		if (err)
		{
			fprintf(stderr, "Invalid URL %s: %s\n", argv[i], url_error(err));
			exit(1);
		}
		if (!ule_find(buf1))
		{
			struct ule *e = ule_new(buf1);
			e->pos = ~0U;
		}
	}
	uns cnt = 0;
	uns remains = argc;
	bb_init(&line);
	while (bgets_bb(b, &line, ~0U))
	{
		struct ule *e = ule_find(line.ptr);
		if (e && e->pos == ~0U)
		{
			e->pos = cnt;
			if (!--remains)
				break;
		}
		cnt++;
	}
	bb_done(&line);
	for (int i=0; i<argc; i++)
	{
		url_auto_canonicalize(argv[i], buf1);
		struct ule *e = ule_find(buf1);
		ASSERT(e);
		if (e->pos != ~0U)
			bprintf(output, "%08x %s\n", e->pos, e->x);
		else
			bprintf(output, "-------- %s\n", e->x);
	}
}

static void
lookup_string_fp(int argc, char **argv)
{
	while (argc--)
	{
		struct fingerprint fp;
		fingerprint(*argv, &fp);
		for (uns i=0; i<12; i++)
			bprintf(output, "%02x", fp.hash[i]);
		bprintf(output, " %s\n", *argv);
		argv++;
	}
}

static void
lookup_url_key(int argc, char **argv)
{
	url_key_init();
	while (argc--)
	{
		byte url[MAX_URL_SIZE];
		int err = url_auto_canonicalize(*argv++, url);
		if (err)
			die("%s: %s", argv[-1], url_error(err));
		byte buf[URL_KEY_BUF_SIZE], *key;
		key = url_key(url, buf);
		struct fingerprint fp;
		fingerprint(key, &fp);
		for (uns i=0; i<12; i++)
			bprintf(output, "%02x", fp.hash[i]);
		bprintf(output, " %s\n", key);
	}
}

struct index_file {
	int option;
	char **default_filename;
	int record_size;
		/*
		 * record_size > 0:	uniform sequence with records of size record_size
		 * record_size == 0:	non-uniform sequence
		 * record_size < 0:	non-uniform sequence with records aligned to -record_size
		 */
	void (*dump_single)(u64 id, void *in);
	u64 (*dump_header)(struct fastbuf *f);
	struct zoek_def *zoek;
};

static char *fn_graph_obj_index, *fn_graph_skel_index;
static char *fn_graph_obj_real;
static char *fn_graph_obj_goes;
static char *fn_graph_obj_deg;
static char *fn_rank = "rank-obj";

enum long_opt {
	F_FIRST = 1000,
	F_FP_SPLITS,
	F_LINK_GRAPH_DEG,
	F_LINK_GRAPH_GOES,
	F_LINK_GRAPH_INDEX,
	F_LINK_GRAPH_REAL,
	F_SKEL_GRAPH,
	F_SKEL_GRAPH_INDEX,
	F_SKEL_GRAPH_VERTEX,
	F_STRING_FP,
	F_URL_KEY,
	F_URL_INDEX,
	F_ADMIN_EXPORT,
};

static struct index_file index_files[] = {
	// option		file name		record size			record dumper		header	zoek
	{ 'a',			&fn_attributes,		sizeof(struct card_attr),	dump_card_attr,		NULL,	NULL },
	{ 'c',			&fn_cards,		-(1 << CARD_POS_SHIFT),		dump_card,		NULL,	NULL },
	{ 'd',			&fn_card_attrs,		sizeof(struct card_attr),	dump_card_attr,		NULL,	NULL },
	{ 'I',			&fn_card_info,		sizeof(struct card_info),	dump_card_info,		NULL,	card_info_zoeks },
	{ 'h',			&fn_checksums,		sizeof(struct csum),		dump_checksum,		NULL,	NULL },
	{ 'F',			&fn_fingerprints,	sizeof(struct card_print),	dump_fingerprint,	NULL,	fingerprint_zoeks },
	{ F_FP_SPLITS,		&fn_fp_splits,		4,				dump_u32,		NULL,	u32_zoeks },
	{ 'l',			&fn_labels,		0,				dump_labels,		NULL,	labels_zoeks },
	{ 'L',			&fn_labels_by_id,	0,				dump_labels_id,		NULL,	labels_id_zoeks },
	{ 'x',			&fn_lexicon,		0,				dump_lexicon,		NULL,	lex_zoeks },
	{ 'Q',			&fn_lex_by_freq,	PASS_POSITION,			dump_lex_temp,		NULL,	lex_zoeks },
	{ 'R',			&fn_lex_raw,		PASS_POSITION,			dump_lex_temp,		NULL,	lex_zoeks },
	{ 'T',			&fn_lex_raw,		0,				dump_lex_stats,		NULL,	NULL },
	{ 'O',			&fn_lex_ordered,	PASS_POSITION,			dump_lex_temp,		NULL,	lex_zoeks },
	{ 'W',			&fn_lex_words,		0,				dump_lex_words,		NULL,	lex_zoeks },
	{ 'k',			&fn_links,		sizeof(struct card_print),	dump_fingerprint,	NULL,	NULL },
	{ 'K',			&fn_blacklist,		4,				dump_u32,		NULL,	u32_zoeks },
	{ 'g',			&fn_graph_obj,		0,				dump_graph,		NULL,	NULL },
	{ F_LINK_GRAPH_DEG,	&fn_graph_obj_deg,	4,				dump_u32,		NULL,	u32_zoeks },
	{ F_LINK_GRAPH_GOES,	&fn_graph_obj_goes,	4,				dump_u32,		NULL,	u32_zoeks },
	{ F_LINK_GRAPH_INDEX,	&fn_graph_obj_index,	-BYTES_PER_O,			dump_graph_index,	NULL,	NULL },
	{ F_LINK_GRAPH_REAL,	&fn_graph_obj_real,	4,				dump_u32,		NULL,	NULL },
	{ 'G',			&fn_graph_obj,		0,				NULL /* kludge */,	NULL,	NULL },
	{ 'm',			&fn_merges,		sizeof(u32),			dump_merges,		NULL,	merges_zoeks },
	{ 'o',			&fn_rank,		sizeof(float),			dump_float,		NULL,	NULL },
	{ 'n',			&fn_notes,		sizeof(struct card_note),	dump_note,		NULL,	NULL },
	{ 'N',			&fn_notes_skel,		sizeof(struct card_note),	dump_note,		NULL,	NULL },
	{ 'p',			&fn_parameters,		sizeof(struct index_params),	dump_params,		NULL,	NULL },
	{ 'P',			&fn_card_prints,	sizeof(struct card_print),	dump_fingerprint,	NULL,	NULL },
	{ 'r',			&fn_references,		0,				dump_refs,		NULL,	NULL },
	{ 't',			&fn_ref_texts,		0,				dump_ref_texts,		NULL,	NULL },
	{ 's',			&fn_sites,		sizeof(struct site_mapping),	dump_sites,		NULL,	NULL },
	{ 'U',			&fn_signatures,		0, /* computed run-time */	dump_signatures,	NULL,	signature_zoeks },
	{ 'y',			&fn_stems,		0,				dump_stems,		NULL,	NULL },
	{ 'Y',			&fn_stems_ordered,	0,				dump_stems_temp,	NULL,	NULL },
	{ 'H',			&fn_string_hash,	sizeof(u32),			dump_u32,		NULL,	u32_zoeks },
	{ 'X',			&fn_string_index,	0,				dump_string_index,	NULL,	NULL },
	{ 'M',			&fn_string_map,		0,				dump_string_map,	NULL,	string_map_zoeks },
	{ F_STRING_FP,		NULL,			0,				NULL /* kludge*/,	NULL,	NULL },
	{ 'u',			&fn_urls,		PASS_POSITION,			dump_urls,		NULL,	urls_zoeks },
	{ 'q',			&fn_urls,		0,				NULL /* kludge */,	NULL,	NULL },
	{ 'w',			&fn_word_index,		0,				dump_word_index,	NULL,	word_index_zoeks },
	{ 'B',			&fn_feedback_gath,	sizeof(struct feedback_gatherer),	dump_feedback_gath,	NULL,	NULL},
	{ 'e',			&fn_image_signatures,	0,				dump_image_signatures,	NULL,	NULL },
	{ 'E',			&fn_image_signatures_unsorted,	0,			dump_image_signatures,	dump_image_signatures_unsorted_header,	NULL },
	{ 'D',			&fn_image_clusters,	IMAGE_CLUSTER_SIZE,		dump_image_clusters,	dump_image_clusters_header,	NULL },
	{ F_SKEL_GRAPH,		&fn_graph_skel,		0,				dump_graph,		NULL,	NULL },
	{ F_SKEL_GRAPH_INDEX,	&fn_graph_skel_index,	-BYTES_PER_O,			dump_graph_index,	NULL,	NULL },
	{ F_SKEL_GRAPH_VERTEX,	&fn_graph_skel,		0,				NULL /* kludge */,	NULL,	NULL },
	{ F_URL_INDEX,		&fn_url_index,		BYTES_PER_O,			dump_url_index,		NULL,	NULL },
	{ F_URL_KEY,		NULL,			0,				NULL /* kludge */,	NULL,	NULL },
	{ F_ADMIN_EXPORT,	&fn_admin_export,	sizeof(struct admin_export),	dump_admin_export,	dump_admin_export_header,	NULL },
	{ 0,			NULL,			0,				NULL,			NULL,	NULL }
};

static char *shortopts = "0abcdef:ghi:klmnopqrstuvwxyz:ABDEFGHIKLMNOQPRTUWXY" CF_SHORT_OPTS;
static struct option longopts[] =
{
	CF_LONG_OPTS
	{ "zoek",		0, 0, 'z' },
	{ "raw",		0, 0, '0' },
	{ "bare",		0, 0, 'b' },
	{ "filename",		0, 0, 'f' },
	{ "verbose",		0, 0, 'v' },
	{ "index",		0, 0, 'i' },
	{ "admin-export",	0, 0, F_ADMIN_EXPORT },
	{ "annotate",		0, 0, 'A' },
	{ "attr",		0, 0, 'a' },
	{ "blacklist",		0, 0, 'K' },
	{ "card",		0, 0, 'c' },
	{ "card-attr",		0, 0, 'd' },
	{ "card-info",		0, 0, 'I' },
	{ "checksum",		0, 0, 'h' },
	{ "finger",		0, 0, 'F' },
	{ "fp-splits",		0, 0, F_FP_SPLITS },
	{ "float",		0, 0, 'o' },
	{ "label",		0, 0, 'l' },
	{ "labels-id",		0, 0, 'L' },
	{ "lexicon",		0, 0, 'x' },
	{ "lexicon-by-freq",	0, 0, 'Q' },
	{ "lexicon-ordered",	0, 0, 'O' },
	{ "lexicon-raw",	0, 0, 'R' },
	{ "lexicon-stats",	0, 0, 'T' },
	{ "lexicon-words",	0, 0, 'W' },
	{ "link",		0, 0, 'k' },
	{ "link-graph",		0, 0, 'g' },
	{ "link-graph-deg",	0, 0, F_LINK_GRAPH_DEG },
	{ "link-graph-goes",	0, 0, F_LINK_GRAPH_GOES },
	{ "link-graph-index",	0, 0, F_LINK_GRAPH_INDEX },
	{ "link-graph-real",	0, 0, F_LINK_GRAPH_REAL },
	{ "merge",		0, 0, 'm' },
	{ "notes",		0, 0, 'n' },
	{ "notes-skel",		0, 0, 'N' },
	{ "parameters",		0, 0, 'p' },
	{ "prints",		0, 0, 'P' },
	{ "reference",		0, 0, 'r' },
	{ "reftexts",		0, 0, 't' },
	{ "skel-graph",		0, 0, F_SKEL_GRAPH },
	{ "skel-graph-index",	0, 0, F_SKEL_GRAPH_INDEX },
	{ "skel-graph-vertex",	0, 0, F_SKEL_GRAPH_VERTEX },
	{ "site",		0, 0, 's' },
	{ "signature",		0, 0, 'U' },
	{ "stems",		0, 0, 'y' },
	{ "stems-ordered",	0, 0, 'Y' },
	{ "string-hash",	0, 0, 'H' },
	{ "string-index",	0, 0, 'X' },
	{ "string-map",		0, 0, 'M' },
	{ "string-fp",		0, 0, F_STRING_FP },
	{ "url",		0, 0, 'u' },
	{ "url-index",		0, 0, F_URL_INDEX },
	{ "url-key",		0, 0, F_URL_KEY },
	{ "url-lookup",		0, 0, 'q' },
	{ "word",		0, 0, 'w' },
	{ "feedback-gath",	0, 0, 'B' },
	{ "image-signatures",	0, 0, 'e' },
	{ "image-signatures-us",0, 0, 'E' },
	{ "image-clusters",	0, 0, 'D' },
	{ NULL,			0, 0, 0 }
};

static char *help = "\
Usage: idxdump [<options>] <index-file> [<id> | [<first-id>]-[<last-id>]]\n\
\n\
Options:\n"
CF_USAGE
"\
-A, --annotate\t\tAnnotate the output with URL's (see supported files below)\n\
-b, --bare\t\tDon't print table heading\n\
-f, --filename\t\tOverride default filename for given index file\n\
-i, --index\t\tOverride default directory containing the index\n\
-v, --verbose\t\tSet verbose mode\n\
-0, --raw\t\tDump raw data\n\
-z, --zoek\t\tOnly dump records matching given string/set (see supported files below)\n\n\
Extra formats:\n\
<int>=\\+[0-9]+\n\
<int>=(\\+0[xX])?[0-9a-fA-F]+\n\
<intset>=<int>[-<int>][,<int>[-<int>][,...]]\n\
<strset>=<str>[,<str>[,...]]\n\n\
Index files:\n\
    --admin-export\tExport for administration tools\n\
-a, --attr\t\tAttributes (--annotate)\n\
-K, --blacklist\t\tBlacklist (-z Value=<intset>)\n\
-c, --card\t\tCards\n\
-d, --card-attr\t\tCard attributes (--annotate)\n\
-I, --card-info\t\tCard info (sorted attributes + notes) for stage 2 (-z OrigCard=<intset>)\n\
-h, --checksum\t\tChecksums\n\
-B, --feedback-gath\tFeedback to the gatherer\n\
-F, --finger\t\tFingerprints (--annotate -z FP=<strset> -z CardID=<intset>)\n\
    --fp-splits\t\tFP splitting tree (-z Value=<intset>)\n\
-o, --float\t\tRank vectors\n\
-D, --image-clusters\tClusters search tree\n\
-e, --image-signatures\tImage signatures\n\
-E, --image-signatures-us\tUnsorted image signatures\n\
-l, --label\t\tLabels (-z MergedID=<intset> -z URLID=<intset> -z RedirID=<intset>)\n\
-L, --labels-id\t\tLabels by id (-z OrigID=<intset>)\n\
-x, --lexicon\t\tFinal lexicon (-z Word=<strset>)\n\
-Q, --lexicon-by-freq\tLexicon sorted by frequency (-z Word=<strset>)\n\
-O, --lexicon-ordered\tLexicon after lexorder (-z Word=<strset>)\n\
-W, --lexicon-words\tLexicon after wsort (-z Word=<strset>)\n\
-R, --lexicon-raw\tRaw lexicon (-z Word=<strset>)\n\
-T, --lexicon-stats\tRaw lexicon statistics\n\
-k, --link\t\tLinks by url\n\
-g, --link-graph\tLink graph\n\
    --link-graph-deg\t... vertex degrees (-z Value=<intset>)\n\
    --link-graph-index\t... translated vertex -> position\n\
    --link-graph-goes\t... real vertex -> translated (-z Value=<intset>)\n\
    --link-graph-real\t... translated vertex -> real (-z Value=<intset>)\n\
-G, --link-graph-vertex\t... find a given vertex\n\
-m, --merge\t\tMerges (--annotate -z Class=<intset>)\n\
-n, --notes\t\tNotes (--annotate)\n\
-N, --notes-skel\tNotes for skeletons\n\
-p, --parameters\tIndex parameters\n\
-P, --prints\t\tCard fingerprints\n\
-r, --reference\t\tReferences\n\
-t, --reftexts\t\tReference texts\n\
    --skel-graph\tSkeleton link graph\n\
    --skel-graph-index\t... vertex -> position\n\
    --skel-graph-vertex\t... find a given vertex\n\
-U, --signature\t\tSignatures (-z CardID=<intset>)\n\
-s, --site\t\tSites\n\
-y, --stems\t\tStem mappings\n\
-Y, --stems-ordered\tTemporary stem mappings from lexorder\n\
-H, --string-hash\tString hash (-z Value=<intset>)\n\
-X, --string-index\tString index\n\
-M, --string-map\tString map (-z FP=<strset>)\n\
    --string-fp\tCalculate fingerprint for a given string\n\
-u, --url\t\tUrl list (-z URL=<strset>)\n\
    --url-index\t\tUrl index (--annotate)\n\
    --url-key\t\tCalculate URL key and fingerprint for a given URL\n\
-q, --url-lookup\tFind given URL's in the URL list\n\
-w, --word\t\tWord index (-z WordID=<intset>)\n\
";

static void NONRET
usage(byte *msg)
{
	if (msg)
	{
		fputs(msg, stderr);
		fputc('\n', stderr);
	}
	fputs(help, stderr);
	exit(1);
}

static void
dump_index_interval(struct index_file *f, struct fastbuf *b, u64 start, u64 stop, u64 header_size)
{
	if (f->record_size == (int) PASS_POSITION)
	{
		ucw_off_t size = bfilesize(b);
		f->dump_single(PASS_POSITION, b);		// startup
		for (u64 pos=start; pos<=stop; pos++)
		{
			f->dump_single(pos, b);
			if (btell(b) >= size)
				break;
		}
	}
	else if (f->record_size > 0)
	{
		byte *buf = alloca(f->record_size);
		bsetpos(b, header_size + f->record_size * start);
		while (start <= stop)
		{
			if (!breadb(b, buf, f->record_size))
				break;
			f->dump_single(start, buf);
			start++;
		}
	}
	else
	{
		if (f->record_size < 0)
		{
			start *= -f->record_size;
			if (stop != ~0ULL)
				stop *= -f->record_size;
		}
		else
			start = MAX(start, header_size);
		if (stop == ~0ULL)
		{
			bseek(b, 0, SEEK_END);
			stop = btell(b);
			if (!stop)
				return;
			stop--;
		}
		bsetpos(b, start);
		while ((u64) (start = btell(b)) <= stop)
			f->dump_single(start, b);
	}
}

static void
dump_index_file(struct index_file *f, struct fastbuf *b, int argc, char **argv)
{
	switch (f->option)
	{
	case 'U':
		/* The file dumped is just a temporary file of the process of
		 * building the index, hence we ignore the contingency that
		 * matcher_signatures could have changed.  The user can modify
		 * its value by setting -Smatcher.signatures anyway.  */
		f->record_size = sizeof(uns) + matcher_signatures * sizeof(u32);
		break;
	case 'q':
		/* The URL lookup function has non-standard arguments */
		lookup_urls(b, argc, argv);
		return;
	case 'G':
	case F_SKEL_GRAPH_VERTEX:
		/* As have the graph lookup functions */
		dump_graph_vertex(b, argc, argv);
		return;
	case F_STRING_FP:
		lookup_string_fp(argc, argv);
		return;
	case F_URL_KEY:
		lookup_url_key(argc, argv);
		return;
	}
	u64 header_size = 0;
	if (f->dump_header)
		header_size = f->dump_header(b);
	f->dump_single(0, NULL);
	if (argc == 0)
		dump_index_interval(f, b, 0, ~0ULL, header_size);
	else
		for (int i=0; i<argc; i++)
		{
			u64 start = 0;
			u64 stop = ~0ULL;
			byte *c = strchr(argv[i], '-');
			if (c)
			{
				*c++ = 0;
				if (*c)
					stop = xtol64(c);
				if (*argv[i])
					start = xtol64(argv[i]);
			}
			else
				start = stop = xtol64(argv[i]);
			if (start > stop)
				usage("Invalid ID range");
			dump_index_interval(f, b, start, stop, header_size);
		}
}

int
main(int argc, char **argv)
{
	byte *force_filename = NULL;
	struct index_file *f = NULL;
	int opt, i;
	struct fastbuf *b, *output_stdout;

	log_init(argv[0]);
	while ((opt = cf_getopt(argc, argv, shortopts, longopts, NULL)) >= 0)
		switch (opt)
		{
			case 'A':
				annotate++;
				break;
			case 'f':
				force_filename = optarg;
				break;
			case 'v':
				verbose++;
				break;
			case 'b':
				bare++;
				break;
			case '0':
				raw++;
				break;
			case 'i':
				fn_directory = optarg;
				break;
			case 'z':
				if (!f)
					usage("Option --zoek must follow an index file");
				if (!f->zoek)
					usage("Option --zoek is not supported for this index file");
				zoek_append(f->zoek, optarg);
				break;
			default:
				for (i=0; ; i++)
					if (opt == index_files[i].option)
					{
						if (f)
							usage("More index files specified");
						f = &index_files[i];
						if (f->zoek)
							zoek_init(f->zoek);
						break;
					}
					else if (!index_files[i].option)
						usage("Invalid option");
		}
	if (!f)
		usage("No index file specified");
	if (f->zoek)
		zoek_commit(f->zoek);
	if (!force_filename && f->default_filename)
	{
		fn_graph_obj_index = cf_printf("%s" FN_GRAPH_INDEX, fn_graph_obj);
		fn_graph_obj_real = cf_printf("%s" FN_GRAPH_REAL, fn_graph_obj);
		fn_graph_obj_goes = cf_printf("%s" FN_GRAPH_GOES, fn_graph_obj);
		fn_graph_obj_deg = cf_printf("%s" FN_GRAPH_DEG, fn_graph_obj);
		fn_graph_skel_index = cf_printf("%s" FN_GRAPH_INDEX, fn_graph_skel);
		force_filename = index_name(*f->default_filename);
	}
	term_charset_id = find_charset_by_name(terminal_charset);
	if (term_charset_id < 0)
		die("Unknown terminal charset %s", terminal_charset);
	output_stdout = output = bfdopen(1, 4096);
	if (f->option == 'c')
		liz_buf = lizard_alloc();	// charset conversion handled by objdump
	else if (!raw)
		output = fb_wrap_charconv_out(output, CONV_CHARSET_UTF8, term_charset_id);
	b = force_filename ? bopen(force_filename, O_RDONLY, 1<<20) : NULL;
	dump_index_file(f, b, argc - optind, argv + optind);
	if (liz_buf)
		lizard_free(liz_buf);
	bclose(b);
	if(output != output_stdout)
		bclose(output);
	bclose(output_stdout);
	return 0;
}
