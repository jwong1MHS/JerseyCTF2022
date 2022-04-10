/*
 *	Sherlock Gatherer -- Content Encoding and Type Identification
 *
 *	(c) 1997--2002 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2006 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/clists.h"
#include "ucw/simple-lists.h"
#include "ucw/conf.h"
#include "ucw/chartype.h"
#include "ucw/fastbuf.h"
#include "ucw/url.h"
#include "ucw/string.h"
#include "gather/gather.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

/* Structures */

static const char * const mode_names[] = { "suffix", "wildcard", "bytes", "isascii", NULL };	// doesn't contain "extension" for purpose
enum cbmode { CBMODE_SUFFIX, CBMODE_WILDCARD, CBMODE_BINARY, CBMODE_IS_ASCII, CBMODE_EXTENSION, CBMODE_END };
enum cbresult { CBRES_DONT_KNOW, CBRES_TRUE, CBRES_FALSE, CBRES_END };

struct crystal_ball {			/* Magic rules for datomanthy */
	cnode n;
	int mode;			/* Crystal ball type */
	char *type;			/* Resulting type */
	char **rule;			/* Name or contents pattern */
	char **compiled;		/* Compiled rule */
};

/* Configuration */

static uns trace_contents;
#define TRACE(x,y...) do { if (trace_contents) log(L_DEBUG, x,##y); } while (0)
#define XTRACE(x,y...) do { if (trace_contents > 1) log(L_DEBUG, x,##y); } while (0)
#define XXTRACE(x,y...) do { if (trace_contents > 2) log(L_DEBUG, x,##y); } while (0)

static clist
	inenc_filters,		/* Input content type / content encoding mapping */
	intype_filters,
	encodings,		/* Encoding => drop suffix */
	type_to_encs;		/* Content type => encoding mapping */
struct clist crystal_balls;	/* contains only modes != CBMODE_EXTENSION */
struct clist ext_crystal_balls;	/* contains only modes == CBMODE_EXTENSION */
static int ascii_fraction = 1000;

struct extension_record {
	byte *extension;	/* key */
	byte *content_type;
};
#define HASH_NODE struct extension_record
#define HASH_PREFIX(p) ext_##p
#define HASH_KEY_STRING extension
#define HASH_WANT_FIND
#define HASH_WANT_NEW
#define HASH_USE_POOL cf_pool
#define HASH_NOCASE
#define HASH_CONSERVE_SPACE
#define HASH_TABLE_ALLOC
#include "ucw/hashtable.h"

struct content_type_record {
	byte *content_type;	/* key */
	struct crystal_ball *cb;
};
#define HASH_NODE struct content_type_record
#define HASH_PREFIX(p) ct_##p
#define HASH_KEY_STRING content_type
#define HASH_WANT_FIND
#define HASH_WANT_FIND_NEXT
#define HASH_WANT_NEW
#define HASH_USE_POOL cf_pool
#define HASH_CONSERVE_SPACE
#define HASH_TABLE_ALLOC
#include "ucw/hashtable.h"

static byte *snerr = "Syntax error";

static byte *
commit_ball(struct crystal_ball *f)
{
	uns size = f->rule ? DARY_LEN(f->rule) : 0;
	if (f->mode != CBMODE_IS_ASCII && !size)
		return "Expecting at least one rule";
	if (f->mode != CBMODE_BINARY)
		return NULL;
	CF_JOURNAL_VAR(f->compiled);
	f->compiled = cf_malloc(size * sizeof(byte*));
	for (uns idx=0; idx<size; idx++)
	{
		int z, b = 0;
		byte *c = f->rule[idx];
		byte *d;

		while (*c)			/* Estimate length of compiled pattern */
		{
			if (!Cdigit(*c))
				return snerr;
			while (Cdigit(*c))
				c++;
			if (*c++ != ':')
				return snerr;
			if (*c == '"')
			{
				c++;
				while (*c != '"')
				{
					if (!*c)
						return snerr;
					c++, b += 2;
				}
				c++;
			}
			else if (Cxdigit(*c))
			{
				while (Cxdigit(*c))
					c++, b++;
				if (b & 1)
					return snerr;
			}
			else
				return snerr;
			if (*c)
			{
				if (*c == ',')
					c++;
				else
					return snerr;
			}
		}

		d = f->compiled[idx] = cf_malloc(b + 1);	/* Compile the pattern */
		c = f->rule[idx];
		while (*c)
		{
			b = 0;
			while (*c != ':')
				b = 10*b + *c++ - '0';
			if (b < 0 || b > 254)
				return "Position out of range";
			if (*++c == '"')
			{
				c++;
				while (*c != '"')
				{
					*d++ = b++;
					*d++ = *c++;
				}
				c++;
			}
			else while (Cxdigit(*c))
			{
				int d0, d1;
				*d++ = b++;
				d0 = *c++;
				d1 = *c++;
				z = (Cxvalue(d0) << 4) | Cxvalue(d1);
				*d++ = z;
			}
			if (b > 254)
				return "Position out of range";
			if (*c)
				c++;
		}
		*d = 0xff;
	}
	return NULL;
}

static byte *
construct_hash_tables(void *ptr UNUSED)
{
	struct crystal_ball *f;
	CF_JOURNAL_VAR(ext_table);
	ext_init();
	CLIST_WALK(f, ext_crystal_balls)
	{
		f->mode = CBMODE_EXTENSION;		// no need to journal, this should have been done in the init-hook and it is constant
		uns size = f->rule ? DARY_LEN(f->rule) : 0;
		for (uns idx=0; idx<size; idx++)
		{
			struct extension_record *rec = ext_new(f->rule[idx]);
			rec->content_type = f->type;
		}
	}
	CF_JOURNAL_VAR(ct_table);
	ct_init();
	CLIST_WALK(f, crystal_balls)
	{
		struct content_type_record *ctr;
		ctr = ct_new(f->type);
		ctr->cb = f;
	}
	return NULL;
}

static byte *
check_destination(simp2_node *ptr)
{
	return ptr->s2 ? NULL : "Missing destination";
}

static struct cf_section cball_config = {
	CF_TYPE(struct crystal_ball),
	CF_COMMIT(commit_ball),
#define F(x)	PTR_TO(struct crystal_ball, x)
	CF_ITEMS {
		CF_LOOKUP("Mode", F(mode), mode_names),
		CF_STRING("Type", F(type)),
		CF_STRING_DYN("rule", F(rule), 100),
		CF_END
	}
#undef F
};
static struct cf_section ext_cball_config = {
	CF_TYPE(struct crystal_ball),
#define F(x)	PTR_TO(struct crystal_ball, x)
	CF_ITEMS {
		CF_STRING("Type", F(type)),
		CF_STRING_DYN("rule", F(rule), 100),
		CF_END
	}
#undef F
};

static struct cf_section mapping_config;	// initialized in the constructor
static struct cf_section content_config = {
	CF_COMMIT(construct_hash_tables),
	CF_ITEMS {
		CF_UNS("Trace", &trace_contents),
		CF_LIST("InEnc", &inenc_filters, &mapping_config),
		CF_LIST("InType", &intype_filters, &mapping_config),
		CF_LIST("Encoding", &encodings, &cf_2string_list_config),
		CF_LIST("TypeEnc", &type_to_encs, &mapping_config),
		CF_LIST("Rule", &crystal_balls, &cball_config),
		CF_LIST("ExtRule", &ext_crystal_balls, &ext_cball_config),
		CF_INT("AsciiFraction", &ascii_fraction),
		CF_END
	}
};

static void CONSTRUCTOR
content_init_config(void)
{
	mapping_config = cf_2string_list_config;
	mapping_config.commit = (cf_hook*) check_destination;
	cf_declare_section("Content", &content_config, 0);
}

/* Perform input filtering of content types and encodigs */

static byte *enc_m, *type_m, *rename_m;		/* Logging messages */

static inline byte *
canonicalize_content_type(byte *ct)
{
	if (!ct || !strcmp(ct, "UNKNOWN"))
		return NULL;
	else
		return ct;

}

static int
perform_translation(clist *filters, byte **type, byte *which)
{
	simp2_node *f;
	int i = 0;

	if (!*type)
		return 0;
	CLIST_WALK(f, *filters)
	{
		int res;
		i++;
		if (which[8] == 'T')
			res = match_ct_patt(f->s1, *type);
		else if (which[8] == 'E')
			res = !strcasecmp(f->s1, *type);
		else
			ASSERT(0);
		if (res)
		{
			XTRACE("Rule #%d: In filter: %s=`%s'", i, which, f->s2);
			if (!strcmp(f->s2, "ERROR"))
			{
				log(L_ERROR_R, "Suspicious %s: %s (rule %d)", which, *type, i);
				*type = NULL;
			}
			else
			{
				*type = canonicalize_content_type(f->s2);
			}
			return 1;
		}
	}
	return 0;
}

static int
maybe_encoding(byte **enc,byte **type)			/* Maybe our content type is just an encoding? */
{
	simp2_node *f;
	int i = 0;

	if(*enc || !*type)
		return 0;
	CLIST_WALK(f, type_to_encs)
	{
		i++;
		if (match_ct_patt(f->s1, *type))
		{
			XTRACE("Rule #%d: Content type %s -> Encoding %s", i, *type, f->s2);
			*type = NULL;
			*enc = f->s2;
			enc_m = "[from content-type] ";
			return 1;
		}
	}
	return 0;
}

static int
encoding_to_type(byte *enc,byte **type)			/* Inverse conversion for checking the Content-Type */
{
	simp2_node *f;
	int i = 0;

	CLIST_WALK(f, type_to_encs)
	{
		i++;
		if (!strcasecmp(f->s2, enc))
		{
			XTRACE("Rule #%d: Encoding %s -> Content type %s", i, enc, f->s1);
			*type = f->s1;
			return 1;
		}
	}
	XTRACE("Content-Encoding %s can not be converted to Content-Type", enc);
	*type = NULL;
	return 0;
}

/* Content encodings */

static inline void
chop_off_suffix(byte *base, byte *s)	/* Chop off filename suffix when decoding */
{
	int l = strlen(base);
	int k = strlen(s);

	if (l >= k && !strcmp(base + l - k, s))
	{
		base[l - k] = 0;
		XTRACE("Renaming to `%s' by cutting suffix `%s'", base, s);
		rename_m = " [suffix cut]";
	}
}

void
cut_inenc_suffix(byte *name, byte *enc)	/* Cut the optional file suffix and return the encoding number */
{
	struct simp2_node *e;

	if (!enc || !name)
		return;

	CLIST_WALK(e, encodings)
		if (!strcasecmp(e->s1, enc))
		{
			if (e->s2)
				chop_off_suffix(name, e->s2);
			return;
		}
}

/* Content type vaticination */

#define	BUFSIZE		4096
#define	HEADSIZE	256

static struct fastbuf *dt_fb;
static byte dt_head[HEADSIZE];
static uns dt_size;
static char *dt_name, *dt_type, *dt_enc;
static uns dt_name_len;

#define	MAX_GRADE	1000

static inline int
is_ascii(void)
{
	byte buf[BUFSIZE];
	uns pos = 0;
	uns non_ascii = 0;

	bseek(dt_fb, 0, SEEK_SET);
	while (1)
	{
		byte *k;
		uns l;
		if (! (l = bread(dt_fb, buf, BUFSIZE)) )
			break;
		for(k=buf; l; k++, l--)
			if (*k < 0x20 && !Cblank(*k))
			{
				if (non_ascii < 50)
					XXTRACE("Found non-ASCII character %x at position %d", *k, (uns)(pos+k-buf));
				non_ascii++;
			}
		pos += k-buf;
	}
	if (non_ascii)
	{
		uns limit = (MAX_GRADE - ascii_fraction) * pos / MAX_GRADE;
		XXTRACE("Found %d non-ASCII characters among %d, limit is %d", non_ascii, pos, limit);
		if (non_ascii > limit)
			return 0;
	}
	return 1;
}

static inline int
match_binary_pattern(byte *r)
{
	uns k, l;

	while (*r != 0xff)
	{
		k = *r++;		/* Address */
		l = *r++;		/* Data */
		if (k >= dt_size || l != dt_head[k])
		{
			XXTRACE("Binary pattern mismatch at position %d: %x!=%x", k, dt_head[k], l);
			return 0;
		}
	}
	XXTRACE("Binary pattern matches");
	return 1;
}

/* Reset at the beginning of vaticinate():  */
static int plausible_name;		/* set to 0 iff the filename contains parameters (probably a script) */
static int ascii_result;		/* cache flag */

static enum cbresult
evaluate_crystal_ball(struct crystal_ball *f)
{
	switch (f->mode)
	{
		case CBMODE_BINARY:
			if (!dt_fb)
				return CBRES_DONT_KNOW;
			for (uns idx=0; idx<DARY_LEN(f->rule); idx++)
				if (match_binary_pattern(f->compiled[idx]))
					return CBRES_TRUE;
			return CBRES_FALSE;
		case CBMODE_SUFFIX:
			if (!dt_name || !plausible_name)
				return CBRES_DONT_KNOW;
			for (uns idx=0; idx<DARY_LEN(f->rule); idx++)
			{
				int start = dt_name_len - strlen(f->rule[idx]);
				if (start >= 0 && !strcasecmp(f->rule[idx], dt_name + start))
					return CBRES_TRUE;
			}
			return CBRES_FALSE;
		case CBMODE_WILDCARD:
			if (!dt_name || !plausible_name)
				return CBRES_DONT_KNOW;
			for (uns idx=0; idx<DARY_LEN(f->rule); idx++)
				if(str_match_pattern_nocase(f->rule[idx], dt_name))
					return CBRES_TRUE;
			return CBRES_FALSE;
		case CBMODE_IS_ASCII:
			if (!dt_fb)
				return CBRES_DONT_KNOW;
			if (ascii_result < 0)
				ascii_result = is_ascii();
			if (!ascii_result)
				return CBRES_FALSE;
			break;
		case CBMODE_EXTENSION:
			/* This is an exception, it takes part in when called
			 * from verification branch of vaticinate().  */
			if (!plausible_name)
				return CBRES_DONT_KNOW;
			else
				return CBRES_FALSE;
		default:
			die("Unknown crystal ball #%d", f->mode);
	}
	return CBRES_TRUE;
}

static int
validate_content_type(byte *ct, byte **type)
{
	struct crystal_ball *g;
	int valid = 1, binary_res[CBRES_END];
	bzero(binary_res, sizeof(binary_res));
	CLIST_WALK(g, crystal_balls)
		if (!strcmp(ct, g->type))
		{
			if (g->mode ==  CBMODE_IS_ASCII
			&& evaluate_crystal_ball(g) == CBRES_FALSE)
			{
				valid = 0;
				break;
			}
			if (g->mode == CBMODE_BINARY)
				binary_res [ evaluate_crystal_ball(g) ]++;
		}
	if (binary_res[ CBRES_FALSE ] && !binary_res[ CBRES_TRUE ])
		valid = 0;
	XTRACE("Looks like Content-Type %s, %s", ct, valid ? "accepted" : "rejected");
	if (valid)
	{
		*type = canonicalize_content_type(ct);
		type_m = "[guessed] ";
	}
	return valid;
}

static int
vaticinate(byte **type)		/* Check the existing Content-Type or find the first matching one if *type==NULL */
{
	byte *dt_name_dot = NULL;
	plausible_name = ! strchr(dt_name, '?');
	if (plausible_name)
		dt_name_dot = strrchr(dt_name, '.');
	dt_name_len = strlen(dt_name);
	ascii_result = -1;

	if (!*type)
	{
		struct crystal_ball *f;

		CLIST_WALK(f, crystal_balls)
		{
			if (f->mode == CBMODE_BINARY && evaluate_crystal_ball(f) == CBRES_TRUE)
			{
				if (validate_content_type(f->type, type))
					return 1;
			}
		}
		if (ext_table.hash_count > 0 && dt_name_dot)
		{
			struct extension_record *hash_rec = ext_find(dt_name_dot + 1);
			if (hash_rec && validate_content_type(hash_rec->content_type, type))
				return 1;
		}
		CLIST_WALK(f, crystal_balls)
		{
			if (f->mode != CBMODE_BINARY && evaluate_crystal_ball(f) == CBRES_TRUE)
			{
				if (validate_content_type(f->type, type))
					return 1;
			}
		}
		XTRACE("No rule to guess Content-Type");
		type_m = "[unrecognized] ";
		return 0;
	}
	else
	{
		int results[CBMODE_END][CBRES_END];	/* number of results[CBMODE_*][CBRESULT_*] */
		struct content_type_record *ctr;

		memset(results, 0, sizeof(results));
		for (ctr=ct_find(*type); ctr; ctr=ct_find_next(ctr))
		{
			struct crystal_ball *f = ctr->cb;
			int res = evaluate_crystal_ball(f);	/* CBMODE_EXTENSION rules are also included */
			results[f->mode][res] ++;
		}
		if (ext_table.hash_count > 0 && dt_name_dot
		&& ext_find(dt_name_dot + 1))
		{
			results[CBMODE_EXTENSION][CBRES_TRUE]++;
			results[CBMODE_EXTENSION][CBRES_FALSE]--;
		}
		for (int j=0; j<CBRES_END; j++)
		{
			results[CBMODE_SUFFIX][j] += results[CBMODE_WILDCARD][j];
			results[CBMODE_SUFFIX][j] += results[CBMODE_EXTENSION][j];
			results[CBMODE_WILDCARD][j] = 0;
			results[CBMODE_EXTENSION][j] = 0;
		}
		type_m = "[confirmed] ";
		if (results[CBMODE_SUFFIX][CBRES_FALSE]
		&& !results[CBMODE_SUFFIX][CBRES_TRUE])
		{
			XTRACE("Unknown suffix `%s' for Content-Type %s, but it does not matter", dt_name, *type);
			type_m = "[confirmed but suffix] ";
		}
		if (results[CBMODE_BINARY][CBRES_FALSE]
		&& !results[CBMODE_BINARY][CBRES_TRUE])
		{
			XTRACE("Content-Type %s is not faithful -- all binary patterns are invalid", *type);
			type_m = "[rejected pattern] ";
			return 0;
		}
		if (results[CBMODE_IS_ASCII][CBRES_FALSE])
		{
			XTRACE("Content-Type %s is not faithful -- not an ascii file", *type);
			type_m = "[rejected ascii] ";
			return 0;
		}
		XTRACE("Content-Type %s is faithful", *type);
		return 1;
	}
}

/* Setting the initial values of Content-Encoding and Content-Type.  */

void
set_content_encoding(byte *ce)
{
	gthis->content_encoding = ce;
	if (perform_translation(&inenc_filters, &ce, "Content-Encoding"))
	{
		TRACE("Content-Encoding translated from %s into %s",
			gthis->content_encoding ? : (byte*)"?",
			ce ? : (byte*)"?");
		gthis->content_encoding = ce;
	}
}

void
set_content_type(byte *ct)
{
	gthis->content_type = ct;
	if (perform_translation(&intype_filters, &ct, "Content-Type"))
	{
		TRACE("Content-Type translated from %s into %s",
			gthis->content_type ? : (byte*)"?",
			ct ? : (byte*)"?");
		gthis->content_type = ct;
	}
}

/* Parsing of the content type field */

static int
check_charset_name(byte *x)
{
  while (*x)
    {
      if (*x <= ' ' || *x >= 0x7f)
	return 0;
      x++;
    }
  return 1;
}

void
parse_content_type(byte *l, byte **charset)
{
	byte *x, *y;

	/* Split the Content-Type value to real content type and charset info */

	while (*l && !Cspace(*l) && *l != ';')
		l++;
	while (*l)
	{
		if (Cspace(*l) || *l == ';')
		{
			*l++ = 0;
			continue;
		}
		x = l;
		while (*l && !Cspace(*l) && *l != ';' && *l != '=')
			l++;
		if (*l != '=')
			continue;
		*l++ = 0;
		y = l;
		if (*l == '"')
		{
			y++, l++;
			while (*l && *l != '"')
				if (*l == '\\' && l[1] == '"')
					l += 2;
				else
					l++;
			if (!*l)
				break;
			*l++ = 0;
		}
		else
		{
			while (*l && !Cspace(*l) && *l != ';')
				l++;
			if (*l)
				*l++ = 0;
		}
		/* x is the key, y is the value */
		if (!strcasecmp(x, "charset") && check_charset_name(y))
			*charset = y;
	}
}

/* DATA TYPE PROCESSING -- MAIN ENTRY POINT */

static void
do_guess_content(void)
{
	byte *enc = dt_enc;
	byte *type = dt_type;
	int count;

	for (count=0; count<2; count++)			/* Run twice, because C-E gzip may be recognised from C-T app/gzip in 2nd pass */
	{
		byte namebuf[MAX_URL_SIZE], *name = NULL;
		uns rejected = 0;
		enc_m = type_m = rename_m = "";

		if (count==1 && !maybe_encoding(&enc, &type))	/* Content-Type actually converted to Content-Encoding */
			break;
		if (enc)					/* Virtually decode encoded files */
		{
			byte *orig_type = type;
			if (!encoding_to_type(enc, &type))	/* Trust unsupported Content-Encoding */
			{
				XTRACE("Unsupported Content-Encoding=%s with Content-Type=%s", enc, orig_type ? : (byte*) "?");
				type = NULL;
				enc_m = "[unsupported but trusted] ";
			}
			if (type && !vaticinate(&type))		/* We can confirm that it isn't properly encoded */
			{
				enc = type = NULL;
				enc_m = "[rejected] ";
			}
			else					/* Check the encoded contents */
			{
				type = orig_type;
				name = namebuf;
				strcpy(name, dt_name);
				cut_inenc_suffix(dt_name,enc);	/* Rename the file and hide the contents */
				bclose(dt_fb);
				dt_fb = NULL;
			}
			/* Fall-thru into checking the Content-Type */
		}
		if (!type || !vaticinate(&type))		/* Ignore non-faithful Content-Type */
		{
			if (type)
			{
				type = NULL;
				rejected = 1;
			}
			vaticinate(&type);			/* Find the best candidate */
		}

		TRACE("Contents: encoding %s%s->%s, type %s%s%s->%s%s for %s",
			enc_m,
			dt_enc ? : "?",
			enc ? : (byte*) "?",
			(byte*) (rejected ? "[rejected] " : ""),
			type_m,
			dt_type ? : "?",
			type ? : (byte*) "?",
			rename_m,
			dt_name ? : "?");
		if (name)
			strcpy(dt_name, name);
		dt_enc = enc;
		dt_type = type;
	}

	bclose(dt_fb);
}

void
guess_content(void)		/* Modify content_{encoding,type} depending on all known attributes */
{
	dt_name = gthis->file_name ? : gthis->url_s.rest;
	dt_enc = gthis->content_encoding;
	dt_type = gthis->content_type;
	if (gthis->contents)				/* Read the head to check the binary patterns */
	{
		dt_fb = fbmem_clone_read(gthis->contents);
		dt_size = bread(dt_fb, dt_head, HEADSIZE);
		XXTRACE("Read %d bytes from head of the file", dt_size);
	}
	else
		dt_fb = NULL;
	do_guess_content();
	gthis->content_encoding = dt_enc;
	gthis->content_type = dt_type;
}

void
guess_content_by_name(byte *name, byte **type, byte **enc)
{
	dt_name = name;
	dt_enc = NULL;
	dt_type = NULL;
	dt_fb = NULL;
	do_guess_content();
	*type = dt_type;
	*enc = dt_enc;
}
