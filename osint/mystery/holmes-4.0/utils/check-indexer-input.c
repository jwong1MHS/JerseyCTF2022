/*
 *	Sherlock Utilities -- Simple Checker of Textual Input for Customizations
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "sherlock/object.h"
#include "sherlock/objread.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/getopt.h"
#include "ucw/url.h"
#include "ucw/bbuf.h"

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>

static byte *custom_attrs = "0123456789";
static byte *custom_subobjs = "";
static byte *single_attrs = "UlLv";
static struct fastbuf *fb;
static struct mempool *mp;
static struct odes *o;
static unsigned long long lineno, obj_lineno;
static uns objno;
static byte *url;
static uns check_dups;

static byte *
read_line(void)
{
	ucw_off_t p = btell(fb);
	byte *s = bgets_mp(fb, mp);
	if (!s)
		return NULL;
	lineno++;
	if (strlen(s) != (uns)(btell(fb) - p - 1))
		log(L_ERROR, "Line %llu: Found a zero character", lineno);
	return s;
}

static uns
read_object(void)
{
	o = obj_new(mp);
	url = mp_printf(mp, "Object at line %llu", obj_lineno = (lineno + 1));
	byte *line;
	struct obj_read_state st;
	uns end = 1;
	obj_read_start(&st, o);
	while (line = read_line()) {
		if (!line[0]) {
			end = 0;
			break;
		}
		obj_read_attr(&st, line[0], line+1);
	}
	obj_read_end(&st);
	if (end) {
		if (o->attrs)
			log(L_ERROR, "Truncated last record (expected an empty line before EOF)");
		return 0;
	}
	objno++;
	return 1;
}

struct hurl {
	long long lineno;
	byte url[1];
};

#define HASH_PREFIX(x) hurl_##x
#define HASH_NODE struct hurl
#define HASH_KEY_ENDSTRING url
#define HASH_AUTO_POOL 4096
#define HASH_WANT_FIND
#define HASH_WANT_NEW
#include "ucw/hashtable.h"

struct hfp {
	byte fp[sizeof(struct fingerprint)];
	long long lineno;
};

#define HASH_PREFIX(x) hfp_##x
#define HASH_NODE struct hfp
#define HASH_KEY_MEMORY fp
#define HASH_KEY_SIZE sizeof(struct fingerprint)
#define HASH_AUTO_POOL 4096
#define HASH_WANT_FIND
#define HASH_WANT_NEW
#include "ucw/hashtable.h"

static void
check_url(void)
{
	byte *u = obj_find_aval(o, 'U');
	if (!u)
		log(L_ERROR, "%s: Missing URL", url);
	else if (strlen(u) >= MAX_URL_SIZE - 1)
		log(L_ERROR, "%s: Too long URL", url);
	else {
		url = u;
		byte canon[MAX_URL_SIZE];
		if (url_auto_canonicalize(url, canon))
			log(L_ERROR, "%s: Invalid URL format", url);
		else {
			struct hurl *hurl = hurl_find(canon);
			if (hurl)
				log(L_ERROR, "%s: Duplicate normalized URL (lines %llu and %llu)", url, hurl->lineno, obj_lineno);
			else {
				hurl_new(canon)->lineno = obj_lineno;
				if (check_dups) {
					struct fingerprint fp;
					url_fingerprint(canon, &fp);
					struct hfp *hfp = hfp_find((void *)&fp);
					if (hfp)
						log(L_WARN, "%s: Duplicate URL hash (lines %llu and %llu)", url, hfp->lineno, obj_lineno);
					else
						hfp_new((void *)&fp)->lineno = obj_lineno;
				}
			}
		}
	}

}

static void
check_version(void)
{
	byte *v = obj_find_aval(o, 'v');
	if (!v)
		log(L_ERROR, "%s: Missing object version (expected 'v2')", url);
	else if (strcmp(v, "2"))
		log(L_ERROR, "%s: Invalid object version (expected 'v2')", url);
}

static uns
check_utf8(byte **p)
{
#define CHECK_NEXT if (unlikely((**p & 0xc0) != 0x80)) goto bad; (*p)++
	uns u = *(*p)++;
	if (u < 0x80)
		;
	else if (unlikely(u < 0xc0)) {
bad:
		return 0;
	}
	else if (u < 0xe0) {
		CHECK_NEXT;
	}
	else if (likely(u < 0xf0)) {
		CHECK_NEXT;
		CHECK_NEXT;
	}
	else
		goto bad;
	return 1;
}

static uns
check_printable(byte **p)
{
	if (**p < ' ' && **p != '\t') {
		log(L_ERROR, "%s: Found non-printable character 0x%02x", url, **p);
		return 0;
	}
	else if (!check_utf8(p)) {
		log(L_ERROR, "%s: Corrupted UTF-8 encoding", url);
		return 0;
	}
	return 1;
		
}

static void
check_meta(byte *val)
{
	/* Consume weight if present */
	if (*val >= '0' && *val <= '3')
		val++;
	/* Check type tag */
	if (!*val) {
		log(L_ERROR, "%s: Missing meta type tag", url);
		return;
	}
	if ((*val & 0xf0) != 0x80 && (*val & 0xf0) != 0x90) {
		log(L_ERROR, "%s: Invalid format of meta type tag", url);
		return;
	}
	if ((*val & 0x0f) >= MT_MAX) {
		log(L_ERROR, "%s: Too large meta type %d", url, *val & 0x0f);
		return;
	}
	/* Check value */
	val++;
	while (*val)
		if (!check_printable(&val))
			return;
}

static void
check_text(byte *val)
{
	while (*val) {
		/* Special tag */
		if (*val >= 0x80 && *val < 0xc0) {
			/* Attribute changer */
			if (!(*val & 0x20)) {
				if ((*val & 0x0f) >= WT_MAX) {
					log(L_ERROR, "%s: Too large word type %d", url, *val & 0x0f);
					return;
				}
			}
			/* Bracket */
			else {
				log(L_WARN, "%s: Brackets checking is not supported", url);
				if (!(*val & 0x10)) {
					if (!*++val) {
						log(L_ERROR, "%s: Truncated bracket start", url);
						return;
					}
				}
			}
			val++;
		}
		/* UTF8 character */
		else if (!check_printable(&val))
			return;
	}
}

static void
check(void)
{
	while (1) {
		mp_flush(mp);
		if (!read_object())
			break;
		check_url();
		check_version();

		for (struct oattr *a = o->attrs; a; a = a->next) {
			if (a->attr < OBJ_ATTR_SON && a->same && strchr(single_attrs, a->attr))
				log(L_ERROR, "%s: Multiple values of attribute %c", url, a->attr);
			switch (a->attr) {
			case 'U':
			case 'v':
			case 'l':
			case 'L':
				break;
			case 'X':
				check_text(a->val);
				break;
			case 'M':
				check_meta(a->val);
				break;
			default:
				if (a->attr >= OBJ_ATTR_SON) {
					if (!strchr(custom_subobjs, a->attr - OBJ_ATTR_SON))
						log(L_ERROR, "%s: Unknown subobject %c (0x%02x)", url, a->attr - OBJ_ATTR_SON, a->attr - OBJ_ATTR_SON);
				}
				else {
					if (!strchr(custom_attrs, a->attr))
						log(L_ERROR, "%s: Unknown attribute %c (0x%02x)", url, a->attr, a->attr);
				}
				break;
			}
		}
	  }

}

static char *shortopts = "a:A:h" CF_SHORT_OPTS;
static struct option longopts[] =
{
	CF_LONG_OPTS
	{ NULL, 0, 0, 0 }
};

static void NONRET
usage(void)
{
	fputs("Usage: bin/check-indexer-input [<options>] [<file>]\n\
\n\
Options:\n"
CF_USAGE
"\
-a <attrs>	List of custom attributes (default='0123456789')\n\
-A <attrs>	List of custom subobjects (default='')\n\
-h              Check duplicate fingerprints\n\
", stderr);
	exit(1);
}

int
main(int argc, char **argv)
{
	int opt;
	log_init(argv[0]);
	while ((opt = cf_getopt(argc, argv, shortopts, longopts, NULL)) >= 0)
		switch (opt) {
		case 'a':
			custom_attrs = optarg;
			break;
		case 'A':
			custom_subobjs = optarg;
			break;
		case 'h':
			check_dups++;
			break;
		default:
			usage();
		}
	if (argc > optind + 1)
		usage();

	ASSERT(MT_MAX <= 16);
	ASSERT(WT_MAX <= 8);
	ASSERT(ST_MAX <= 8);

	hurl_init();
	if (check_dups) {
		url_key_init();
		hfp_init();
	}

	if (argc == optind) {
		log(L_INFO, "Scanning standard input");
		fb = bfdopen_shared(1, 1024 * 1024);
	}
	else {
		log(L_INFO, "Scanning %s", argv[optind]);
		fb = bopen(argv[optind], O_RDONLY, 1024 * 1024);
	}
	mp = mp_new(65536);
	check();
	mp_delete(mp);
	bclose(fb);
	log(L_INFO, "Scanned %u objects", objno);

	return 0;
}
