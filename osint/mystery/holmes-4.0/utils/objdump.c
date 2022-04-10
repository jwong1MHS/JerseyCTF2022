/*
 *	Sherlock Gatherer -- ObjDump
 *
 *	(c) 2001--2004 Robert Spalek <robert@ucw.cz>
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/bucket.h"
#include "sherlock/object.h"
#include "sherlock/index.h"
#include "sherlock/tagged-text.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "charset/charconv.h"
#include "charset/fb-charconv.h"
#include "lang/detect.h"
#include "utils/dumpconfig.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static struct fastbuf *output;
static uns is_recoding;
static int raw_dump;
static int do_lang_detect;
static int verbose;
static uns bucket_type = BUCKET_TYPE_PLAIN;
static struct lang_detect *lang_detect;

/* Maximal input line length.  */
#define	BUFSIZE	1024

static char *options = CF_SHORT_OPTS "rlvw:";

static char *help = "\
Usage: objdump <options>\n\
\n\
Options:\n"
CF_USAGE
"-r\t\tRaw dump (don't convert character sets)\n\
-l\t\tLanguage detection\n\
-v\t\tBe verbose\n\
-w <type>\tRaw input of bucket of a given type\n\
";

static void NONRET
usage(void)
{
	fputs(help, stderr);
	exit(1);
}

static byte outtext[BUFSIZE];			/* formatted output text */
static byte *outend = outtext;
static uns out_space;
static uns this_attr;

static void
flush_outtext(void)
{
	if (outend == outtext)
		return;
	bprintf(output, "%c%s\n", this_attr, outtext);	// performs the recoding
	outtext[0] = 0;
	outend = outtext;
}

static void
add_word(byte *b, byte *e)
{
	if (outend-outtext + 1 + e-b >= (int)(line_len - 1))
		flush_outtext();
	if (out_space && outend != outtext)
		*outend++ = ' ';
	strncpy(outend, b, e-b);
	outend[e-b] = 0;
	outend += e-b;
	out_space = 0;
}

static char *wt_names[16] = { WORD_TYPE_USER_NAMES };
static char *mt_names[16] = { META_TYPE_USER_NAMES };

static void
objdump_line(uns type, byte *line)
{
	if (!type)
		return;
	if (type != 'X')
		flush_outtext();
	if (type == 'Z')
		bprintf(output, "Z%s\n", line);
	else if (type == 'N' || type == 'H')
	{
		if (is_recoding)
			bprintf(output, "%c omitted due to charset recoding\n", type);
		else
			bprintf(output, "%c%s\n", type, line);
	}
	else if (type == 'X' || type == 'M')
	{
		int is_meta = (type == 'M');
		char **type_names = is_meta ? mt_names : wt_names;
		byte *source = line, *end = line + strlen(line);
#if defined(CONFIG_LANG) && defined(CONFIG_LANG_DETECT)
		if (type == 'X' && do_lang_detect)
			lang_detect_add_string(lang_detect, source);
#endif
		out_space = 1;
		this_attr = type;
		while (source < end)
		{
			byte *last_char, *curr = source;
			uns tag;
			do
			{
				last_char = curr;
				GET_TAGGED_CHAR(curr, tag);
			}
			while (tag && tag != ' ' && tag < 0x80000000);
			if (last_char > source)
				add_word(source, last_char);
			source = curr;
			if (tag < 0x80000000)	/* space, end of string */
			{
				out_space = 1;
				continue;
			}
			byte buffer[64];
			int len;
			if (tag < 0x80010000)
			{
				if (tag & 0x10)
				{
					out_space = 1;
					len = sprintf(buffer, "<break>");
					if (type == 'X')
						add_word(buffer, buffer+len);
				}
				out_space = 1;
				len = sprintf(buffer, "<%s>", type_names[tag & 0x0f] ? : "???");
				add_word(buffer, buffer+len);
				out_space = 1;
			}
			else if (tag < 0x80020000)
			{
				len = sprintf(buffer, "<ref %d>", tag & 0x03ff);
				add_word(buffer, buffer+len);
			}
			else
			{
				len = sprintf(buffer, "</ref>");
				add_word(buffer, buffer+len);
			}
		}
		if (is_meta)
			flush_outtext();
	}
	else
		bprintf(output, "%c%s\n", type, line);
}

static void
objdump_done(void)
{
	flush_outtext();
	bprintf(output, "\n");
#if defined(CONFIG_LANG) && defined(CONFIG_LANG_DETECT)
	if (do_lang_detect)
	{
		struct lang_detect_results *res = lang_detect_compute(lang_detect);
		if (res->lang1 < 0)
			bprintf(output, "The language cannot be detected\n\n");
		else
		{
			struct lang_detect_lang_flag *l1, *l2;
			l1 = lang_detect_lang_flags + res->lang1;
			l2 = lang_detect_lang_flags + res->lang2;
			bprintf(output, "Language detection:\n");
			bprintf(output, "1st match: #%d (%s%s, %d), variance %d\n",
				res->lang1, lang_code_to_name(l1->id), l1->is_accented ? "" : "-CA",
				l1->id, res->variances[res->lang1]);
			bprintf(output, "2nd match: #%d (%s%s, %d), variance %d\n",
				res->lang2, lang_code_to_name(l2->id), l2->is_accented ? "" : "-CA",
				l2->id, res->variances[res->lang2]);
			bprintf(output, "Ratio of variances %d, minimal ratio %d\n", res->ratio, res->min_ratio);
			bprintf(output, "Found %d occurences of %d sequences\n\n", res->total_occur, res->nonzero_seq);
			if (verbose > 0)
			{
				bprintf(output, "Variances of languages:\n");
				for (uns i=0; i<lang_detect_nr_langs; i++)
				{
					struct lang_detect_lang_flag *l = lang_detect_lang_flags + i;
					bprintf(output, "#%d (%s%s, %d), variance %d (ratio %d)\n",
						i, lang_code_to_name(l->id), l->is_accented ? "" : "-CA",
						l->id, res->variances[i],
						1000 * res->variances[i] / res->variances[res->lang1]);
				}
				bprintf(output, "\n");
			}
			if (verbose > 1)
			{
				bprintf(output, "Occurences of sequences:\n");
				for (uns i=0; i<res->nonzero_seq; i++)
				{
					if (verbose == 2 && i >= lang_detect_lang_flags[ res->lang1 ].nr_seq)
					{
						bprintf(output, "Cut here\n");
						break;
					}
					struct lang_detect_sequence *seq = lang_detect_sequences[ res->sf[i].id ];
					bprintf(output, "%d. %s, %d occurences, expected at [%d, %d]\n",
						i+1, seq->text, res->sf[i].occur,
						seq->order[res->lang1], seq->order[res->lang2]);
				}
				bprintf(output, "\n");
			}
		}
		lang_detect_start(lang_detect);
	}
#endif
}

static void
dump_stdin(void)
{
	struct fastbuf *b;
	byte buf[BUFSIZE];
	b = bfdopen_shared(0, 4096);
	bgets(b, buf, BUFSIZE);
	if (strncmp(buf, "###", 3))		/* a single object */
	{
		do
		{
			objdump_line(buf[0], buf+1);
		}
		while (bgets(b, buf, BUFSIZE));
		objdump_done();
	}
	else					/* multiple objects divided by ### lines */
	{
		do
		{
			oid_t oid;
			uns buck_len, buck_type;
			sscanf(buf+4, "%x %d %x", &oid, &buck_len, &buck_type);
			bprintf(output, "### %08x %06d %08x\n", oid, buck_len, buck_type);
			/* Note: O means bucket number or card-id, but search server
			puts OID here.  We cannot conform to it, since we have no access
			to such a attribute here.  */
			while(1)
			{
				if (!bgets(b, buf, BUFSIZE))
				{
					buf[0] = 0;
					break;
				}
				if (!strncmp(buf, "###", 3))
					break;
				objdump_line(buf[0], buf+1);
			}
			objdump_done();
		}
		while (buf[0]);
	}
	bclose(b);
}

static void
objdump_odes(struct odes *d)
{
	for(struct oattr *a=d->attrs; a; a=a->next)
		for(struct oattr *b=a; b; b=b->same)
			objdump_line(a->attr, b->val);
	objdump_done();
}

static void
read_and_dump(uns type)
{
	struct buck2obj_buf *b2ob = buck2obj_alloc();
	struct mempool *pool = mp_new(4096);
	struct fastbuf *r, *w;

	w = fbmem_create(4096);
	r = bfdopen(0, 4096);
	bbcopy(r, w, ~0U);
	uns len = btell(r);
	bclose(r);
	r = fbmem_clone_read(w);

	struct odes *oh = obj_new(pool);
	struct odes *ob = obj_new(pool);
	if (buck2obj_parse(b2ob, type, len, r, oh, NULL, ob, 1) < 0)
		bprintf(output, ".Parse error: %m\n");
	else {
		put_attr_set_type(BUCKET_TYPE_PLAIN);
		objdump_odes(oh);
		objdump_odes(ob);
	}

	mp_delete(pool);
	buck2obj_free(b2ob);
	bclose(r);
	bclose(w);
}

int
main(int argc, char **argv)
{
	int opt;
	struct fastbuf *output_stdout;

	log_init(argv[0]);
	while ((opt = cf_getopt(argc, argv, options, CF_NO_LONG_OPTS, NULL)) >= 0)
		switch (opt)
		{
			case 'r':
				raw_dump = 1;
				break;
			case 'l':
				do_lang_detect++;
				break;
			case 'v':
				verbose++;
				break;
			case 'w':
				bucket_type = strtoul(optarg, NULL, 16);
				if (bucket_type < 10)
					bucket_type += BUCKET_TYPE_PLAIN;
				break;
			default:
				usage();
		}
	if (optind < argc)
		usage();
	output_stdout = output = bfdopen(1, 4096);
	if (!raw_dump)
	{
		int term_charset_id = find_charset_by_name(terminal_charset);
		if (term_charset_id < 0)
			die("Unknown terminal charset %s", terminal_charset);
		is_recoding = (term_charset_id != CONV_CHARSET_UTF8);
		output = fb_wrap_charconv_out(output, CONV_CHARSET_UTF8, term_charset_id);
	}
#if defined(CONFIG_LANG) && defined(CONFIG_LANG_DETECT)
	if (do_lang_detect)
	{
		lang_detect_load_tables();
		lang_detect = lang_detect_alloc(NULL);
		lang_detect_start(lang_detect);
	}
#endif
	if (bucket_type == BUCKET_TYPE_PLAIN)
		dump_stdin();
	else
		read_and_dump(bucket_type);
	if(output != output_stdout)
    		bclose(output);
	bclose(output_stdout);
	return 0;
}
