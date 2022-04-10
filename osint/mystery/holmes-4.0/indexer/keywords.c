/*
 *	Sherlock Indexer -- Processing {URL,File,Catalog} keywords
 *
 *	(c) 2003--2006 Robert Spalek <robert@ucw.cz>
 *
 *	old code taken from centrum/indexer/oook.c by
 *
 *	(c) 2002--2003 Martin Mares <mj@ucw.cz>
 *
 *	This module attaches keywords from URL's and file names to the
 *	documents.  It filters them if they are too frequent or if an URL has
 *	too many of them.
 *
 *	XXX: beware that some regular expression libraries mistreat nested
 *	parentheses, e.g. ((text)|(another text)), so we cannot substitute to \2\3.
 */

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "ucw/clists.h"
#include "ucw/url.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/chartype.h"
#include "ucw/mempool.h"
#include "ucw/bitarray.h"
#include "ucw/unicode.h"
#include "ucw/regex.h"
#include "ucw/string.h"
#include "charset/unicat.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"
#include "indexer/lexicon.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

#define	TRACE(level, x...)	if (trace_level >= level) printf(x)

/*** Configuration ***/

struct url_pattern {
  cnode n;
  regex *rx;
  char *replacement;
  uns weight;
  uns use_hardcoded;
};

static uns trace_level = 0;
static clist url_patterns, file_patterns;
static uns name_words;
static uns limit_count[5][3][4];
	/* [type of limitation: 0=urlmax, 1=sitemax, 2=totalmax,
		3=decrease threshold, 4=delete threshold]
	   [type: 0=URL, 1=File, 2=Catalog]
	   [weight: 0..3] */

static char *
compile_regexp(uns number UNUSED, char **pars, regex **rx)
{
  cf_journal_block(rx, sizeof(void*));
  *rx = rx_compile(pars[0], 0);
  return NULL;
}

static char *
pattern_commit(struct url_pattern *ptr)
{
  if (ptr->weight > 3)
    return "Weight out of range";
  return NULL;
}

static struct cf_section pattern_config = {
  CF_TYPE(struct url_pattern),
  CF_COMMIT(pattern_commit),
#define F(x)	PTR_TO(struct url_pattern, x)
  CF_ITEMS {
    CF_PARSER("Pattern", F(rx), compile_regexp, 1),
    CF_STRING("Replace", F(replacement)),
    CF_UNS("Weight", F(weight)),
    CF_UNS("Hardcoded", F(use_hardcoded)),
    CF_END
  }
};

static char *
keywords_init(void *ptr UNUSED)
{
  memset(limit_count, -1, sizeof(limit_count));
  return NULL;
}

static struct cf_section keywords_config = {
  CF_INIT(keywords_init),
  CF_ITEMS {
    CF_UNS("Trace", &trace_level),
    CF_LIST("URLWordPattern", &url_patterns, &pattern_config),
    CF_LIST("FileWordPattern", &file_patterns, &pattern_config),
    CF_UNS("NameWords", &name_words),
    CF_UNS_ARY("URLMaxURLWords", limit_count[0][0], 4),
    CF_UNS_ARY("URLMaxFileWords", limit_count[0][1], 4),
    CF_UNS_ARY("URLMaxCatalogWords", limit_count[0][2], 4),
    CF_UNS_ARY("SiteMaxURLWords", limit_count[1][0], 4),
    CF_UNS_ARY("SiteMaxFileWords", limit_count[1][1], 4),
    CF_UNS_ARY("SiteMaxCatalogWords", limit_count[1][2], 4),
    CF_UNS_ARY("TotalMaxURLWords", limit_count[2][0], 4),
    CF_UNS_ARY("TotalMaxFileWords", limit_count[2][1], 4),
    CF_UNS_ARY("TotalMaxCatalogWords", limit_count[2][2], 4),
    CF_UNS_ARY("DecreaseURLWords", limit_count[3][0], 4),
    CF_UNS_ARY("DecreaseFileWords", limit_count[3][1], 4),
    CF_UNS_ARY("DecreaseCatalogWords", limit_count[3][2], 4),
    CF_UNS_ARY("RemoveURLWords", limit_count[4][0], 4),
    CF_UNS_ARY("RemoveFileWords", limit_count[4][1], 4),
    CF_UNS_ARY("RemoveCatalogWords", limit_count[4][2], 4),
    CF_END
  }
};

/*** Extraction of keywords from URL and filename ***/

#define LINEBUF_SIZE 1024

static struct fastbuf *keywords;
static uns uk_cnt, nk_cnt;

static int
hardcoded_parse_url(uns type, char *url, char *buf, uns buf_len UNUSED)
{
  char url_buf[MAX_URL_SIZE];
  struct url urlp;
  if (url_split(url, &urlp, url_buf))
    return -1;
  if (type == 'U')
  {
#if 0
    URLWordPattern	http://(www[^.]*\.|)([a-z0-9]+)\.[a-z]+/(|(index|default)\.[a-zA-Z0-9]+)	\2	3
    URLWordPattern	http://(www[^.]*\.|)([^.]+)\.[a-z]+/(|(index|default)\.[a-zA-Z0-9]+)	\2	2
    URLWordPattern	http://(www[^.]*\.|)([^.]+)\.[^/]*/(|(index|default)\.[a-zA-Z0-9]+)	\2	1
#endif
    if (urlp.protoid != URL_PROTO_HTTP
	|| (urlp.port != ~0U && urlp.port != 80)
	|| urlp.user || urlp.pass)
      return -1;
    char *d;
    if ((d = strchr(urlp.rest, '.')))
    {
      uns l = d - urlp.rest;
      if ((l != 6 || strncasecmp(urlp.rest, "/index", 7))
      && (l != 8 || strncasecmp(urlp.rest, "/default", 8)))
	return -1;
      if (!*++d)
	return -1;
      for (; *d; d++)
	if (!Calnum(*d))
	  return -1;
    }
    else
    {
      if (urlp.rest[0] != '/' || urlp.rest[1])
	return -1;
    }
    char *w[3];
    int pars = str_sepsplit(urlp.host, '.', w, 3);
    if (!pars || pars == 1)
      return -1;
    if (pars == 2 || (pars == 3 && !strncasecmp(w[0], "www", 3)))
    {
      uns err = 0;
      for (d=w[pars-1]; *d; d++)
	if (!Calpha(*d))
	{
	  err=1;
	  break;
	}
      if (!err)
      {
	err = '3';
	for (d=w[pars-2]; *d; d++)
	  if (!Calnum(*d))
	  {
	    err = '2';
	    break;
	  }
	strcpy(buf, w[pars-2]);
	return err;
      }
    }
    if (pars == -1)
      pars = 3;
    if (pars == 3 && !strncasecmp(w[0], "www", 3))
      d = w[1];
    else
      d = w[0];
    strcpy(buf, d);
    return '1';
  }
  else if (type == 'F')
  {
#if 0
    FileWordPattern	....://[^?]*/(([^/.?=;]+)|([^/?=;]+)\.[^/.?=;]*)/?$	\2\3	2
    FileWordPattern	....://[^?]*/(([^/.?=;]+)|([^/?=;]+)\.[^/.?=;]*)/?\?.*	\2\3	1
#endif
    char *p = strchr(urlp.rest, '?');
    uns level;
    if (p)
    {
      level = '1';
      *p = 0;
    }
    else
    {
      level = '2';
      p = urlp.rest + strlen(urlp.rest);
    }
    if (p > urlp.rest + 1 && p[-1] == '/')
      *--p = 0;
    char *s = strrchr(urlp.rest, '/');
    ASSERT(s);
    s++;
    char *d = strchr(s, '.') ? : p;
    if (d == s || (d != p && strchr(d+1, '.')))
      return -1;
    for (p=s; *p; p++)
      if (*p == '=' || *p == ';')
	return -1;
    memcpy(buf, s, d-s);
    buf[d-s] = 0;
    return level;
  }
  else
    ASSERT(0);
  return -1;
}

static int
extract_url_keywords(byte *url, byte *buf, uns source, uns target, byte type)
{
  struct url_pattern *p;

  byte *orig_buf = buf;
  buf += sprintf(buf, "%c%x %x 0 ", type, source, target);
  clist *list = type == 'U' ? &url_patterns : &file_patterns;
  CLIST_WALK(p, *list)
  {
    int wt;
    if (p->use_hardcoded)
      wt = hardcoded_parse_url(type, url, buf, LINEBUF_SIZE - (buf-orig_buf));
    else
    {
      if (rx_subst(p->rx, p->replacement, url, buf, LINEBUF_SIZE - (buf-orig_buf)) == 1)
	wt = p->weight;
      else
	wt = -1;
    }
    if (wt >= 0)
    {
      buf[-2] = wt;
      if (type == 'U')
	uk_cnt++;
      else
	nk_cnt++;
      return 1;
    }
  }
  return 0;
}

static void
generate_url_keywords(bitarray_t is_image)
{
  struct fastbuf *ul;
  byte url[MAX_URL_SIZE], url2[MAX_URL_SIZE], buf[LINEBUF_SIZE];
  uns i=0;

  ul = index_bopen(fn_urls, O_RDONLY, 1);
  while (bgets(ul, url, sizeof(url)))
    {
      if (url_deescape(url, url2))
      {
        i++;
        continue;
      }
      uns v = merges_follow(i);		// just resolves redirects
      if (extract_url_keywords(url2, buf, i, v, 'U'))
	{
	  bputs(keywords, buf);
	  bputc(keywords, '\n');
	}
      if ((name_words >= 2 ||
	   name_words && bit_array_isset(is_image, i)) &&
	  extract_url_keywords(url2, buf, i, v, 'F'))
	{
	  bputs(keywords, buf);
	  bputc(keywords, '\n');
	}
      i++;
    }
  bclose(ul);
  log(L_INFO, "Generated %d URL keywords and %d File keywords", uk_cnt, nk_cnt);
}

/*** Construction of lexhash and conversion to a binary format ***/

static struct fastbuf *binary;
static uns written;

typedef struct {
  u32 source, source_site, target, target_site;
  byte type, weight;
  byte source_pagerank;
  uns word;
  u16 order;
} keyword_t;

keyword_t kw;

#define	LH_MKLEX
#include "indexer/lexhash.h"

static enum word_class
lm_lookup(enum word_class orig_class, u16 *uni, uns ulen, word_id_t *idp, void *user UNUSED)
{
  struct verbum *v;
  enum word_class class = orig_class;

  if (orig_class == WC_NORMAL)
    {
      v = lh_lookup(uni, ulen);
      *idp = v;
      class = v->id & 7;
    }
  return class;
}

static void
lm_got_word(uns pos, uns cat, word_id_t w, void *user UNUSED)
{
  keyword_t rec = kw;
  rec.word = w->id >> 3;
  rec.order = pos;
  bwrite(binary, &rec, sizeof(rec));
  written++;
  TRACE(2, "\t-> @%d T%d C%d I%x <%s>\n", pos, w->id & 7, cat, w->id >> 3, w->word);
  w->u.count++;
}

static uns cplx_pos = -1;
static void
lm_got_complex(uns pos, uns cat, word_id_t wroot, word_id_t wctxt, uns after, void *user UNUSED)
{
  TRACE(2, "\t-> @%d T%d <%x,%x,%d>\n", pos, cat, wroot->id >> 3, wctxt->id >> 3, after);
  if (pos != cplx_pos)		// avoid duplicate entries when a context word is registered for both sides
  {
    lm_got_word(pos, cat, wroot, user);
    cplx_pos = pos;
  }
  //lm_got_word(pos, cat, wctxt, user);
}

#define	LM_SIMPLE
#include "indexer/lexmap.h"

static byte **lh_words;		// for friendly dumping
static uns lh_words_size;

static void
lh_dump(void)
{
  lh_words = big_alloc_zero(lh_words_size = ((lh_id/8 + 1) * sizeof(byte *)));
  TRACE(2, "\nLexical hash table:\n\n");
  LH_WALK(v)
  {
    lh_words[v->id >> 3] = v->word;
    TRACE(2, "%08x <%s> C%d R%d\n", v->id >> 3, v->word, v->id & 7, v->u.count);
  }
}

/*** Sorter ***/

#define	SORT_PREFIX(x)		keyword_##x
#define	SORT_KEY_REGULAR	keyword_t
#define	SORT_INPUT_FB
#define	SORT_OUTPUT_FB
#define	CMP(attr)		COMPARE(a->attr, b->attr)
#define	REVCMP(attr)		REV_COMPARE(a->attr, b->attr)
#define	CMP_EQ1st(attr,cmpattr) \
  if ((a->attr == a->cmpattr) ^ (b->attr == b->cmpattr)) \
    return (a->attr == a->cmpattr) ? -1 : 1; \
  CMP(attr)
  /* Used with (source,target) or (source_site,target_site).
     It puts original page before redirects and pages from my site before other pages.

     Caveat!  This only works if indexer/merges.c does NOT swap source and target vertices
     when merging trees.  */

static uns sort_phase;

static inline int
keyword_compare(keyword_t *a, keyword_t *b)
{
  if (sort_phase == 1 || sort_phase == 2)	// prune cheaters with many redirects
  {
    CMP(target);
    CMP(type);
    CMP(weight);
    if (sort_phase == 1)			//1st pass: site and URL limits
      CMP(source_site);				//2nd pass: total limit
    if ((a->source == a->target) ^ (b->source == b->target))
      return (a->source == a->target) ? -1 : 1;
    REVCMP(source_pagerank);
    CMP(source);
    CMP(order);
    if (trace_level >= 1)
      log(L_WARN_R, "Duplicate record %x of type %c and weight %c", a->source, a->type, a->weight);
    return 0;
  }
  else if (sort_phase == 3)			// prune common words
  {
    CMP(word);
    CMP(type);
    CMP(weight);
    return 0;
  }
  else if (sort_phase == 4)		// give the data a nice order
  {
    CMP(target);
    CMP_EQ1st(source_site, target_site);
    CMP_EQ1st(source, target);
    CMP(type);
    CMP(order);
    if (trace_level >= 1)
      log(L_WARN_R, "Duplicate record %x of type %c", a->source, a->type);
    return 0;
  }
  else
    ASSERT(0);
  return 0;
}

#include "ucw/sorter/sorter.h"

static void
dump_binary_stream(struct fastbuf *binary)
{
  keyword_t rec;
  while (bread(binary, &rec, sizeof(keyword_t)))
  {
    printf("Bucket %x (siteid %08x) merged to %x, %dth word #%x <%s> of type %c and weight %c\n",
      rec.source, rec.source_site, rec.target, rec.order, rec.word, lh_words[rec.word], rec.type, rec.weight);
  }
}

static void
resort(struct fastbuf **F, uns phase)
{
  sort_phase = phase;
  struct fastbuf *f = *F;
  brewind(f);
  f = keyword_sort(f, NULL);
  brewind(f);
  if (trace_level >= 2)
  {
    dump_binary_stream(f);
    brewind(f);
  }
  *F = f;
}

static uns
type_to_index(byte type)
{
  switch (type)
  {
    case 'U': return 0;
    case 'F': return 1;
#ifdef MT_CAT_KW_USER
    case 'C': return 2;
#endif
    default: ASSERT(0); return 0;
  }
}

static byte *reasons[3] = { "url", "site", "total" };
static byte *actions[2] = { "Decreased", "Removed" };
static byte *types[3] = { "url", "file", "catalog" };
static uns deleted[3][3][4];

static void
filter_cheaters(struct fastbuf **F, uns consider_total_limit)
{
  struct fastbuf *in = *F;
  struct fastbuf *out = index_bopen_tmp(1);
  keyword_t last = { .word = ~0U, .target = 0, .source_site = 0, .source = 0 }, this;
  uns url_cnt = 0, site_cnt = 0, total_cnt = 0;
  bzero(deleted, sizeof(deleted));
  while (bread(in, &this, sizeof(keyword_t)))
  {
    if (last.word == ~0U || last.target != this.target
    || last.type != this.type || last.weight != this.weight)
      total_cnt = 0;
    if (!total_cnt || last.source_site != this.source_site)
      site_cnt = 0;
    if (!site_cnt || last.source != this.source)
      url_cnt = 0;
    uns idx = type_to_index(this.type), wt = this.weight - '0';
    int reject = -1;
    uns curr=0, limit=0;
    if (url_cnt >= limit_count[0][idx][wt])
      reject=0, curr=url_cnt, limit=limit_count[0][idx][wt];
    else if (site_cnt >= limit_count[1][idx][wt])
      reject=1, curr=site_cnt, limit=limit_count[1][idx][wt];
    else if (total_cnt >= limit_count[2][idx][wt] && consider_total_limit)
      reject=2, curr=total_cnt, limit=limit_count[2][idx][wt];
    if (reject < 0)
    {
      bwrite(out, &this, sizeof(keyword_t));
      total_cnt++;
      site_cnt++;
      url_cnt++;
    }
    else
    {
      TRACE(1, "Cheater caught for type %c, weight %c: merged %8x, siteid %08x, url %8x exceeded %s limit %3d>%d, %dth word <%s>\n",
        this.type, this.weight, this.target, this.source_site, this.source, reasons[reject], curr+1, limit, this.order+1, lh_words[this.word]);
      url_cnt++;
      if (reject >= 1)
        site_cnt++;
      if (reject >= 2)
        total_cnt++;
      deleted[reject][idx][wt]++;
    }
    last = this;
  }
  bclose(in);
  bflush(out);
  *F = out;

  for (uns reason=0; reason<3; reason++)
  {
    byte buf[200];
    uns len = 0, count = 0;
    len = sprintf(buf, "Cut keywords over %s limit: ", reasons[reason]);
    for (uns idx=0; idx<3; idx++)
    {
      len += sprintf(buf+len, "%s:", types[idx]);
      for(uns wt=0; wt<4; wt++)
      {
        len += sprintf(buf+len, " %d", deleted[reason][idx][wt]);
	count += deleted[reason][idx][wt];
      }
      len += sprintf(buf+len, ", ");
    }
    if (count)
      log(L_DEBUG, "%s", buf);
  }
}

static void
flush_words(struct fastbuf *in, ucw_off_t start_pos, uns count, struct fastbuf *out, keyword_t *last)
{
  if (!count)
    return;
  uns idx = type_to_index(last->type), wt = last->weight - '0';
  uns decrease = 0, remove = 0;
  uns limit;
  if (count > (limit=limit_count[4][idx][wt]))
  {
    deleted[1][idx][wt] += 1; //count;
    TRACE(1, "Removed word of type %c and weight %c with frequency %6d>%d <%s>\n",
      last->type, last->weight, count, limit, lh_words[last->word]);
    remove = 1;
  }
  else if (count > (limit=limit_count[3][idx][wt]))
  {
    deleted[0][idx][wt] += 1; //count;
    TRACE(1, "Decreased word of type %c and weight %c with frequency %6d>%d <%s>\n",
      last->type, last->weight, count, limit, lh_words[last->word]);
    if (wt > 0)
      decrease = 1;
    else
      remove = 1;
  }
  bseek(in, start_pos, SEEK_SET);
  keyword_t tmp;
  for (uns i=0; i<count; i++)
  {
    bread(in, &tmp, sizeof(keyword_t));
    if (decrease)
      tmp.weight--;
    else if (remove)
      tmp.weight = '0';
    bwrite(out, &tmp, sizeof(keyword_t));
  }
  bread(in, &tmp, sizeof(keyword_t));	// the new one is already read if !EOF
}

static void
filter_common_words(struct fastbuf **F)
{
  struct fastbuf *in = *F;
  struct fastbuf *out = index_bopen_tmp(1);
  keyword_t last, this;
  uns cnt = 0;
  ucw_off_t seek = 0;
  bzero(deleted, sizeof(deleted));
  last.word = ~0U;
  while (bread(in, &this, sizeof(keyword_t)))
  {
    if (last.word == ~0U || last.word != this.word
    || last.type != this.type || last.weight != this.weight)
    {
      flush_words(in, seek, cnt, out, &last);
      cnt = 1;
      seek = btell(in) - sizeof(keyword_t);
    }
    else
      cnt++;
    last = this;
  }
  flush_words(in, seek, cnt, out, &last);
  bclose(in);
  bflush(out);
  *F = out;

  for (uns reason=0; reason<2; reason++)
  {
    byte buf[200];
    uns len = 0, count = 0;
    len = sprintf(buf, "%s keywords over limit: ", actions[reason]);
    for (uns idx=0; idx<3; idx++)
    {
      len += sprintf(buf+len, "%s:", types[idx]);
      for(uns wt=0; wt<4; wt++)
      {
        len += sprintf(buf+len, " %d", deleted[reason][idx][wt]);
	count += deleted[reason][idx][wt];
      }
      len += sprintf(buf+len, ", ");
    }
    if (count)
      log(L_DEBUG, "%s", buf);
  }
}

/*** Dumping the final labels ***/

static void
dump_keywords(struct fastbuf *labels, byte *buf, keyword_t *rec, byte min_wt)
{
  if (rec->source == (u32)~0U)	//TODO: dump nothing if all words have weight 0
    return;
  static byte types[3] = { MT_URL_KEYWD, MT_FILE,
#ifdef MT_CAT_KW_USER
    MT_CAT_KW_USER
#endif
  };
  uns idx = type_to_index(rec->type);
  byte start[3], *s = start;
  if (rec->type == 'U' || rec->type == 'F')
    *s++ = min_wt;		//FIXME: weight changes inside the attribute instead of min_wt
  *s++ = 0x90 + types[idx];
  *s = 0;

  bputl(labels, rec->source);
  bputc(labels, LABEL_TYPE_URL);
  bput_attr_format(labels, 'M', "%s%s", start, buf);
  bput_attr_separator(labels);
}

static void
dump_into_labels(struct fastbuf *labels, struct fastbuf *binary)
{
  byte buf[LINEBUF_SIZE] = " ";
  uns records = 0, words = 0;
  uns len = 0;
  byte min_wt = '9';
  keyword_t rec, last;
  last.source = ~0U;
  last.type = 0;
  while (bread(binary, &rec, sizeof(keyword_t)))
  {
    if (rec.source != last.source || rec.type != last.type)
    {
      dump_keywords(labels, buf+1, &last, min_wt);
      records++;
      len = 0;
      min_wt = '9';
    }
    else
    {
      if (rec.order > last.order + 1)
      {
        /* This is perfectly OK due to lexmapper.  */
	//TODO: insert something like "a"
        TRACE(1, "URL %x and type %c has missing words %d..%d between <%s> and <%s>\n",
	  rec.source, rec.type, last.order+1, rec.order-1, lh_words[last.word], lh_words[rec.word]);
      }
      else if (rec.order <= last.order)
      {
        ASSERT(rec.order == last.order);
	/* This can happen for duplicates from catalogue.  */
      }
    }
    len += sprintf(buf + len, " %s", lh_words[rec.word]);
    min_wt = MIN(min_wt, rec.weight);
    words++;
    ASSERT(len <= LINEBUF_SIZE);	// should not exceed
    last = rec;
  }
  dump_keywords(labels, buf+1, &last, min_wt);
  log(L_INFO, "Dumped into %d words in %d records into labels", words, records);
}

/*** Main program ***/

static char *short_opts = CF_SHORT_OPTS "udtT";
static char *help = "\
Usage: keywords [<options>]\n\
\n\
Options:\n"
CF_USAGE
"-u\tParse URL's to obtain keywords\n\
-d\tDump pruned keywords into labels\n\
-t\tTruncate current keywords (export from oook will be deleted)\n\
-T\tTruncate labels (dangerous!)\n\
";

static void NONRET
usage(void)
{
  fputs(help, stderr);
  exit(1);
}

int
main(int argc, char **argv)
{
  uns phases = 0, truncate = 0;
  int opt;

  log_init(argv[0]);
  cf_declare_section("Keywords", &keywords_config, 0);
  while ((opt = cf_getopt(argc, argv, short_opts, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
    {
      case 'u':
        phases |= 1;
	break;
      case 'd':
        phases |= 2;
	break;
      case 't':
	truncate |= 1;
	break;
      case 'T':
	truncate |= 2;
	break;
      default:
	usage();
    }
  if (!phases)		// default for indexer.sh
    phases = 3;
  if (optind < argc)
    usage();

  attrs_part_map(0);
  notes_part_map(0);

  if (phases & 1)
  {
    log(L_INFO, "Going to parse URLs into URL and File keywords");
    keywords = index_bopen(fn_keywords, O_RDWR | O_CREAT | (truncate&1 ? O_TRUNC : O_APPEND), 0);

    bitarray_t is_image = big_alloc_zero(BIT_ARRAY_BYTES(card_count));
    for (uns i=0; i<card_count; i++)
      if (bring_note(i)->flags & (CARD_NOTE_IMAGE | CARD_NOTE_AUDIO))
	bit_array_set(is_image, i);

    merges_map(0);
    generate_url_keywords(is_image);
    merges_unmap();

    big_free(is_image, BIT_ARRAY_BYTES(card_count));
    bflush(keywords);
  }
  else
  {
    keywords = index_bopen(fn_keywords, O_RDONLY, 1);
  }

  if (phases & 2)
  {
    log(L_INFO, "Going to translate keywords into binary form");
    byte *card_weights;
    READ_ATTR(card_weights, weight);
#ifdef CONFIG_SITES
    u32 *card_sites;
    READ_ATTR(card_sites, site_id);
#endif
    binary = index_bopen_tmp(1);
    lm_init();
    lh_init();
    byte buf[LINEBUF_SIZE];
    bsetpos(keywords, 0);
    while (bgets(keywords, buf, LINEBUF_SIZE))
    {
      int cnt, len;
      cnt = sscanf(buf, "%c%x %x %c %n", &kw.type, &kw.source, &kw.target, &kw.weight, &len);
      if (cnt != 4)
        die("Invalid format: %s", buf);
#ifdef	CONFIG_SITES
      kw.source_site = card_sites[kw.source];
      kw.target_site = card_sites[kw.target];
#else
      kw.source_site = 0;
      kw.target_site = 0;
#endif
      kw.source_pagerank = card_weights[kw.source];
      lm_doc_start(NULL);
      cplx_pos = -1;
      lm_map_text(buf + len);
    }
    lh_dump();
#ifdef CONFIG_SITES
    FREE_ATTR(card_sites);
#endif
    FREE_ATTR(card_weights);
    bflush(binary);
    log(L_INFO, "Written %d words", written);

    resort(&binary, 1);
    filter_cheaters(&binary, 0);
    resort(&binary, 2);
    filter_cheaters(&binary, 1);
    resort(&binary, 3);

    /* Make the fastbuf seekable */
    struct fastbuf *stream = binary;
    binary = bopen_file(stream->name, O_RDONLY, &indexer_fb_params); 
    filter_common_words(&binary);
    bclose(stream);

    /* Dump the keywords into labels.  */
    resort(&binary, 4);
    struct fastbuf *labels = index_bopen(fn_labels_by_id, O_WRONLY | O_CREAT | (truncate&2 ? O_TRUNC : O_APPEND), 0);
    put_attr_set_type(BUCKET_TYPE_V33);
    dump_into_labels(labels, binary);
    bclose(labels);
    bclose(binary);

    big_free(lh_words, lh_words_size);
  }
  bclose(keywords);
  notes_part_unmap();
  attrs_part_unmap();

  return 0;
}

/*
TODO:
- catalog keyword should be numbered by unique numbers to distinguish duplicates, otherwise we are unable to get the phrases
  correct after a sorting phase.  we could use URL id, because this is always equal with merged vertex for catalog entries.
  then we can either delete duplicate entries or preserve all of them.

  this is not prior, because current behaviour is to merge everything (and just produce a lot of warnings), which does not
  matter for keywords, which do not usually form a continuous text, hence phrases do not matter

- consider one more phase: pruning of many EQUAL keywords for a document.  it might obtain a lot of them from redirects and
  if we are pruning cheaters, this might do it better, we would preserve at least non-equal words

  or maybe better: compute a 32-bit hash for each sequence of keywords from one
  source (to allow also phrases).  then sort these sequences on this hash and
  descending on the weight of the source object.  delete all duplicates.  do
  this even before the site and total deletion.  should solve e.g.
  http://www.domain.cz/ and http://www.domain.cz/index.html producing two
  instances of the same keyword

  something like this would be also nice for ref-texts
*/
