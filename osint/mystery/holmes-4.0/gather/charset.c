/*
 *	Sherlock Gatherer -- Character Set Guessing
 *
 *	(c) 1997--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2004 Robert Spalek <robert@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/clists.h"
#include "ucw/fastbuf.h"
#include "ucw/chartype.h"
#include "ucw/unicode.h"
#include "ucw/ff-unicode.h"
#include "ucw/string.h"
#include "ucw/stkstring.h"
#include "lang/lang.h"
#include "gather/gather.h"
#include "charset/charconv.h"
#include "sherlock/conf.h"

#include <stdio.h>
#include <string.h>

/* Configuration */

static uns trace_charsets;
static uns log_charset_errors;
static int believe_server_charset;	/* 1=always, -1=never, 0=check */
static int believe_meta_charset;	/* 1=always, -1=never, 0=check */
static int fallback_charset = CONV_CHARSET_ASCII;
static int improbable_penalty = 5;
static int forbidden_penalty = 50;
static int utf8_penalty = 20;
static int believe_min_grade[2] = { 700, 0};

#define TRACE(x,y...) do { if (trace_charsets) log(L_DEBUG, x,##y); } while (0)
#define XTRACE(x,y...) do { if (trace_charsets > 1) log(L_DEBUG, x,##y); } while (0)

#define MAX_CHARSETS (CONV_NUM_CHARSETS+1)
COMPILE_ASSERT(num_charset_check, MAX_CHARSETS <= 32);

static u32 *typical_chars, *forbidden_chars, *improbable_chars;

#define MAX_LFAMS 32			/* Language families and everything around them */
#define DEFAULT_LFAM 31

struct lang_family {
  cnode n;
  uns id;
  char *name;
  uns lang_set;
  int *auto_sets;			/* DARY of auto-detected charsets */
  struct unirange *chars_typical, *chars_forbidden, *chars_improbable;
};

struct def_lang {
  cnode n;
  char *host_patt, *lang_list;
};

struct charset_names {
  cnode n;
  int charset_id;			/* As known to the conversion library */
  char **aliases;			/* DARY of charset aliases */
};

static clist def_lang_list, lfam_list, charset_list;
static struct lang_family *lfam_by_id[MAX_LFAMS];
static uns num_lfams;
static char **charset_blacklist;

#define CHN_UNKNOWN		-1
#define CHN_BLACKLISTED		-2

struct charset_name {
  int id;
  byte name[1];
};

#define HASH_NODE		struct charset_name
#define HASH_PREFIX(x)		chn_##x
#define HASH_KEY_ENDSTRING	name
#define HASH_WANT_FIND
#define HASH_WANT_NEW
#define HASH_NOCASE
#define HASH_USE_POOL		cf_pool
#define HASH_ALLOC_TABLE
#include "ucw/hashtable.h"

static int
lookup_charset(byte *name)
{
  struct charset_name *n = chn_find(name);
  return n ? n->id : -1;
}

static byte *
cs_cf_commit_aliases(void)
{
  CF_JOURNAL_VAR(chn_table);
  chn_init();
  CLIST_FOR_EACH(struct charset_names *, n, charset_list)
    {
      char *cname = charset_name(n->charset_id);
      if (chn_find(cname))
	return cf_printf("Charset name or alias `%s' re-defined as name", cname);
      chn_new(cname)->id = n->charset_id;
      for (uns i=0; i<DARY_LEN(n->aliases); i++)
	{
	  byte *alias = n->aliases[i];
	  if (chn_find(alias))
	    return cf_printf("Charset name or alias `%s' re-defined as alias", alias);
	  chn_new(alias)->id = n->charset_id;
	}
    }
  for (uns i=0; i<DARY_LEN(charset_blacklist); i++)
    {
      byte *cname = charset_blacklist[i];
      if (chn_find(cname))
	return cf_printf("Charset name, alias or blacklist entry `%s' re-defined as blacklist entry", cname);
      chn_new(cname)->id = CHN_BLACKLISTED;
    }
  return NULL;
}

static void
mark_chars(u32 *char_array, uns fam_id, struct unirange *ranges)
{
  u32 lfmask = (fam_id == DEFAULT_LFAM ? ~0U : (1U << fam_id));
  for (uns i=0; i<DARY_LEN(ranges); i++)
    for (uns code=ranges[i].min; code<=ranges[i].max; code++)
      {
	uns x = conv_ucs_to_x(code);
	if (x == 256)				// Internal code of replacement char
	  continue;
	forbidden_chars[x] &= ~lfmask;		// Clear the bits from all other bitmaps
	typical_chars[x] &= ~lfmask;
	improbable_chars[x] &= ~lfmask;
	char_array[x] |= lfmask;
      }
}

static byte *
cs_cf_commit_families(void)
{
  CF_JOURNAL_VAR(num_lfams);
  CF_JOURNAL_VAR(lfam_by_id);
  num_lfams = 0;
  bzero(lfam_by_id, sizeof(lfam_by_id));

  uns size = conv_x_count();
  XTRACE("Allocating character class maps for %d characters", size);
  CF_JOURNAL_VAR(typical_chars);
  CF_JOURNAL_VAR(forbidden_chars);
  CF_JOURNAL_VAR(improbable_chars);
  typical_chars = cf_malloc_zero(4*size);
  forbidden_chars = cf_malloc_zero(4*size);
  forbidden_chars[0] = ~0U;
  improbable_chars = cf_malloc_zero(4*size);

  CLIST_FOR_EACH(struct lang_family *, f, lfam_list)
    {
      if (!strcmp(f->name, "*"))
	{
	  if (lfam_by_id[DEFAULT_LFAM])
	    return "Multiple wildcard language families defined";
	  f->id = DEFAULT_LFAM;
	}
      else if (num_lfams >= DEFAULT_LFAM)
	return "Too many language families defined";
      else if (!f->lang_set)
	return cf_printf("Language family `%s' contains no languages", f->name);
      else
	f->id = num_lfams++;
      lfam_by_id[f->id] = f;
    }

  if (!lfam_by_id[DEFAULT_LFAM])
    return "At least a default language family must be defined";

  for (int i=DEFAULT_LFAM; i>=0; i--)
    {
      struct lang_family *f = lfam_by_id[i];
      if (f)
	{
	  mark_chars(typical_chars, f->id, f->chars_typical);
	  mark_chars(improbable_chars, f->id, f->chars_improbable);
	  mark_chars(forbidden_chars, f->id, f->chars_forbidden);
	}
    }
  return NULL;
}

static byte *
cs_cf_commit(void)
{
  byte *err;
  err = cs_cf_commit_aliases();
  if (!err)
    err = cs_cf_commit_families();
  return err;
}

static byte *
cs_charset_parser(byte *s, int *ptr)
{
  *ptr = find_charset_by_name(s);
  return (*ptr < 0) ? "Unknown charset" : NULL;
}

static void
cs_charset_dumper(struct fastbuf *b, int *ptr)
{
  bprintf(b, "%d(%s) ", *ptr, (*ptr < 0) ? "?" : charset_name(*ptr));
}

static struct cf_user_type cs_type_charset = {
  .size = sizeof(int),
  .name = "charset",
  .parser = (cf_parser1 *) cs_charset_parser,
  .dumper = (cf_dumper1 *) cs_charset_dumper
};

static byte *
cs_cf_init_charset(struct charset_names *c)
{
  c->charset_id = -1;
  return NULL;
}

static byte *
cs_cf_commit_charset(struct charset_names *c)
{
  if (c->charset_id < 0)
    return "Missing charset name";
  return NULL;
}

static struct cf_section cs_cf_charset = {
  CF_TYPE(struct charset_names),
  CF_INIT(cs_cf_init_charset),
  CF_COMMIT(cs_cf_commit_charset),
  CF_ITEMS {
    CF_USER("Name", PTR_TO(struct charset_names, charset_id), &cs_type_charset),
    CF_STRING_DYN("Aliases", PTR_TO(struct charset_names, aliases), CF_ANY_NUM),
    CF_END
  }
};

static byte *
cs_cf_init_family(struct lang_family *f)
{
  f->name = "*";
  return NULL;
}

static struct cf_section cs_cf_family = {
  CF_TYPE(struct lang_family),
  CF_INIT(cs_cf_init_family),
#define F(f) PTR_TO(struct lang_family, f)
  CF_ITEMS {
    CF_STRING("Name", F(name)),
    CF_PARSER("Languages", F(lang_set), lang_cf_parse_set, CF_ANY_NUM),
    CF_USER_DYN("AutoSets", F(auto_sets), &cs_type_charset, CF_ANY_NUM),
    CF_USER_DYN("CTypical", F(chars_typical), &cf_type_unirange, CF_ANY_NUM),
    CF_USER_DYN("CForbid", F(chars_forbidden), &cf_type_unirange, CF_ANY_NUM),
    CF_USER_DYN("CImprobable", F(chars_improbable), &cf_type_unirange, CF_ANY_NUM),
    CF_END
  }
#undef F
};

static byte *
cs_cf_commit_deflang(struct def_lang *d)
{
  if (!d->host_patt)
    return "Missing HostPatt";
  if (!d->lang_list)
    return "Missing LangList";
  return NULL;
}

static struct cf_section cs_cf_deflang = {
  CF_TYPE(struct def_lang),
  CF_COMMIT(cs_cf_commit_deflang),
  CF_ITEMS {
    CF_STRING("HostPatt", PTR_TO(struct def_lang, host_patt)),
    CF_STRING("LangList", PTR_TO(struct def_lang, lang_list)),
    CF_END
  }
};

static struct cf_section charset_config = {
  CF_COMMIT(cs_cf_commit),
  CF_ITEMS {
    CF_UNS("Trace", &trace_charsets),
    CF_UNS("LogErrors", &log_charset_errors),
    CF_INT("BelieveServer", &believe_server_charset),
    CF_INT("BelieveMETA", &believe_meta_charset),
    CF_USER("FallbackCharset", &fallback_charset, &cs_type_charset),
    CF_LIST("Charset", &charset_list, &cs_cf_charset),
    CF_STRING_DYN("CharsetBlacklist", &charset_blacklist, CF_ANY_NUM),
    CF_LIST("LangFamily", &lfam_list, &cs_cf_family),
    CF_LIST("DefLang", &def_lang_list, &cs_cf_deflang),
    CF_INT("ImprobablePenalty", &improbable_penalty),
    CF_INT("ForbiddenPenalty", &forbidden_penalty),
    CF_INT("UTF8Penalty", &utf8_penalty),
    CF_INT("BelieveMinGrade", believe_min_grade),
    CF_INT("BelieveMinGrade2", believe_min_grade + 1),
    CF_END
  }
};

static void CONSTRUCTOR charset_init_config(void)
{
  cf_declare_section("Charset", &charset_config, 0);
}

/* Languages */

static uns
guess_lfams(int *lfams)
{
  byte *doc_lang, *guessed_lang = NULL;
  int langs[MAX_LANG_LIST_ITEMS];
  int n = 0;
  int nfams = 0;

  if (doc_lang = gthis->language)
    n = lang_parse_list(stk_strdup(doc_lang), NULL, langs, 1);
  if (n <= 0)
    {
      CLIST_FOR_EACH(struct def_lang *, d, def_lang_list)
	if (str_match_pattern_nocase(d->host_patt, gthis->url_s.host))
	  {
	    guessed_lang = d->lang_list;
	    n = lang_parse_list(stk_strdup(guessed_lang), NULL, langs, 1);
	    break;
	  }
    }

  uns mask = 0;
  for (int i=0; i<n; i++)
    if (langs[i] >= 0)
      {
	int found = 0;
	CLIST_FOR_EACH(struct lang_family *, fam, lfam_list)
	  if (fam->id != DEFAULT_LFAM)
	    if (fam->lang_set & (1 << langs[i]))
	      {
		if (!(mask & (1 << fam->id)))
		  {
		    mask |= (1 << fam->id);
		    lfams[nfams++] = fam->id;
		  }
		found++;
	      }
	if (!found)
	  mask |= 1 << DEFAULT_LFAM;
      }

  if (!mask)
    mask = 1 << DEFAULT_LFAM;
  if (mask & (1 << DEFAULT_LFAM))
    lfams[nfams++] = DEFAULT_LFAM;
  lfams[nfams] = -1;

  TRACE("Language families: orig=<%s> guess=<%s> mask=%08x",
	doc_lang ? : (byte *) "?",
	guessed_lang ? : (byte *) "?",
	mask);

  return mask;
}

/* Character set vaticination */

static void
calc_histogram(uns *hist)
{
  struct fastbuf *b = fbmem_clone_read(gthis->contents);
  int c;

  bzero(hist, 256*sizeof(hist[0]));
  while ((c = bgetc(b)) >= 0)
    hist[c]++;
  bclose(b);
}

static unsigned short int *get_charset_table(uns id)
{
  struct conv_context ct;

  conv_init(&ct);
  conv_set_charset(&ct, id, CONV_CHARSET_UTF8);
  return ct.in_to_x;
}

#define	MAX_GRADE	1000

static int
grade_histogram(byte *chs_name, unsigned short int *table, u32 lfamset, uns *histogram)
{
  int forbidden = 0, typical = 0, improbable = 0, non_ascii = 0;
  for (uns i=0; i<256; i++)
    if (histogram[i])
    {
      uns x = table[i];
      if (x >= 0x80 || x < 0x20 && !Cblank(x))
	non_ascii += histogram[i];
      if (forbidden_chars[x] & lfamset)
	forbidden += histogram[i];
      else if (typical_chars[x] & lfamset)
	typical += histogram[i];
      else if (improbable_chars[x] & lfamset)
	improbable += histogram[i];
    }
  int grade;
  if (non_ascii)
    grade = (typical - improbable * improbable_penalty - forbidden * forbidden_penalty) * MAX_GRADE / non_ascii;
  else
    grade = MAX_GRADE;
  XTRACE("%s: forbidden=%d, typical=%d, improbable=%d, non-ascii=%d ==> grade=%d", chs_name, forbidden, typical, improbable, non_ascii, grade);
  return grade;
}

static int
grade_utf8(void)
{
  struct fastbuf *b = fbmem_clone_read(gthis->contents);
  int c;
  uns code;
  int correct = 0, incorrect = 0, neutral = 0;

  while ((c = bgetc(b)) >= 0)
    {
      if (c < 0x80)
	;
      else if (c < 0xc0)
	incorrect++;
      else if (c >= 0xfe)
	incorrect++;
      else
	{
	  int cnt = 1;
	  while (c & (0x40 >> cnt))
	    cnt++;
	  code = c & (0x3f >> cnt);
	  while (cnt--)
	    {
	      c = bgetc(b);
	      if (c < 0x80 || c >= 0xc0)
	      {
		incorrect++;
		if (c >= 0xc0)
		  bungetc(b);
		goto on_err;
	      }
	      code = (code << 6) | (c & 0x3f);
	    }
	  if (code < 0x10000)
	    {
	      uns x = conv_ucs_to_x(code);
	      if (x != 256)
		correct++;
	      else
		neutral++;
	    }
on_err:
	  ;
	}
    }
  bclose(b);
  int grade;
  if (correct + incorrect + neutral)
    grade = (correct - incorrect * utf8_penalty) * MAX_GRADE / (correct + incorrect + neutral);
  else
    grade = MAX_GRADE;
  XTRACE("utf-8: correct=%d, incorrect=%d, neutral=%d ==> grade=%d", correct, incorrect, neutral, grade);
  return grade;
}

static int
check_charset(uns id, u32 lfamset, uns *histogram, int threshold)
{
  byte *chs_name = charset_name(id);
  int grade;

  if (id == CONV_CHARSET_UTF8)
    grade = grade_utf8();
  else
  {
    unsigned short int *table = get_charset_table(id);
    grade = grade_histogram(chs_name, table, lfamset, histogram);
  }

  XTRACE("Charset %s %s (grade %d, threshold %d)", chs_name, (grade >= threshold) ? "accepted" : "is improbable", grade, threshold);
  return grade;
}

static void
build_charset_list(int *list, int *lfams)
{
  int i;
  u32 mask = 0;

  while ((i = *lfams++) >= 0)
    {
      struct lang_family *fam = lfam_by_id[i];
      ASSERT(fam);
      for (uns j=0; j<DARY_LEN(fam->auto_sets); j++)
	{
	  uns k = fam->auto_sets[j];
	  if (!(mask & (1 << k)))
	    {
	      mask |= 1 << k;
	      *list++ = k;
	    }
	}
    }
  *list = -1;
}

static int
guess_charset(u32 lfamset, int *lfams, uns *histogram, int threshold)
{
  int list[MAX_CHARSETS+1];
  int bestgrd = MAX(threshold, -1), bestset = -1;
  int i, set, grade;

  build_charset_list(list, lfams);
  for (i=0; list[i] >= 0; i++)
    {
      set = list[i];
      if (set == CONV_CHARSET_UTF8)
	{
	  grade = grade_utf8();
	}
      else
	{
	  unsigned short int *table = get_charset_table(set);
	  grade = grade_histogram(charset_name(set), table, lfamset, histogram);
	}
      if (grade > bestgrd)
	{
	  bestgrd = grade;
	  bestset = set;
	}
    }
  return bestset;
}

static int
guess_fallback(int *lfams)
{
  while (*lfams >= 0)
    {
      struct lang_family *fam = lfam_by_id[*lfams++];
      ASSERT(fam);
      if (DARY_LEN(fam->auto_sets) > 0)
	return fam->auto_sets[0];
    }
  return -1;
}

/* Conversion */

static void
convert_stream_charset(uns id)
{
  struct fastbuf *src, *dest;
  struct conv_context ct;
  uns flags, len;
  byte destbuf[4096];

  if (id == CONV_CHARSET_UTF8)
  {
    if (grade_utf8() < MAX_GRADE)
    {
      XTRACE("Repairing broken UTF-8");
      src = fbmem_clone_read(gthis->contents);
      dest = fbmem_create(16384);
      int c;
      while ((c = bget_utf8(src)) >= 0)
	bput_utf8(dest, c);
      bclose(src);
      bclose(gthis->contents);
      gthis->contents = dest;
    }
    return;
  }

  XTRACE("Recoding to canonical charset");
  src = fbmem_clone_read(gthis->contents);
  dest = fbmem_create(16384);
  conv_init(&ct);
  conv_set_charset(&ct, id, CONV_CHARSET_UTF8);
  do
    {
      flags = conv_run(&ct);
      if (flags & CONV_DEST_END)
	{
	  if (ct.dest != ct.dest_start)
	    bwrite(dest, destbuf, ct.dest - destbuf);
	  ct.dest = destbuf;
	  ct.dest_end = destbuf + sizeof(destbuf);
	}
      else if (flags & CONV_SOURCE_END)
	{
	  byte *s;
	  len = bdirect_read_prepare(src, &s);
	  if (!len)
	    break;
	  ct.source = s;
	  ct.source_end = s + len;
	  bdirect_read_commit(src, s+len);
	}
      else
	ASSERT(0);
    }
  while (1);
  if (ct.dest != ct.dest_start)
    bwrite(dest, destbuf, ct.dest - destbuf);

  bclose(src);
  bclose(gthis->contents);
  gthis->contents = dest;
}

/* Main decision routine */

void
convert_charset(byte *meta_charset)
{
  byte *server_charset = gthis->charset;
  uns histogram[256];
  int lfams[MAX_LFAMS+1];
  u32 lfamset;
  byte *cs;
  int s_id = CHN_UNKNOWN, m_id = CHN_UNKNOWN, g_id = CHN_UNKNOWN, id = CHN_UNKNOWN;
  byte *s_rem = "";
  byte *m_rem = "";
  byte *source = "?";

  lfamset = guess_lfams(lfams);
  ASSERT(lfams[0] >= 0);
  calc_histogram(histogram);

  int pass = 0;
  int doc_grade;
start_pass:
  doc_grade = -1000000;

  /* Charset supplied by the server */
  if (cs = server_charset)
    {
      source = "server";
      if ((s_id = lookup_charset(cs)) == CHN_UNKNOWN)
	{
	  if (log_charset_errors)
	    log(L_ERROR_R, "Unrecognized server charset %s", cs);
          if (believe_server_charset >= 2)
	    goto unknown;
	  s_rem = " [unknown]";
	}
      else if (believe_server_charset < 0)
	s_rem = " [forced-no]";
      else if (s_id == CHN_BLACKLISTED)
        {
	  s_rem = " [blacklisted]";
	  goto blacklisted;
	}
      else if (believe_server_charset > 0)
	{
	  s_rem = " [forced-ok]";
	  id = s_id;
	  goto okay;
	}
      else
      {
	int grade = check_charset(s_id, lfamset, histogram, believe_min_grade[pass]);
	if (grade >= believe_min_grade[pass])
	{
	  s_rem = " [confirmed]";
	  id = s_id;
	  goto okay;
	}
	else
	{
	  if (log_charset_errors > 1)
	    log(L_ERROR_R, "%s server charset %s", ((grade < 0) ? "Disproved" : "Improbable"), cs);
	  s_rem = (grade < 0) ? " [disproved]" : " [improbable]";
	  doc_grade = grade;
	}
      }
    }

  /* Charset supplied by META tags */
  if (cs = meta_charset)
    {
      source = "META";
      if ((m_id = lookup_charset(cs)) == CHN_UNKNOWN)
	{
	  if (log_charset_errors)
	    log(L_ERROR_R, "Unrecognized META charset %s", cs);
          if (believe_meta_charset >= 2)
	    goto unknown;
	  m_rem = " [unknown]";
	}
      else if (believe_meta_charset < 0)
	m_rem = " [forced-no]";
      else if (m_id == CHN_BLACKLISTED)
        {
	  m_rem = " [blacklisted]";
	  goto blacklisted;
	}
      else if (believe_meta_charset > 0)
	{
	  m_rem = " [forced-ok]";
	  id = m_id;
	  goto okay;
	}
      else
      {
	int grade = check_charset(m_id, lfamset, histogram, believe_min_grade[pass]);
	if (grade >= believe_min_grade[pass])
	{
	  m_rem = " [confirmed]";
	  id = m_id;
	  goto okay;
	}
	else
	{
	  if (log_charset_errors > 1)
	    log(L_ERROR_R, "%s META charset %s", ((grade < 0) ? "Disproved" : "Improbable"), cs);
	  m_rem = (grade < 0) ? " [disproved]" : " [improbable]";
	  if (grade >= doc_grade)
	    doc_grade = grade;
	}
      }
    }

  /* Try auto-detection */
  if ((g_id = guess_charset(lfamset, lfams, histogram, doc_grade)) >= 0)
    {
      source = "guessed";
      id = g_id;
      goto okay;
    }
  else if (!pass && (s_id >= 0 || m_id >= 0))
    {
      TRACE("Entering 2nd pass with smaller threshold");
      pass = 1;
      goto start_pass;
    }
  else if ((g_id = guess_fallback(lfams)) >= 0)
  {
    if (check_charset(g_id, lfamset, histogram, doc_grade) > doc_grade)
    {
      source = "guessed-fallback";
      id = g_id;
      goto okay;
    }
  }

  /* Everything failed, try server/meta charset or fall back */
  if (s_id >= 0)
    {
      source = "fallback-server";
      id = s_id;
    }
  else if (m_id >= 0)
    {
      source = "fallback-meta";
      id = m_id;
    }

 okay:
  if (id < 0)
    {
      source = "fallback";
      id = fallback_charset;
    }
  cs = charset_name(id);

  TRACE("Charsets: server=<%s>%s meta=<%s>%s -> %s [%s]",
	server_charset ? : (byte *) "?", s_rem,
	meta_charset ? : (byte *) "?", m_rem,
	cs, source);
  gthis->charset = cs;
  obj_set_attr(gthis->aa, 'c', cs);
  if ((server_charset && server_charset[0] && strcasecmp(server_charset, cs)) ||
      (meta_charset && meta_charset[0] && strcasecmp(meta_charset, cs)))
    obj_add_attr_format(gthis->aa, '.', "Charsets: server=<%s>%s meta=<%s>%s -> %s [%s]",
			server_charset ? : (byte *) "?", s_rem,
			meta_charset ? : (byte *) "?", m_rem,
			cs, source);

  convert_stream_charset(id);
  return;

blacklisted:
  TRACE("Charsets: server=<%s>%s meta=<%s>%s -> blacklisted [%s]",
	server_charset ? : (byte *) "?", s_rem,
	meta_charset ? : (byte *) "?", m_rem,
	source);
  gerror(2407, "Blacklisted charset");

unknown:
  TRACE("Charsets: server=<%s>%s meta=<%s>%s -> unknown",
        server_charset ? : (byte *) "?", s_rem,
	meta_charset ? : (byte *) "?", m_rem);
  gerror(2407, "Unknown charset");
}
