/*
 *	Sherlock Indexer -- Generate Index Reports
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/url.h"
#include "ucw/fastbuf.h"
#include "ucw/chartype.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"
#include "lang/lang.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

static char *fn_class_log;
static uns class_threshold;
static uns filetype_stats;
static uns language_stats;
static uns domain_stats;
static uns domain_stats_limit = ~0U;

static struct cf_section reporter_config = {
  CF_ITEMS {
    CF_STRING("ClassLog", &fn_class_log),
    CF_UNS("ClassThreshold", &class_threshold),
    CF_UNS("FiletypeStats", &filetype_stats),
    CF_UNS("LanguageStats", &language_stats),
    CF_UNS("DomainStats", &domain_stats),
    CF_UNS("DomainStatsLimit", &domain_stats_limit),
    CF_END
  }
};

static u32 *sizes;

#define SORT_PREFIX(x) large_##x
#define SORT_KEY_REGULAR struct large_entry
#define SORT_DATA_SIZE(k) ((k).len)
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

struct large_entry {
  u32 cls;
  u32 len;
};

static inline int
large_compare(struct large_entry *a, struct large_entry *b)
{
  REV_COMPARE(sizes[a->cls], sizes[b->cls]);
  COMPARE(a->cls, b->cls);
  return 0;
}

#include "ucw/sorter/sorter.h"

static void
report_merges(void)
{
  if (!fn_class_log)
    return;
  log(L_INFO, "Generating equivalence class log");

  /* Load merging array */
  merges_map(0);

  /* Calculate class sizes */
  sizes = big_alloc_zero(card_count*4);
  for (uns i=0; i<card_count; i++)
    if (!(bring_attr(i)->flags & CARD_FLAG_EMPTY) && merges[i] != (u32)~0U)
      sizes[merges[i]]++;

  /* Log large classes */
  struct large_entry le;
  struct fastbuf *urls = index_bopen(fn_urls, O_RDONLY, 1);
  struct fastbuf *b = index_bopen_tmp(1);
  uns this_url = 0;
  for (uns i=0; i<card_count; i++)
    if (!(bring_attr(i)->flags & CARD_FLAG_EMPTY) && merges[i] != (u32)~0U && sizes[merges[i]] >= class_threshold)
      {
	char buf[MAX_URL_SIZE+1];
	while (this_url <= i)
	  {
	    bgets(urls, buf, sizeof(buf));
	    this_url++;
	  }
	le.cls = merges[i];
	le.len = strlen(buf);
	bwrite(b, &le, sizeof(le));
	bwrite(b, buf, le.len);
      }
  bclose(urls);
  merges_unmap();
  brewind(b);
  b = large_sort(b, NULL);
  struct fastbuf *out = index_bopen(fn_class_log, O_WRONLY | O_CREAT | O_TRUNC, 1);
  while (breadb(b, &le, sizeof(le)))
    {
      bprintf(out, "%d\t%08x\t", sizes[le.cls], le.cls);
      bbcopy(b, out, le.len);
      bputc(out, '\n');
    }
  bclose(b);
  bclose(out);

  /* Clean up */
  big_free(sizes, card_count*4);
}

#ifdef CONFIG_FILETYPE
static void
report_filetypes(void)
{
  if (!filetype_stats)
    return;

  uns ft_cnt[2][MAX_FILE_TYPES];
  bzero(ft_cnt, sizeof(ft_cnt));
  for (uns i=0; i<card_count; i++)
    {
      struct card_attr *ca = bring_attr(i);
      uns t = CA_GET_FILE_TYPE(ca);
      ft_cnt[0][t]++;
      if (!(ca->flags & (CARD_FLAG_DUP | CARD_FLAG_EMPTY)))
	ft_cnt[1][t]++;
    }

  uns bufsize = 32;
  for (uns t=0; t<MAX_FILE_TYPES; t++)
    bufsize += strlen(custom_file_type_names[t]) + 16;
  byte buf[bufsize];

  for (uns i=0; i<2; i++)
    {
      byte *p = buf + sprintf(buf, "Filetypes %s:", i ? "out" : "in");
      for (uns t=0; t<MAX_FILE_TYPES; t++)
	p += sprintf(p, " %s=%d", custom_file_type_names[t], ft_cnt[i][t]);
      log(L_INFO, "%s", buf);
    }
}

#else
static inline void
report_filetypes(void)
{
  if (filetype_stats)
    log(L_ERROR, "Filetype statistics requested, but Holmes was compiled without support for filetypes");
}
#endif

#ifdef CONFIG_LANG
static void
report_langs(void)
{
  if (!language_stats)
    return;

  uns lang_cnt[MAX_LANGUAGES+1];
  bzero(lang_cnt, sizeof(lang_cnt));
  for (uns i=0; i<card_count; i++)
    {
      struct card_attr *ca = bring_attr(i);
#ifdef CONFIG_FILETYPE
      uns t = CA_GET_FILE_TYPE(ca);
      if (!FILETYPE_IS_TEXT(t))
	continue;
#endif
      lang_cnt[CA_GET_FILE_LANG(ca)]++;
    }

  uns bufsize = 32;
  for (uns l=0; l<MAX_LANGUAGES; l++)
    if (lang_code_exists(l))
      bufsize += strlen(lang_code_to_name(l)) + 16;
  byte buf[bufsize];

  byte *p = buf + sprintf(buf, "Languages:");
  uns unknown_lang_cnt = 0;
  for (uns l=0; l<MAX_LANGUAGES; l++)
    if (lang_code_exists(l))
      p += sprintf(p, " %s=%d", lang_code_to_name(l), lang_cnt[l]);
    else
      unknown_lang_cnt += lang_cnt[l];
  sprintf(p, " \?\?=%d", unknown_lang_cnt);
  log(L_INFO, "%s", buf);
}
#else
static inline void
report_langs(void)
{
  if (language_stats)
    log(L_ERROR, "Language statistics requested, but Holmes was compiled without support for multiple languages");
}
#endif

struct domain {
  uns cnt;
  char *dom;
};

static inline void
dom_init_data(struct domain *d)
{
  d->cnt = 0;
}

#define HASH_NODE struct domain
#define HASH_PREFIX(x) dom_##x
#define HASH_KEY_STRING dom
#define HASH_WANT_LOOKUP
#define HASH_GIVE_INIT_DATA
#define HASH_AUTO_POOL 4096
#include "ucw/hashtable.h"

#define ASORT_PREFIX(x) domain_##x
#define ASORT_KEY_TYPE struct domain
#define ASORT_LT(x,y) (x.cnt > y.cnt)
#include "ucw/sorter/array-simple.h"

static void
report_domains(void)
{
  if (!domain_stats)
    return;

  struct fastbuf *b = index_bopen(fn_urls, O_RDONLY, 1);
  char url[MAX_URL_SIZE], buf[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
  uns numeric = 0;
  dom_init();
  while (bgets(b, url, sizeof(url)))
    {
      struct url ur;
      if (url_deescape(url, buf) || url_split(buf, &ur, buf2))
	ASSERT(0);
      char *d = ur.host + strlen(ur.host);
      if (Cdigit(d[-1]))
	numeric++;
      else
	{
	  uns lev = domain_stats;
	  while (d > ur.host && lev)
	    if (*--d == '.')
	      lev--;
	  if (*d == '.')
	    d++;
	  struct domain *dom = dom_lookup(d);
	  if (dom->dom == d)			// just created
	    dom->dom = xstrdup(d);
	  dom->cnt++;
	}
    }
  bclose(b);

  uns count = 0;
  HASH_FOR_ALL(dom, dom UNUSED)
    count++;
  HASH_END_FOR;

  struct domain domains[count];
  uns i = 0;
  HASH_FOR_ALL(dom, dom)
    domains[i++] = *dom;
  HASH_END_FOR;

  domain_sort(domains, count);
  for (i=0; i<count && i<domain_stats_limit; i++)
    log(L_INFO, "Domain %s: %u", domains[i].dom, domains[i].cnt);
  uns other = 0;
  for (; i<count; i++)
    other += domains[i].cnt;
  if (other)
    log(L_INFO, "Other domains: %u", other);
  log(L_INFO, "Numeric addresses: %u", numeric);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  cf_declare_section("Reporter", &reporter_config, 0);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  attrs_part_map(0);
  report_merges();
  report_filetypes();
  report_langs();
  report_domains();
  attrs_part_unmap();
  return 0;
}
