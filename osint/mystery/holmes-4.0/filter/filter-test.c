/*
 *	Sherlock Filter Engine -- Filter Testing Utility
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/url.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "filter/filter.h"
#include "sherlock/index.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

enum builtin_type {
  B_GATHERER = 1,
  B_GATHERD = 2,
  B_SHEPHERD = 4,
  B_INDEXER = 8,
  B_URL = 16
};

enum out_type {
  W_STATS = 1,
  W_ACCEPT = 2,
  W_REJECT = 4,
  W_VERBOSE = 8,
  W_VERDICT = 16,
  W_SUMMARY = 32,
  W_DUMP = 64,
};

/* Keep in sync with the list of builtin variables in filter/builtin.c */
#define VARS \
  VAR(url, T_STR, B_URL) \
  VAR(protocol, T_STR, B_URL) \
  VAR(host, T_STR, B_URL) \
  VAR(port, T_INT, B_URL) \
  VAR(path, T_STR, B_URL) \
  VAR(username, T_STR, B_URL) \
  VAR(password, T_STR, B_URL) \
  VAR(server_type, T_STR, B_GATHERER) \
  VAR(content_type, T_STR, B_GATHERER | B_GATHERD | B_SHEPHERD) \
  VAR(content_encoding, T_STR, B_GATHERER | B_GATHERD | B_SHEPHERD) \
  VAR(ignore_links, T_INT, B_GATHERER) \
  VAR(ignore_text, T_INT, B_GATHERER) \
  VAR(error_code, T_INT, B_GATHERER) \
  VAR(user_mark, T_INT, B_SHEPHERD) \
  VAR(section, T_INT, B_GATHERD | B_SHEPHERD) \
  VAR(section_soft_max, T_INT, B_GATHERD) \
  VAR(section_hard_max, T_INT, B_GATHERD) \
  VAR(queue_bonus, T_INT, B_GATHERD | B_SHEPHERD) \
  VAR(queue_key, T_INT, B_GATHERD | B_SHEPHERD) \
  VAR(soft_limit, T_INT, B_SHEPHERD) \
  VAR(hard_limit, T_INT, B_SHEPHERD) \
  VAR(fresh_limit, T_INT, B_SHEPHERD) \
  VAR(min_delay, T_INT, B_SHEPHERD) \
  VAR(max_conn, T_INT, B_SHEPHERD) \
  VAR(monitor, T_INT, B_SHEPHERD) \
  VAR(select_bonus, T_INT, B_SHEPHERD) \
  VAR(refresh_schema, T_INT, B_SHEPHERD) \
  VAR(refresh_boost, T_INT, B_SHEPHERD) \
  VAR(stable_time, T_INT, B_SHEPHERD) \
  VAR(filter_robots, T_INT, B_SHEPHERD) \
  VAR(area, T_INT, B_SHEPHERD) \
  VAR(site_level, T_INT, B_INDEXER) \
  VAR(site, T_STR, B_INDEXER) \
  VAR(bonus, T_INT, B_INDEXER) \
  VAR(language, T_STR, B_INDEXER) \
  VAR(card_bonus, T_INT, B_INDEXER) \
  VAR(title, T_STR, B_INDEXER) \
  VAR(image_size, T_INT, B_INDEXER) \
  VAR(image_aspect_ratio, T_INT, B_INDEXER) \
  VAR(image_colors, T_INT, B_INDEXER) \
  VAR(audio_length, T_INT, B_INDEXER) \
  VAR(audio_bitrate, T_INT, B_INDEXER) \
  VAR(audio_srate, T_INT, B_INDEXER) \
  VAR(audio_channels, T_INT, B_INDEXER) \
  VAR(noindex, T_INT, B_INDEXER) \
  VAR(want_xform, T_INT, B_SHEPHERD | B_INDEXER) \
  VAR(url_xform, T_STR, B_SHEPHERD | B_INDEXER) \
  VAR(src_url, T_STR, B_SHEPHERD | B_INDEXER)

struct args {
#define VAR(name, type, class) type name;
#define T_INT int
#define T_STR byte *
  VARS
#undef T_STR
#undef T_INT
#undef VAR
};

static const struct var {
  char *name;
  uns is_string;
  uns offset;
  uns class;
} vars[] = {
#define VAR(name, type, class) { #name, type, OFFSETOF(struct args, name), class },
#define T_INT 0
#define T_STR 1
  VARS
#undef T_STR
#undef T_INT
#undef VAR
};
static uns vars_cnt = ARRAY_SIZE(vars);

static struct filter_binding *
gen_bindings(uns class)
{
  struct filter_binding *binds = xmalloc_zero((vars_cnt+1) * sizeof(*binds));
  uns j = 0;

  for (uns i=0; i<vars_cnt; i++)
    if (vars[i].class & class)
      {
	binds[j].name = vars[i].name;
	binds[j].offset = vars[i].offset;
	j++;
      }
  return binds;
}

struct verdict {
  uns cnt;
  byte msg[1];
};

static inline void verdict_hash_init_data(struct verdict *v)
{
  v->cnt = 0;
}

#define HASH_PREFIX(x) verdict_hash_##x
#define HASH_NODE struct verdict
#define HASH_KEY_ENDSTRING msg
#define HASH_WANT_LOOKUP
#define HASH_GIVE_INIT_DATA
#include "ucw/hashtable.h"

static void NONRET usage(void)
{
  fputs("\
Usage: filter-test [<options>] <filter-file>\n\
\n\
General options:\n"
CF_USAGE "\
\n\
Options selecting sets of builtin variables:\n\
--gatherer\t\tGathering library\n\
--gatherd\t\tGatherd selection rules\n\
--shepherd\t\tShepherd selection rules\n\
--indexer\t\tIndexer\n\
--all-vars\t\tAll known variables\n\
\n\
Options selecting format of output:\n\
--stats\t\t\tPrint statistics\n\
--accepted\t\tPrint URL's accepted by the filter\n\
--rejected\t\tPrint URL's rejected by the filter\n\
--verbose\t\tPrint all URL's along with filter verdicts\n\
--verdicts\t\tPrint filter verdicts\n\
--summary\t\tPrint summary of all verdicts\n\
--urlkey\t\tAlso show the URL key\n\
\n\
Options controlling filter variables:\n\
--init var=val\t\tInitialize given filter variable\n\
--dump\t\t\tDump filter variables after each URL\n\
", stderr);
  exit(1);
}

static struct option longopts[] = {
  CF_LONG_OPTS
  { "gatherer",		0, 0, 0x8000 | B_GATHERER },
  { "gatherd",		0, 0, 0x8000 | B_GATHERD },
  { "shepherd",		0, 0, 0x8000 | B_SHEPHERD },
  { "indexer",		0, 0, 0x8000 | B_INDEXER },
  { "all-vars",		0, 0, 0x80ff },
  { "stats",		0, 0, 's' },
  { "accepted",		0, 0, 'a' },
  { "rejected",		0, 0, 'r' },
  { "verbose",		0, 0, 'v' },
  { "verdicts",		0, 0, 'w' },
  { "summary",		0, 0, 'm' },
  { "urlkey",		0, 0, 'k' },
  { "init",		1, 0, 'i' },
  { "dump",		0, 0, 'd' },
  { NULL,		0, 0, 0 }
};

static void
dump_variables(struct fastbuf *fo, struct args *a)
{
  for (uns i=0; i<vars_cnt; i++)
  {
    bprintf(fo, "%3d: %-20s = ", vars[i].offset, vars[i].name);
    void *ptr = (byte *) a + vars[i].offset;
    if (vars[i].is_string)
      bputsn(fo, (* (char **) ptr) ? : "(null)");
    else
      bprintf(fo, "%d\n", * (int *) ptr);
  }
}

int main(int argc, char **argv)
{
  int opt;
  uns out = 0;
  uns class = B_URL;
  uns show_keys = 0;
  struct args a_init;
  bzero(&a_init, sizeof(a_init));

  log_init("filter-test");
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS, longopts, NULL)) >= 0)
    switch (opt)
      {
      case 's':
	out |= W_STATS;
	break;
      case 'a':
	out |= W_ACCEPT;
	break;
      case 'r':
	out |= W_REJECT;
	break;
      case 'v':
	out |= W_VERBOSE;
	break;
      case 'w':
	out |= W_VERDICT;
	break;
      case 'm':
	out |= W_SUMMARY;
	break;
      case 'k':
	show_keys = 1;
	break;
      case 'i': ;
	uns i = 0;
	byte *val = strchr(optarg, '=');
	if (!val)
	  die("Expecting --init variable=value, not %s", optarg);
	*val++ = 0;
	while (i < vars_cnt && strcasecmp(vars[i].name, optarg))
	  i++;
	if (i >= vars_cnt)
	  die("Unknown filter variable %s", optarg);
	void *ptr = (byte *) &a_init + vars[i].offset;
	if (vars[i].is_string)
	  * (byte **) ptr = cf_strdup(val);
	else
	{
	  char *c;
	  errno = 0;
	  * (int *) ptr = strtoul(val, &c, 0);
	  if (c && *c || errno == ERANGE)
	    die("Invalid number %s: %m", val);
	}
	break;
      case 'd':
	out |= W_DUMP;
	break;
      default:
	if (opt & 0x8000)
	  class |= opt;
	else
	  usage();
      }
  if (optind != argc - 1)
    usage();

  struct filter *filter = filter_load(argv[optind], filter_builtin_vars, gen_bindings(class), NULL);
  struct filter_args *ar = filter_intr_new(filter);
  struct mempool *mp = mp_new(65536);

  if (out & W_SUMMARY)
    verdict_hash_init();
  if (show_keys)
    url_key_init();

  byte url[MAX_URL_SIZE], buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], keybuf[URL_KEY_BUF_SIZE];
  struct fastbuf *fi = bfdopen(0, 65536);
  struct fastbuf *fo = bfdopen(1, 65536);
  int autoflush = (isatty(1) > 0);
  uns nacc=0, nrej=0;
  while (bgets(fi, url, sizeof(url)))
    {
      struct args a = a_init;
      struct url ur;
      uns e;
      byte *msg = NULL;
      if (show_keys)
	{
	  byte *key = url_key(url, keybuf);
	  bprintf(fo, "URL Key: %s\n", key);
	}
      if (e = url_canon_split(url, buf1, buf2, &ur))
	{
	  msg = url_error(e);
	  e = 0;
	}
      else
	{
	  a.url = url;
	  a.protocol = ur.protocol;
	  a.host = ur.host;
	  a.port = ur.port;
	  a.path = ur.rest;
	  a.username = ur.user;
	  a.password = ur.pass;
	  ar->pool = mp;
	  ar->raw = &a;
	  mp_flush(mp);
	  e = filter_intr_run(ar);
	  if (!e)
	    msg = ar->msg ? : (byte*) "Filtered out";
	  if (out & W_DUMP)
	    dump_variables(fo, &a);
	}
      if (!e)
	nrej++;
      else
	{
	  nacc++;
	  msg = "OK";
	}
      if (out & (W_ACCEPT | W_REJECT | W_VERBOSE))
	{
	  if (((out & W_ACCEPT) && e) || ((out & W_REJECT) && !e) || (out & W_VERBOSE))
	    {
	      bputs(fo, url);
	      if (out & (W_VERBOSE | W_VERDICT))
		bprintf(fo, ": %s", msg);
	      bputc(fo, '\n');
	    }
	}
      else if (out & W_VERDICT)
	bputsn(fo, msg);
      if (out & W_SUMMARY)
	verdict_hash_lookup(msg)->cnt++;
      if (autoflush)
	bflush(fo);
    }

  if (out & W_STATS)
    bprintf(fo, "Accepted %d, rejected %d URL's\n", nacc, nrej);
  if (out & W_SUMMARY)
    {
      HASH_FOR_ALL(verdict_hash, v)
	{
	  bprintf(fo, "%9d %s\n", v->cnt, v->msg);
	}
      HASH_END_FOR;
    }

  bclose(fo);
  bclose(fi);
  return 0;
}
