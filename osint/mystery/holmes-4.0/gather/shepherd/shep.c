/*
 *	Sherlock Shepherd -- Manual Control
 *
 *	(c) 2004--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006--2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/sorter/common.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/protocol.h"
#include "gather/shepherd/man.h"

#include <stdlib.h>
#include <stdio.h>

static struct selector *selector;
static int opt_current;
static int opt_closed;
static uns opt_zombie_error;
static uns state_name_parsed;
static uns command_is_rw;
static uns command_is_remote;

static enum command {
  CMD_UNDEF,
  CMD_FIRST = 0x100,
  CMD_HELP,
  CMD_LIST,				/* CMD_LIST_xxx are translated to this one automatically */
  CMD_LIST_IDX,
  CMD_LIST_URLS,
  CMD_LIST_FP,
  CMD_LIST_ALL,
  CMD_INSERT,
  CMD_INSERT_REFS,
  CMD_DELETE,
  CMD_SET,				/* CMD_SET_xxx are translated to this one or merged with CMD_INSERT */
  CMD_SET_WEIGHT,
  CMD_SET_TRUE_WEIGHT,
  CMD_SET_FREQUENCY,
  CMD_SET_SECTION,
  CMD_SET_AREA,
  CMD_SET_INIT,
  CMD_SET_NEEDED,
  CMD_SET_REGATHER,
  CMD_SET_UNREF,
  CMD_SITES,
  CMD_SITES_FILTER,
  CMD_BUCKETS,
  CMD_HISTOGRAM,
  CMD_QKEY_STATS,
  CMD_REFRESH_STATS,
  CMD_FILTER,
  CMD_REMOTE_SET,			/* Some CMD_REMOTE_xxx go here */
  CMD_REMOTE_CLEANUP,
  CMD_REMOTE_IDLE,
  CMD_REMOTE_PRIVATE,
  CMD_REMOTE_DELETE_OLD,
  CMD_REMOTE_UNLOCK,
  CMD_BORROW,				/* CMD_BORROW_xxx go here */
  CMD_BORROW_STATE,
  CMD_BORROW_QUICK,
  CMD_BORROW_RETURN,
  CMD_BORROW_ROLLBACK,
  CMD_AREAS,
  CMD_SET_AREA_PARAMS,			/* CMD_SET_AREA_xxx go here */
  CMD_SET_AREA_SOFT,
  CMD_SET_AREA_HARD,
  CMD_SET_AREA_PLAN,
  CMD_REPLACE_INIT,
  CMD_TURN_TO_ZOMBIE,
} command;

enum opt_opt {
  OPT_UNDEF,
  OPT_FIRST = 0x300,
  OPT_VERBOSE,
  OPT_BARE,
  OPT_DAYS,
  OPT_SHOW_SITES,
  OPT_MULTI_AGE,
  OPT_SHOW_PLAN,
  OPT_SHOW_LASTMOD,
  OPT_CLOSED,
  OPT_CURRENT,
  OPT_SERVER
};

/*** Parsing of command line ***/

#undef CF_USAGE_TAB
#define CF_USAGE_TAB "\t"

static void NONRET
usage(byte *msg, ...)
{
  va_list args;
  va_start(args, msg);
  if (msg)
    {
      fprintf(stderr, "Invalid parameters: ");
      vfprintf(stderr, msg, args);
      fprintf(stderr, ".\nTry `shep --help' for more information.\n");
    }
  else
    fprintf(stderr, "\
Usage: shep [<options>] [<state>] <action> [<selector>]\n\
\n\
State specifications:\n\
<path>\t\t\t\tState in the given directory\n\
    --current\t\t\tThe current state (default for read-only operations)\n\
    --closed\t\t\tLast closed state\n\
\n\
Actions working over the network (no state, no selectors):\n\
    --remote-cleanup[=<0/1>]\tChange the `cleanup scheduled' switch\n\
    --remote-idle[=<0/1>]\tChange the `idle' (do not reap) switch\n\
    --remote-private[=<0/1>]\tChange the `private' (do not send buckets) switch\n\
    --remote-delete-old[=<0-2>]\tChange the `delete old states' switch\n\
    --remote-unlock\t\tReset the `locked' switch\n\
    --borrow-state\t\tBorrow the current state for manual changes (wait for close first)\n\
    --borrow-quick\t\tBorrow the current state (wait for close or corked first)\n\
    --return-state[=<phase>]\tReturn the borrowed state and switch to given phase (default=closed)\n\
    --rollback-state\t\tRoll back the borrowed state\n\
\n\
Actions working on states: (unless noted, different types cannot be combined)\n\
    --buckets\t\t\tShow buckets\n\
    --filter\t\t\tTest filtering rules (--urls selector only)\n\
-l, --list\t\t\tList index records\n\
-U, --list-urls\t\t\tList URL's\n\
    --list-fp\t\t\tList footprints\n\
-L, --list-all\t\t\tList index records with URL's\n\
    --insert\t\t\tInsert new URL's (--urls selector only; combines with --set-*)\n\
    --insert-refs\t\t--insert on all URL's referenced by the selected documents\n\
    --delete\t\t\tDelete records\n\
    --set-weight=<num>\t\tSet temporary document weight\n\
    --set-true-weight=<num>\tSet true document weight\n\
    --set-frequency=<num>\tSet refresh frequency\n\
    --set-section=<num>\t\tSet section number\n"
#ifdef CONFIG_AREAS
"\
    --set-area=<num>\t\tSet area number\n"
#endif
"\
    --set-init[=<0/1>]\t\tSet `belongs to initial set' flag\n\
    --set-needed[=<0/1>]\tSet `needed for eq finder' flag\n\
    --set-regather[=<0/1>]\tSet `priority regather requested' flag\n\
    --set-unref[=<0/1>]\t\tSet `unreferenced' flag\n\
-s, --sites\t\t\tList sites\n\
    --sites-filter\t\tList sites including parameters set by site filter\n\
-h, --histogram\t\t\tShow age histogram and other statistics\n\
-Q, --qkey-stats\t\tShow site statistics for each qkey\n\
    --refresh-stats\t\tShow refresh statistics for each qkey\n\
    --replace-init\t\tReplace the set of initial pages by given URL's\n\
    --turn-to-zombie=<error>\tTurns document into a zombie with a given error code (2000-2999)\n\
\n\
Selectors (most of them cannot be combined):\n\
    --all\t\t\tAll documents (default for read-only operations)\n\
-u, --url <url-pattern> ...\tURL's matching the given pattern (*'s allowed)\n\
    --urls <url> ...\t\tSpecified URL's (`-' = read from stdin; optimized for large lists)\n\
-q, --qkey [<port>:]<key>[/<pxlen>] ...\tSpecified queueing keys [also: unresolved, invalid, non-ip]\n\
-f, --fp <fp-pattern> ...\tFootprints matching the given pattern (*'s allowed)\n\
    --norm-fp <fp-pattern> ...\tNormalized footprints matching the given pattern\n\
    --fps <fp> ...\t\tSpecified footprints (`-' = read from stdin; optimized for large lists)\n\
    --only-flags [-]<flag> ...\tLimit to entries with specified flags\n\
    --only-newer <age>[dh]\tLimit to entries older than <age> seconds (days, hours)\n\
    --only-older <age>[dh]\tLimit to entries newer than <age> seconds (days, hours)\n\
    --only-types <type> ...\tLimit to entries of specified types\n"
#ifdef CONFIG_AREAS
"\
    --only-area <area>\t\tLimit to entries in the given area\n\
\n\
Commands working on areas and their selectors:\n\
    --areas\t\t\tList areas and their limits\n\
    --set-area-soft=<n>\t\tSet soft limit\n\
    --set-area-hard=<n>\t\tSet hard limit\n\
    --set-area-plan=<n>\t\tSet planning limit\n\
    --area <id> ...\t\tSelect the given area\n"
#endif
"\n\
Options:\n\
-b, --bare\t\t\tDon't print table headings\n\
    --days\t\t\tPrint document ages in days\n\
    --multi-age\t\t\tMultiply ages by refresh frequencies\n\
    --server=<host>[:<port>]\tServer to connect to (default: localhost)\n\
    --show-lastmod\t\tShow time of last modification instead of age\n\
    --show-plan\t\t\tInclude approximate planner priority in --list* commands\n\
-t, --show-sites\t\tResolve sites in --list* and --*stats commands\n\
-v, --verbose\t\t\tIncrease verbosity level\n\
" CF_USAGE);
  exit(1);
}

#define SHORT_OPTS "-bfhlqstuvLQU" CF_SHORT_OPTS

static struct { int opt; uns code; } short_opts[] = {
  { 'b',	OPT_BARE },
  { 'f',	SEL_FP },
  { 'h',	CMD_HISTOGRAM },
  { 'l',	CMD_LIST_IDX },
  { 'L',	CMD_LIST_ALL },
  { 'q',	SEL_QKEY },
  { 'Q',	CMD_QKEY_STATS },
  { 's',	CMD_SITES },
  { 't',	OPT_SHOW_SITES },
  { 'u',	SEL_URL },
  { 'U',	CMD_LIST_URLS },
  { 'v',	OPT_VERBOSE },
};

static struct option long_opts[] = {
  CF_LONG_OPTS
  { "help",				0, NULL, CMD_HELP },
  { "buckets",				0, NULL, CMD_BUCKETS },
  { "filter",				0, NULL, CMD_FILTER },
  { "list",				0, NULL, CMD_LIST_IDX },
  { "list-urls",			0, NULL, CMD_LIST_URLS },
  { "list-fp",				0, NULL, CMD_LIST_FP },
  { "list-all",				0, NULL, CMD_LIST_ALL },
  { "insert",				0, NULL, CMD_INSERT },
  { "insert-refs",			0, NULL, CMD_INSERT_REFS },
  { "delete",				0, NULL, CMD_DELETE },
  { "set-weight",			1, NULL, CMD_SET_WEIGHT },
  { "set-true-weight",			1, NULL, CMD_SET_TRUE_WEIGHT },
  { "set-frequency",			1, NULL, CMD_SET_FREQUENCY },
  { "set-section",			1, NULL, CMD_SET_SECTION },
  { "set-area",				1, NULL, CMD_SET_AREA },
  { "set-init",				2, NULL, CMD_SET_INIT },
  { "set-needed",			2, NULL, CMD_SET_NEEDED },
  { "set-regather",			2, NULL, CMD_SET_REGATHER },
  { "set-unref",			2, NULL, CMD_SET_UNREF },
  { "sites",				0, NULL, CMD_SITES },
  { "sites-filter",			0, NULL, CMD_SITES_FILTER },
  { "histogram",			0, NULL, CMD_HISTOGRAM },
  { "qkey-stats",			0, NULL, CMD_QKEY_STATS },
  { "refresh-stats",			0, NULL, CMD_REFRESH_STATS },
  { "remote-cleanup",			2, NULL, CMD_REMOTE_CLEANUP },
  { "remote-idle",			2, NULL, CMD_REMOTE_IDLE },
  { "remote-private",			2, NULL, CMD_REMOTE_PRIVATE },
  { "remote-delete-old",		2, NULL, CMD_REMOTE_DELETE_OLD },
  { "remote-unlock",			0, NULL, CMD_REMOTE_UNLOCK },
  { "borrow-state",			0, NULL, CMD_BORROW_STATE },
  { "borrow-quick",			0, NULL, CMD_BORROW_QUICK },
  { "return-state",			2, NULL, CMD_BORROW_RETURN },
  { "rollback-state",			0, NULL, CMD_BORROW_ROLLBACK },
  { "areas",				0, NULL, CMD_AREAS },
  { "set-area-soft",			1, NULL, CMD_SET_AREA_SOFT },
  { "set-area-hard",			1, NULL, CMD_SET_AREA_HARD },
  { "set-area-plan",			1, NULL, CMD_SET_AREA_PLAN },
  { "replace-init",			0, NULL, CMD_REPLACE_INIT },
  { "turn-to-zombie",			1, NULL, CMD_TURN_TO_ZOMBIE },
  { "all",				0, NULL, SEL_ALL },
  { "url",				0, NULL, SEL_URL },
  { "urls",				0, NULL, SEL_URLS },
  { "qkey",				0, NULL, SEL_QKEY },
  { "fp",				0, NULL, SEL_FP },
  { "norm-fp",				0, NULL, SEL_NORM_FP },
  { "fps",				0, NULL, SEL_FPS },
  { "only-types",			0, NULL, SEL_ONLY_TYPES },
  { "only-flags",			0, NULL, SEL_ONLY_FLAGS },
  { "only-older",			0, NULL, SEL_ONLY_OLDER },
  { "only-newer",			0, NULL, SEL_ONLY_NEWER },
  { "only-area",			0, NULL, SEL_ONLY_AREA },
  { "area",				0, NULL, SEL_AREA },
  { "verbose",				0, NULL, OPT_VERBOSE },
  { "bare",				0, NULL, OPT_BARE },
  { "current",				0, NULL, OPT_CURRENT },
  { "closed",				0, NULL, OPT_CLOSED },
  { "days",				0, NULL, OPT_DAYS },
  { "multi-age",			0, NULL, OPT_MULTI_AGE },
  { "server",				1, NULL, OPT_SERVER },
  { "show-sites",			0, NULL, OPT_SHOW_SITES },
  { "show-plan",			0, NULL, OPT_SHOW_PLAN },
  { "show-lastmod",			0, NULL, OPT_SHOW_LASTMOD },
  { NULL,				0, NULL, 0 },
};

static void
set_command_excl(enum command cmd, enum sel_type sel, int rw)
{
  if (command)
    usage("Multiple commands are not allowed");
  command = cmd;
  if (rw >= 0)
    command_is_rw = rw;
  else
    command_is_remote = 1;
  ASSERT(!selector);
  selector = selector_init(sel);
  selector->areas_rw = command_is_rw;
  selector->need_all = command_is_rw;
}

static void
set_command_set(int insertable)
{
  if ((command == CMD_INSERT || command == CMD_INSERT_REFS) && insertable)
    return;
  set_command_excl(CMD_SET, ST_INDEX_ENTRIES, 1);
}

static int
parse_set(int low, int high)
{
  char *e;
  long int x = strtol(optarg, &e, 0);
  if (e && *e || x < low || x > high)
    usage("Argument `%s' out of range", optarg);
  return x;
}

static void
parse_set_flag(uns flag)
{
  set_command_set(1);
  int val = optarg ? parse_set(0, 1) : 1;
  man_opt.set_flag_mask |= flag;
  if (val)
    man_opt.set_flag_val |= flag;
  else
    man_opt.set_flag_val &= ~flag;
}

static uns remote_command;
static uns remote_arg;
static byte *remote_phase;

static void
parse_remote_set(uns shepp_cmd)
{
  set_command_excl(CMD_REMOTE_SET, ST_UNDEF, -1);
  remote_command = shepp_cmd;
  remote_arg = optarg ? parse_set(0, 1) : 1;
}

static void
parse_remote_set_del_old(void)
{
  set_command_excl(CMD_REMOTE_SET, ST_UNDEF, -1);
  remote_command = SHEPP_REQ_SET_DELETE_OLD;
  remote_arg = optarg ? parse_set(0, 2) : 2;
}

static void
parse_borrow(uns shepp_cmd)
{
  set_command_excl(CMD_BORROW, ST_UNDEF, -1);
  remote_command = shepp_cmd;
  remote_phase = optarg;
}

static void
parse_command(enum command opt)
{
  switch (opt)
    {
    case CMD_HELP:
      usage(NULL);
      break;
    case CMD_LIST_IDX:
      set_command_excl(CMD_LIST, ST_INDEX_ENTRIES, 0);
      man_opt.show_idx = 2;
      break;
    case CMD_LIST_URLS:
      set_command_excl(CMD_LIST, ST_INDEX_ENTRIES, 0);
      man_opt.show_urls = 1;
      break;
    case CMD_LIST_FP:
      set_command_excl(CMD_LIST, ST_INDEX_ENTRIES, 0);
      man_opt.show_idx = 1;
      break;
    case CMD_LIST_ALL:
      set_command_excl(CMD_LIST, ST_INDEX_ENTRIES, 0);
      man_opt.show_idx = 2;
      man_opt.show_urls = 1;
      break;
    case CMD_BUCKETS:
    case CMD_HISTOGRAM:
    case CMD_REFRESH_STATS:
      set_command_excl(opt, ST_INDEX_ENTRIES, 0);
      break;
    case CMD_INSERT_REFS:
    case CMD_DELETE:
      set_command_excl(opt, ST_INDEX_ENTRIES, 1);
      break;
    case CMD_INSERT:
      set_command_excl(CMD_INSERT, ST_CONTRIB, 1);
      break;
    case CMD_SITES:
    case CMD_SITES_FILTER:
    case CMD_QKEY_STATS:
      set_command_excl(opt, ST_SITES, 0);
      break;
    case CMD_SET_WEIGHT:
      set_command_set(1);
      man_opt.set_weight = parse_set(0, 255);
      man_opt.set_flag_mask |= USF_TRUE_WEIGHT;
      man_opt.set_flag_val &= ~USF_TRUE_WEIGHT;
      break;
    case CMD_SET_TRUE_WEIGHT:
      set_command_set(1);
      man_opt.set_weight = parse_set(0, 255);
      man_opt.set_flag_mask |= USF_TRUE_WEIGHT;
      man_opt.set_flag_val |= USF_TRUE_WEIGHT;
      break;
    case CMD_SET_FREQUENCY:
      set_command_set(0);
      man_opt.set_freq = parse_set(0, 255);
      break;
    case CMD_SET_SECTION:
      set_command_set(1);
      man_opt.set_section = parse_set(0, 255);
      break;
    case CMD_SET_INIT:
      parse_set_flag(USF_INIT);
      break;
    case CMD_SET_NEEDED:
      parse_set_flag(USF_NEEDED_BY_EQ);
      break;
    case CMD_SET_REGATHER:
      parse_set_flag(USF_REGATHER);
      break;
    case CMD_SET_UNREF:
      parse_set_flag(USF_UNREF);
      break;
    case CMD_FILTER:
      set_command_excl(opt, ST_FILTERING, 0);
      break;
    case CMD_REMOTE_CLEANUP:
      parse_remote_set(SHEPP_REQ_SET_CLEANUP);
      break;
    case CMD_REMOTE_IDLE:
      parse_remote_set(SHEPP_REQ_SET_IDLE);
      break;
    case CMD_REMOTE_PRIVATE:
      parse_remote_set(SHEPP_REQ_SET_PRIVATE);
      break;
    case CMD_REMOTE_DELETE_OLD:
      parse_remote_set_del_old();
      break;
    case CMD_REMOTE_UNLOCK:
      set_command_excl(CMD_REMOTE_UNLOCK, ST_UNDEF, -1);
      break;
    case CMD_BORROW_STATE:
      parse_borrow(SHEPP_REQ_BORROW_STATE);
      break;
    case CMD_BORROW_QUICK:
      parse_borrow(SHEPP_REQ_BORROW_STATE_Q);
      break;
    case CMD_BORROW_RETURN:
      parse_borrow(SHEPP_REQ_RETURN_STATE);
      break;
    case CMD_BORROW_ROLLBACK:
      parse_borrow(SHEPP_REQ_ROLLBACK_STATE);
      break;
#ifdef CONFIG_AREAS
    case CMD_SET_AREA:
      set_command_set(1);
      man_opt.set_area = parse_set(0, 0x7fffffff);
      break;
    case CMD_AREAS:
      set_command_excl(opt, ST_AREAS, 0);
      break;
    case CMD_SET_AREA_SOFT:
      if (command != CMD_SET_AREA_PARAMS)
	set_command_excl(CMD_SET_AREA_PARAMS, ST_AREAS, 1);
      man_opt.set_area_soft = parse_set(0, 0x7fffffff);
      break;
    case CMD_SET_AREA_HARD:
      if (command != CMD_SET_AREA_PARAMS)
	set_command_excl(CMD_SET_AREA_PARAMS, ST_AREAS, 1);
      man_opt.set_area_hard = parse_set(0, 0x7fffffff);
      break;
    case CMD_SET_AREA_PLAN:
      if (command != CMD_SET_AREA_PARAMS)
	set_command_excl(CMD_SET_AREA_PARAMS, ST_AREAS, 1);
      man_opt.set_area_plan = parse_set(0, 0x7fffffff);
      break;
#else
    case CMD_SET_AREA:
    case CMD_AREAS:
    case CMD_SET_AREA_SOFT:
    case CMD_SET_AREA_HARD:
    case CMD_SET_AREA_PLAN:
      usage("Areas are not available, compile Sherlock with CONFIG_AREAS first");
#endif
    case CMD_REPLACE_INIT:
      set_command_excl(CMD_REPLACE_INIT, ST_URL_HASH, 1);
      break;
    case CMD_TURN_TO_ZOMBIE:
      set_command_excl(opt, ST_INDEX_ENTRIES, 1);
      opt_zombie_error = parse_set(2000, 2999);
      break;
    default:
      ASSERT(0);
    }

  if (!state_name_parsed)
    {
      state_name_parsed = 1;
      if (command_is_remote)
	{
	  if (man_opt.state || opt_current || opt_closed)
	    usage("Remote commands do not accept a state");
	}
      else
	{
	  if (man_opt.state && (opt_current || opt_closed) || (opt_current && opt_closed))
	    usage("Multiple states specified");
	  if (!man_opt.state)
	    {
	      byte *sn;
	      if (opt_closed)
		sn = "/closed";
	      else if (opt_current || !command_is_rw)
		sn = "/current";
	      else
		usage("There is no default state for r/w operations");
	      man_opt.state = mp_strcat(cf_pool, state_dir, sn);
	      if (man_opt.verbose)
		log(L_INFO, "Working on state %s", man_opt.state);
	    }
	}
    }
}

static enum sel_opt last_sel_opt_type;
static uns sel_opt_count;

static void
parse_selector(enum sel_opt opt)
{
  last_sel_opt_type = opt;
  char *err;
  if (err = selector_opt(selector, last_sel_opt_type, NULL))
    usage(err);
}

static void
check_selector(void)
{
  if (command_is_remote)
    return;
  if (!sel_opt_count && command_is_rw)
    usage("Nothing selected");
  byte *err = selector_check(selector);
  if (err)
    usage(err);
}

static void
parse_nonoption(void)
{
  if (!selector || !last_sel_opt_type)
    {
      if (command)
	usage("Selector expected after command");
      if (man_opt.state)
	usage("Multiple state names are not allowed");
      man_opt.state = optarg;
      return;
    }

  sel_opt_count++;
  byte *err;
  if (!strcmp(optarg, "-"))
    {
      byte line[2048];
      struct fastbuf *b = bfdopen_shared(0, 65535);
      while (bgets(b, line, sizeof(line)))
        {
	  if (unlikely(!!(err = selector_opt(selector, last_sel_opt_type, line))))
	    usage(err);
	}
      bclose(b);
    }
  else if (unlikely(!!(err = selector_opt(selector, last_sel_opt_type, optarg))))
    usage(err);
}

static void
parse_option(enum opt_opt opt)
{
  switch (opt)
    {
    case OPT_VERBOSE:
      man_opt.verbose = 1;
      break;
    case OPT_BARE:
      man_opt.bare = 1;
      break;
    case OPT_DAYS:
      man_opt.age_display_unit = 86400;
      break;
    case OPT_SHOW_SITES:
      man_opt.show_sites = 1;
      break;
    case OPT_MULTI_AGE:
      man_opt.multi_age = 1;
      break;
    case OPT_SHOW_PLAN:
      man_opt.show_plan = 1;
      break;
    case OPT_SHOW_LASTMOD:
      man_opt.show_lastmod = 1;
      break;
    case OPT_CLOSED:
      if (command)
	usage("--closed must precede command");
      opt_closed++;
      break;
    case OPT_CURRENT:
      if (command)
	usage("--current must precede command");
      opt_current++;
      break;
    case OPT_SERVER:
      man_opt.server = optarg;
      break;
    default:
      ASSERT(0);
    }
}

int
main(int argc, char **argv)
{
  int opt;

  log_init(argv[0]);

  while ((opt = cf_getopt(argc, argv, SHORT_OPTS, long_opts, NULL)) > 0)
    {
      man_init();
      if (opt < 0x100)
	{
	  uns i;
	  for (i=0; i<ARRAY_SIZE(short_opts); i++)
	    if (short_opts[i].opt == opt)
	      break;
	  if (i < ARRAY_SIZE(short_opts))
	    opt = short_opts[i].code;
	}
      if (opt >= OPT_FIRST)
	parse_option(opt);
      else if (opt >= SEL_FIRST)
	parse_selector(opt);
      else if (opt >= CMD_FIRST)
	parse_command(opt);
      else if (opt == 1)
	parse_nonoption();
      else
	usage(NULL);
    }

  if (command == CMD_UNDEF)
    usage("Missing command");
  if (!man_opt.verbose)
    sorter_trace = 0;
  check_selector();

  switch (command)
    {
    case CMD_LIST:
      cmd_list(selector);
      break;
    case CMD_SET:
      cmd_set(selector);
      break;
    case CMD_DELETE:
      cmd_delete(selector);
      break;
    case CMD_INSERT:
      cmd_insert(selector);
      break;
    case CMD_INSERT_REFS:
      cmd_insert_refs(selector);
      break;
    case CMD_SITES:
      cmd_sites(selector);
      break;
    case CMD_SITES_FILTER:
      cmd_sites_filter(selector);
      break;
    case CMD_BUCKETS:
      cmd_buckets(selector);
      break;
    case CMD_HISTOGRAM:
      cmd_histogram(selector);
      break;
    case CMD_QKEY_STATS:
      cmd_qkey_stats(selector, 0);
      break;
    case CMD_REFRESH_STATS:
      cmd_qkey_stats(selector, 1);
      break;
    case CMD_FILTER:
      /* Everything is done inside the selector */
      break;
    case CMD_REMOTE_SET:
      cmd_remote_set(remote_command, remote_arg);
      break;
    case CMD_REMOTE_UNLOCK:
      cmd_remote_unlock();
      break;
    case CMD_BORROW:
      cmd_borrow(remote_command, remote_phase);
      break;
#ifdef CONFIG_AREAS
    case CMD_AREAS:
      cmd_areas(selector);
      break;
    case CMD_SET_AREA_PARAMS:
      cmd_set_area_params(selector);
      break;
#endif
    case CMD_REPLACE_INIT:
      cmd_replace_init(selector);
      break;
    case CMD_TURN_TO_ZOMBIE:
      cmd_turn_to_zombie(selector, opt_zombie_error);
      break;
    default:
      die("Command not implemented yet");
    }

  if (selector)
    selector_cleanup(selector);
  return 0;
}
