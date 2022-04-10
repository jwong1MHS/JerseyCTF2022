/*
 *	Sherlock Shepherd Daemon -- Dump Data Structures
 *
 *	(c) 2003--2007 Martin Mares <mj@ucw.cz>
 *	(c) 2004 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/string.h"
#include "sherlock/object.h"
#include "ucw/mempool.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/export.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

static char *state;
static char *force_file;
static struct fastbuf *output;

static uns resolve_urls;

static struct mempool *pool;
static struct buck2obj_buf *buck_buf;

static void
init_buckets(void)
{
  pool = mp_new(1<<14);
  buck_buf = buck2obj_alloc();
  bucket_open(0);
}

static void
done_buckets(void)
{
  bucket_close();
  buck2obj_free(buck_buf);
  mp_delete(pool);
}

static struct fastbuf *
dump_read_state_file(byte *name)
{
  if (force_file)
    return bopen(force_file, O_RDONLY, 65536);
  else
    return read_state_file(state, name);
}

static void
resolve_url(oid_t oid)
{
  struct obuck_context ctx;
  ctx.hdr.oid = oid;
  obuck_find_by_oid(&bucket_file, &ctx, 0);
  struct fastbuf *b = obuck_fetch(&bucket_file, &ctx);
  mp_flush(pool);
  uns body;
  struct odes *o = obj_read_bucket(buck_buf, pool, ctx.hdr.type, ctx.hdr.length, b, &body, 1);
  byte *U = o ? obj_find_aval(o, 'U') : NULL;
  if (U)
    bputs(output, U);
  else
    bputc(output, '?');
  bclose(b);
}

static void
dump_index(byte *name)
{
  struct fastbuf *idx = dump_read_state_file(name);
  struct fastbuf *contrib = NULL;
  struct url_state s;

  if (resolve_urls)
    init_buckets();
  bputsn(output, "Site footprint   Path footprint   Bucket    LastTouch Stb Frq Tim Rtr Wei Sec   Area Flags        Type");
  while (breadb(idx, &s, sizeof(s)))
    {
      static const char *type_names[] = URL_STATE_TYPE_NAMES;
      byte flags[sizeof(URL_STATE_ALL_FLAG_NAMES)];
      bprintf(output, "%08x%08x:%08x%08x %08x %10u %3d %3d %3d %3d %3d %3d %6d %s %-3s ",
	      FP_QUAD(s.fp),
	      s.oid,
	      s.last_seen,
	      s.stable_time,
	      s.refresh_freq,
	      s.download_time,
	      s.retry_count,
	      s.weight,
	      s.section,
#ifdef CONFIG_AREAS
	      s.area,
#else
	      0,
#endif
	      str_format_flags(flags, URL_STATE_ALL_FLAG_NAMES, ustate_all_flags(&s)),
	      (ustate_type(&s) < ARRAY_SIZE(type_names) ? type_names[ustate_type(&s)] : "???"));
      if (resolve_urls)
	{
	  if (ustate_type(&s) == UTYPE_SKEY)
	    {
	      u32 k = s.oid;
	      bprintf(output, "%d.%d.%d.%d", (k >> 24) & 0xff, (k >> 16) & 0xff, (k >> 8) & 0xff, k & 0xff);
	    }
	  else if (ustate_type(&s) == UTYPE_ZOMBIE)
	    bputc(output, '-');
	  else if (s.flags & USF_CONTRIB)
	    {
	      if (!contrib)
		contrib = read_state_file(state, "contrib");
	      bsetpos(contrib, (ucw_off_t)s.oid << CONTRIB_SHIFT);
	      struct contrib c;
	      if (!breadb(contrib, &c, sizeof(c)))
		bputs(output, "?");
	      else
		bbcopy(contrib, output, c.url_len);
	    }
	  else if (s.oid < OID_ERROR)
	    resolve_url(s.oid);
	  else
	    bputc(output, '-');
	}
      bputc(output, '\n');
    }
  if (resolve_urls)
    done_buckets();
  if (contrib)
    bclose(contrib);
  bclose(idx);
}

static void
dump_sites(void)
{
  struct fastbuf *s = dump_read_state_file("sites");
  struct site_list_entry sle;

  bprintf(output, "Site footprint   Norm. footprint  Active Inact. Gathrd Oscill  Fresh SKey     AvT ErrCyc Name\n");
  if (bgetl(s) != SITE_LIST_MAGIC)
    die("Insufficient level of magic");
  while (breadb(s, &sle, sizeof(sle)))
    {
      byte name[MAX_URL_SIZE];
      bgets0(s, name, sizeof(name));
      bprintf(output, "%08x%08x %08x%08x %6d %6d %6d %6d %6d %08x %3d %6d %s://%s:%d/\n",
	      FP_PAIR(sle.fp),
	      FP_PAIR(sle.norm_fp),
	      sle.num_active, sle.num_inactive, sle.num_gathered, sle.num_oscillations, sle.num_fresh,
	      sle.skey, sle.avg_download_time, sle.error_cycles, url_proto_names[sle.proto], name, sle.port);
    }
  bclose(s);
}

static void
dump_contrib(void)
{
  struct fastbuf *f = dump_read_state_file("contrib");
  struct contrib c;

  bputsn(output, "Site footprint   Path footprint   Wei Sec   Area Fl URL");
  while (breadb(f, &c, sizeof(c)))
    {
      byte url[MAX_URL_SIZE];
      url[breadb(f, url, c.url_len)] = 0;
      bprintf(output, "%08x%08x:%08x%08x %3d %3d %6d %02x %s\n",
	      FP_QUAD(c.fp), c.weight, c.section,
#ifdef CONFIG_AREAS
	      c.area,
#else
	      0,
#endif
	      c.flags, url);
      uns len = sizeof(c) + c.url_len;
      bskip(f, ALIGN_TO(len, CONTRIB_ALIGN) - len);
    }
  bclose(f);
}

static void
dump_plan(void)
{
  struct fastbuf *f = dump_read_state_file("plan");
  struct plan_site_entry e;

  while (breadb(f, &e, sizeof(e)))
    {
      bprintf(output, "Site: qkey=%04x:%08x:%d robots=%08x delay=%d entries=%d\n",
	      QK_TRIPLE(e.qkey), e.robot_oid, e.delay, e.entry_count);
      uns cnt = e.entry_count;
      while (cnt--)
	{
	  struct plan_entry p;
	  breadb(f, &p, sizeof(p));
	  byte flags[9];
	  str_format_flags(flags, PLAN_ENTRY_FLAG_NAMES, p.flags);
	  bprintf(output, "\t%08x pri=%10u rtr=%3d wei=%3d flg=%s sec=%3d area=%6d\n",
		  p.oid, p.priority, p.retry_count, p.weight, flags, p.section,
#ifdef CONFIG_AREAS
		  p.area
#else
		  0
#endif
		  );
	}
    }
  bclose(f);
}

static void
dump_filtered_sites(void)
{
  site_hash_init(NULL);
  site_hash_load(state, SITE_HASH_FILTER);
  bprintf(output, "Site footprint   SoftLim HardLim FrshLim MinDly QBonus SBonus Fil Name\n");
  struct site *s = NULL;
  while (s = site_next(s))
    bprintf(output, "%08x%08x %7d %7d %6d %6d %6d %6d %s %s://%s:%d/\n", FP_PAIR(s->fp),
	    s->soft_limit, s->hard_limit, s->fresh_limit, s->min_delay, s->queue_bonus, s->select_bonus,
	    (s->flags & SITE_REJECTED) ? "rej" : "acc", url_proto_names[s->proto], s->hostname, s->port);
}

static void
resolve_index(void)
{
  struct fastbuf *in = bfdopen_shared(0, 65536);
  byte line[1024];
  uns id;

  init_buckets();
  while (bgets(in, line, sizeof(line)))
    {
      int l = strlen(line);
      bwrite(output, line, l);
      if (l >= 42 && line[16] == ':' && line[33] == ' ' &&
	  strncmp(line+l-4, "QKY", 3) &&
	  sscanf(line+34, "%x", &id) == 1)
	{
	  bputc(output, ' ');
	  resolve_url(id);
	}
      bputc(output, '\n');
    }
  done_buckets();
  bclose(in);
}

static void
hash_urls(void)
{
  struct fastbuf *in = bfdopen_shared(0, 65536);
  byte line[1024];

  while (bgets(in, line, sizeof(line)))
    {
      struct footprint fp;
      int err = url_footprint(&fp, line);
      if (err)
	fprintf(stderr, "%s: %s\n", line, url_error(err));
      else
	printf("%08x%08x:%08x%08x\n", FP_QUAD(fp));
    }
  bclose(in);
}

static void
dump_exports(void)
{
  struct fastbuf *f = dump_read_state_file("exports");
  struct export_entry e;

  bputsn(output, "OID      LastTouch Strip Weight");
  while (breadb(f, &e, sizeof(e)))
    bprintf(output, "%08x %9d %5d %6d\n", (uns)e.oid, e.last_checked_time & ~1U, e.last_checked_time & 1, e.weight);
  bclose(f);
}

static void
dump_checkpoints(void)
{
  struct fastbuf *f = dump_read_state_file("checkpoints");
  struct checkpoint_entry c;
  uns len;

  bputsn(output, "Time             Buckets    Journal    Contrib         Urls");
  while (len = bread(f, &c, sizeof(c)))
    {
      if (len < sizeof(c))
	bputsn(output, "<INCOMPLETE>");
      else
	bprintf(output, "%9d %13lld %10lld %10lld %12lld\n", c.time, (long long)c.buckets_pos, (long long)c.journal_pos, (long long)c.contrib_pos, (long long)c.urls_pos);
    }
  bclose(f);
}

static void
dump_log(void)
{
  struct fastbuf *f = dump_read_state_file("state-log");
  struct state_log_entry e;

  bputsn(output, "Site footprint   Path footprint   Src Act Ar1 Ar2");
  while (breadb(f, &e, sizeof(e)))
    bprintf(output, "%08x%08x:%08x%08x %3d %3d %3d %3d\n",
	    FP_QUAD(e.fp), e.source, e.action, e.arg1, e.arg2);
  bclose(f);
}

static void
dump_urls(void)
{
  struct url_db *db = url_db_open_file(force_file ? : url_database_file, O_RDONLY, 0);

  bputsn(output, "Bucket   Site footprint   Path footprint   URL");
  for (struct url_record *r = url_db_find_first(db); r; r = url_db_find_next(db))
    bprintf(output, "%08x %08x%08x:%08x%08x %s\n", r->oid, FP_QUAD(r->fp), r->url);
  url_db_close(db);
}

static void
dump_params(void)
{
  struct fastbuf *f = dump_read_state_file("params");
  struct state_params par;

  bzero(&par, sizeof(par));		// Short reads are OK in case of older versions
  if (bread(f, &par, sizeof(par)) < sizeof(par))
    bputsn(output, "### WARNING: Short read, probably an older version");
  bclose(f);

  if (par.params_magic != PARAMS_MAGIC)
    die("Invalid magic number: %08x", par.params_magic);
  bprintf(output, "Format version: %08x\n", par.format_version);
  byte flags[33];
  bprintf(output, "Flags: %s\n", str_format_flags(flags, STATE_FLAG_NAMES, par.flags));
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-dump [<options>] <command> [<state>]\n\
\n\
Options:\n"
CF_USAGE
"-u, --urls\t\tResolve full URL's (slow)\n\
\n\
Commands:\n\
-k, --checkpoint\tDump the checkpoints\n\
-c, --contrib\t\tDump the contributions\n\
-x, --exports\t\tDump the export list\n\
-f, --file=<file>\tForce dumping of a given file outside state directory\n\
-h, --hash\t\tDump footprints (hashes) of given URL's\n\
-i, --index\t\tDump the index\n\
-j, --journal\t\tDump the journal\n\
-l, --log\t\tDump the state log\n\
-P, --params\t\tDump state parameters\n\
-p, --plan\t\tDump the gathering plan\n\
-r, --resolve\t\tResolve URL's in --index output given on stdin\n\
-s, --sites\t\tDump the site list\n\
-t, --filtered-sites\tDump the site list after filtering\n\
-d, --url-db\t\tDump URL database\n\
");
  exit(1);
}

static char *shortopts = CF_SHORT_OPTS "Pcf:hijklprstuxd";
static struct option longopts[] = {
  CF_LONG_OPTS
  { "checkpoints",	0, 0, 'k' },
  { "contrib",		0, 0, 'c' },
  { "exports",		0, 0, 'x' },
  { "file",		1, 0, 'f' },
  { "hash",		0, 0, 'h' },
  { "index",		0, 0, 'i' },
  { "journal",		0, 0, 'j' },
  { "log",		0, 0, 'l' },
  { "params",		0, 0, 'P' },
  { "plan",		0, 0, 'p' },
  { "resolve",		0, 0, 'r' },
  { "sites",		0, 0, 's' },
  { "filtered-sites",	0, 0, 't' },
  { "urls",		0, 0, 'u' },
  { "url-db",		0, 0, 'd' },
  { NULL,		0, 0, 0 }
};

int
main(int argc, char **argv)
{
  log_init(argv[0]);

  int opt, cmd = -1;
  int want_state = 0;
  while ((opt = cf_getopt(argc, argv, shortopts, longopts, NULL)) > 0)
    switch (opt)
      {
      case 'P':
      case 'c':
      case 'i':
      case 'j':
      case 'k':
      case 'l':
      case 'p':
      case 's':
      case 't':
      case 'x':
	want_state = 1;
	if (cmd >= 0)
	  usage();
	cmd = opt;
	break;
      case 'u':
	resolve_urls = 1;
	break;
      case 'f':
	force_file = optarg;
	break;
      case 'r':
      case 'h':
      case 'd':
	cmd = opt;
	break;
      default:
	usage();
      }

  if (want_state)
    {
      if (optind == argc)
	die("Don't know how to find the current state yet");
      else if (optind == argc-1)
	state = argv[optind];
      else
	usage();
    }
  else if (optind == argc)
    state = NULL;
  else
    usage();

  output = bfdopen_shared(1, 65536);
  switch (cmd)
    {
    case 'P':
      dump_params();
      break;
    case 'c':
      dump_contrib();
      break;
    case 'd':
      dump_urls();
      break;
    case 'h':
      hash_urls();
      break;
    case 'i':
      dump_index("index");
      break;
    case 'j':
      dump_index("journal");
      break;
    case 'k':
      dump_checkpoints();
      break;
    case 'l':
      dump_log();
      break;
    case 'p':
      dump_plan();
      break;
    case 'r':
      resolve_index();
      break;
    case 's':
      dump_sites();
      break;
    case 't':
      dump_filtered_sites();
      break;
    case 'x':
      dump_exports();
      break;
    default:
      usage();
    }
  bclose(output);

  return 0;
}
