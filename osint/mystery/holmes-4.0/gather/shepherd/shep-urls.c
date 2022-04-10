/*
 *	Sherlock Shepherd Daemon -- Utility for Maniplutaion with URL Database
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/bucket.h"
#include "sherlock/object.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/chartype.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/man.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

static char *shortopts = "abcdDf:i:rs" CF_SHORT_OPTS;
static struct option longopts[] =
{
  CF_LONG_OPTS
  { "file",     	1, 0, 'f' },
  { "build",		0, 0, 'b' },
  { "append",   	0, 0, 'a' },
  { "sort",		0, 0, 's' },
  { "incremental",	1, 0, 'i' },
  { "check",    	0, 0, 'c' },
  { "dump",		0, 0, 'd' },
  { "dump-sorted",	0, 0, 'D' },
  { "resolve",		0, 0, 'r' },
  { NULL, 0, 0, 0 }
};

static char *help = "\
Usage: shep-urls [<options>]\n\
\n\
Options:\n"
CF_USAGE
"\
-f, --file=<path>         Force custom path to (sorted) URL database\n\
-b, --build               (Re)build URL database from bucket file\n\
-a, --append              Incremental build of URL database\n\
-s, --sort                Sort URL database by footprints\n\
-i, --incremental=<path>  Use this file as the previous database version for incremental sorting\n\
-c, --check               Check consistency\n\
-d, --dump                Dump URLs\n\
-D, --dump-sorted         Dump sorted URLs\n\
-r, --resolve             Resolve OID (-r 'xxxxxxx') from unsorted DB,\n\
                          footprint (-r 'xxxxxxxxxxxxxxxx:xxxxxxxxxxxxxxxx') or\n\
			  site print (-r 'xxxxxxxxxxxxxxxx:*') from sorted DB\n\
";

static void NONRET
usage(void)
{
  fputs(help, stderr);
  exit(1);
}

static char *force_file;
static uns want_build;
static uns want_append;
static uns want_sort;
static uns want_check;
static uns want_dump;
static uns want_dump_sorted;
static char *want_resolve;
static char *inc_file;

static struct fastbuf *output;

static struct url_db *
my_url_db_open(int mode)
{
  return url_db_open_file(force_file ? : url_database_file, mode, 0);
}

static void
append(void)
{
  log(L_INFO, "Appending to URL database");
  struct url_db *db = my_url_db_open(O_RDWR | O_APPEND);
  struct mempool *pool = mp_new(65536);
  struct buck2obj_buf *buck_buf = buck2obj_alloc();
  struct obuck_header bh;
  struct fastbuf *buck;
  struct odes *o;
  bucket_open(0);
  uns progress_cnt = 0, record_cnt = 0;
  oid_t progress_last_oid = obuck_predict_last_oid(&bucket_file);
  struct url_record *rec = url_db_find_last(db);
  oid_t first_oid = rec ? rec->oid + 1 : 0;

  while (buck = obuck_slurp_pool(&bucket_file, &bh, rec ? rec->oid : OBUCK_OID_ANY))
    {
      if (!(progress_cnt++ % 10000))
        setproctitle("shep-urls: %d buckets (%d%%) -> %d records", progress_cnt,
        progress_last_oid ? (int)((float)bh.oid / (float)progress_last_oid * 100) : 0,
        record_cnt);
      if (bh.oid < first_oid)
	continue;
      mp_flush(pool);
      o = obj_new(pool);
      uns body_start;
      if (buck2obj_parse(buck_buf, bh.type, bh.length, buck, o, &body_start, NULL, 1))
	continue;
      byte *url = obj_find_aval(o, 'U');
      ASSERT(url);
      url_db_write(db, bh.oid, url);
      record_cnt++;
    }

  bucket_close();
  buck2obj_free(buck_buf);
  mp_delete(pool);
  url_db_close(db);
  log(L_INFO, "Generated %u URL records from %u buckets", record_cnt, progress_cnt);
}

static void
build(void)
{
  log(L_INFO, "Creating a new URL database");
  struct url_db *db = my_url_db_open(O_CREAT | O_TRUNC | O_WRONLY | O_APPEND);
  url_db_close(db);
  append();
}

struct sorted_trailer {
  ucw_time_t time;
  u64 src_size;
};

static void
merge(struct sel_text_writer *dest, struct sel_text_src *src1, struct sel_text_src *src2)
{
  struct sel_text_record *rec1, *rec2;
  rec1 = sel_text_find_first(src1);
  rec2 = sel_text_find_first(src2);

  while (rec1 || rec2)
    {
      int c;
      if (!rec1)
	c = 1;
      else if (!rec2)
	c = -1;
      else
	c = fp_cmp(&rec1->fp, &rec2->fp);
      if (c <= 0)
        {
	  sel_text_write(dest, &rec1->fp, rec1->o);
	  rec1 = sel_text_find_next(src1);
	}
      else
        {
	  sel_text_write(dest, &rec2->fp, rec2->o);
	  rec2 = sel_text_find_next(src2);
	}
    }
}

static void
sort(void)
{
  if (!url_database_file || !*url_database_file || !url_sorted_file || !*url_sorted_file)
    die("URL database not configured");

  struct fastbuf *fb = bopen(url_database_file, O_RDONLY, 65536);
  struct url_db_hdr hdr;
  struct sorted_trailer tr;
  breadb(fb, &hdr, sizeof(hdr));
  url_db_check_header(&hdr);
  ucw_off_t src_size = bfilesize(fb);

  uns incr = 0;
  if (inc_file)
    {
      DBG("Requested incremental sort from %s", inc_file);
      struct fastbuf *inc = bopen(inc_file, O_RDONLY, 65536);
      bsetpos(inc, bfilesize(inc) - sizeof(tr));
      breadb(inc, &tr, sizeof(tr));
      bclose(inc);
      if (hdr.time == tr.time)
        {
	  log(L_INFO, "Preparing for incremental sort from position 0x%llx", (long long)tr.src_size);
	  bsetpos(fb, tr.src_size);
	  incr++;
	}
      else
	log(L_INFO, "Cannot sort incrementally, sorting everything");
    }

  log(L_INFO, "Sorting URLs");
  fb = url_db_sort_records(fb);

  log(L_INFO, "Applying block compression");
  struct sel_text_writer *writer = sel_text_create();
  sel_text_merge_dups(writer);
  struct url_record record;
  struct mempool *pool = mp_new(4096);
  struct odes *o;
  byte url[MAX_URL_SIZE];
  uns count = 0;
  while (breadb(fb, &record, sizeof(record)))
    {
      url_record_decode(&record);
      breadb(fb, url, ALIGN_TO(record.len, 4));
      url[record.len] = 0;
      mp_flush(pool);
      o = obj_new(pool);
      obj_set_attr(o, 'U', url);
      sel_text_write(writer, &record.fp, o);
      count++;
    }
  mp_delete(pool);
  fb = sel_text_finish(writer);
  log(L_INFO, "Processed %u new URLs", count);

  if (incr)
    {
      log(L_INFO, "Merging");
      bflush(fb);
      struct sel_text_src *src1 = sel_text_open_file(inc_file);
      struct sel_text_src *src2 = sel_text_open_file(fb->name);
      struct sel_text_writer *dest = sel_text_create();
      merge(dest, src1, src2);
      sel_text_close(src1);
      sel_text_close(src2);
      bclose(fb);
      fb = sel_text_finish(dest);
    }

  DBG("Writing trailer (time=%d size=%llx)", (int)hdr.time, (long long)src_size);
  bseek(fb, 0, SEEK_END);
  tr.time = hdr.time;
  tr.src_size = src_size;
  bwrite(fb, &tr, sizeof(tr));
  bfix_tmp_file(fb, url_sorted_file);
}

static void
check(void)
{
  log(L_INFO, "Checking URL database");
  struct url_db *db = my_url_db_open(O_RDONLY);
  oid_t min_oid = 0;
  uns cnt = 0;
  for (struct url_record *r = url_db_find_first(db); r; r = url_db_find_next(db))
    {
      if (unlikely(r->oid < min_oid))
	die("URL database: Non-increasing OID sequence in the record before %0llx", (long long)url_db_tell(db));
      min_oid = r->oid + 1;
      struct footprint fp;
      int err = url_footprint(&fp, r->url);
      if (unlikely(err))
	die("URL database: Invalid URL '%s' in the record before %0llx", r->url, (long long)url_db_tell(db));
      if (unlikely(fp_cmp(&fp, &r->fp)))
	die("URL database: Invalid footprint in the record before %0llx (URL '%s')", (long long)url_db_tell(db), r->url);
      cnt++;
    }
  log(L_INFO, "Found %u records", cnt);
  url_db_close(db);
}

static void
dump_header(void)
{
  bputsn(output, "Bucket   Site footprint   Path footprint   URL");
}

static void
dump_record(struct url_record *r)
{
  bprintf(output, "%08x %08x%08x:%08x%08x %s\n", r->oid, FP_QUAD(r->fp), r->url);
}

static void
dump_text_header(void)
{
  bputsn(output, "Site footprint   Path footprint   URL");
}

static void
dump_text_record(struct sel_text_record *r)
{
  bprintf(output, "%08x%08x:%08x%08x %s\n", FP_QUAD(r->fp), obj_find_aval(r->o, 'U') ? : (byte *)"");
}

static void
dump(void)
{
  struct url_db *db = my_url_db_open(O_RDONLY);
  dump_header();
  for (struct url_record *r = url_db_find_first(db); r; r = url_db_find_next(db))
    dump_record(r);
  url_db_close(db);
}

static void
dump_sorted(void)
{
  struct sel_text_src *src = sel_text_open_file(force_file ? : url_sorted_file);
  dump_text_header();
  for (struct sel_text_record *r = sel_text_find_first(src); r; r = sel_text_find_next(src))
    dump_text_record(r);
  sel_text_close(src);
}

static uns
parse_x8(byte *c)
{
  uns x = 0;
  for (uns i = 0; i < 8; i++)
    if (Cxdigit(c[i]))
      x = (x << 4) | Cxvalue(c[i]);
    else if (c[i])
      die("Invalid hexadecimal number");
    else
      die("Invalid number length");
  return x;
}

static void
resolve(void)
{
  struct url_db *db;
  struct url_record *r;
  struct sel_text_src *src;
  struct sel_text_record *t;
  byte *c = want_resolve;
  uns gt;

  if (strlen(want_resolve) < 16)
    {
      dump_header();
      uns oid = parse_x8(c);
      db = my_url_db_open(O_RDONLY);
      if ((r = url_db_find(db, oid, &gt)) && !gt)
        dump_record(r);
      url_db_close(db);
    }
  else if (c[16] != ':')
    die("Invalid footprint");
  else
    {
      dump_text_header();
      struct footprint fp;
      fp.site.x[0] = parse_x8(c);
      fp.site.x[1] = parse_x8(c + 8);
      src = sel_text_open_file(force_file ? : url_sorted_file);
      if (strlen(c) == 33)
        {
	  fp.rest.x[0] = parse_x8(c + 17);
	  fp.rest.x[1] = parse_x8(c + 25);
	  for (t = sel_text_find(src, &fp, &gt); t && !fp_cmp(&t->fp, &fp); t = sel_text_find_next(src))
	    dump_text_record(t);
	}
      else if (c[17] == '*' && !c[18])
        {
	  bzero(&fp.rest, sizeof(fp.rest));
	  for (t = sel_text_find(src, &fp, &gt); t && !site_fp_cmp(&t->fp.site, &fp.site); t = sel_text_find_next(src))
	    dump_text_record(t);
	}
      else
	die("Invalid footprint");
      sel_text_close(src);
    }
}

int
main(int argc, char **argv)
{
  int opt;
  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, shortopts, longopts, NULL)) >= 0)
    switch (opt)
      {
	case 'f':
	  force_file = optarg;
	  break;
        case 'b':
          want_build++;
          break;
	case 'a':
	  want_append++;
	  break;
	case 's':
	  want_sort++;
	  break;
	case 'i':
	  inc_file = optarg;
	  break;
	case 'c':
	  want_check++;
	  break;
	case 'd':
	  want_dump++;
	  break;
	case 'D':
	  want_dump_sorted++;
	  break;
	case 'r':
          want_resolve = "";
	  break;
        default:
          usage();
          break;
      }
  if (argc != optind + !!want_resolve || argc < 2)
    usage();
  if (want_resolve)
    want_resolve = argv[argc - 1];

  output = bfdopen_shared(1, 65536);
  if (want_build)
    build();
  else if (want_append)
    append();
  if (want_check)
    check();
  if (want_dump)
    dump();
  if (want_dump_sorted)
    dump_sorted();
  if (want_sort)
    sort();
  if (want_resolve)
    resolve();
  bclose(output);

  return 0;
}
