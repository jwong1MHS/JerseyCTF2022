/*
 *	Sherlock Shepherd Daemon -- URL Database
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"
#include "ucw/fastbuf.h"
#include "ucw/url.h"
#include "ucw/mempool.h"
#include "gather/shepherd/shepherd.h"

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>

/* See doc/file-formats for the format of the database. */

COMPILE_ASSERT(URL_DB_STRUCT_TEST, sizeof(struct url_record) == 24);

#define RECORD_MAX_SIZE ((uns)sizeof(struct url_record) + ALIGN_TO(MAX_URL_SIZE, 4))
#define SPLIT_LIMIT MAX(65536, 8 * RECORD_MAX_SIZE)
#define FORWARD_LIMIT (256 * 1024)
#define SKIP_LIMIT MAX(SPLIT_LIMIT, FORWARD_LIMIT)

void
url_record_decode(struct url_record *rec)
{
  *(byte *)&rec->oid          &= rec->flags | ~0x01;
  *(byte *)&rec->fp.site.x[0] &= rec->flags | ~0x02;
  *(byte *)&rec->fp.site.x[1] &= rec->flags | ~0x04;
  *(byte *)&rec->fp.rest.x[0] &= rec->flags | ~0x08;
  *(byte *)&rec->fp.rest.x[1] &= rec->flags | ~0x10;
}

static void
url_record_reencode(struct url_record *rec)
{
  *(byte *)&rec->oid          |= 0x01;
  *(byte *)&rec->fp.site.x[0] |= 0x02;
  *(byte *)&rec->fp.site.x[1] |= 0x04;
  *(byte *)&rec->fp.rest.x[0] |= 0x08;
  *(byte *)&rec->fp.rest.x[1] |= 0x10;
}

static void
url_record_encode(struct url_record *rec)
{
  rec->zero = 0;
  rec->flags =
    *(byte *)&rec->oid          & 0x01 |
    *(byte *)&rec->fp.site.x[0] & 0x02 |
    *(byte *)&rec->fp.site.x[1] & 0x04 |
    *(byte *)&rec->fp.rest.x[0] & 0x08 |
    *(byte *)&rec->fp.rest.x[1] & 0x10;
  url_record_reencode(rec);
}

static uns
url_record_check(struct url_record *rec)
{
  return 0x1f == ((rec->flags & ~0x1f) |
    (*(byte *)&rec->oid          & 0x01) |
    (*(byte *)&rec->fp.site.x[0] & 0x02) |
    (*(byte *)&rec->fp.site.x[1] & 0x04) |
    (*(byte *)&rec->fp.rest.x[0] & 0x08) |
    (*(byte *)&rec->fp.rest.x[1] & 0x10));
}

struct url_db {
  int fd;
  struct fastbuf *fb;
  ucw_off_t size;
  oid_t min_oid;		/* First available OID for writing */

  /* Buffer for the current record */
  struct url_record rec;	
  byte buf[MAX_URL_SIZE];	/* Extends rec.url zero-sized array */
};

static void
url_db_rewind(struct url_db *db)
{
  bsetpos(db->fb, sizeof(struct url_db_hdr));
}

void
url_db_sync(struct url_db *db)
{
  bfilesync(db->fb);
}

ucw_off_t
url_db_get_size(struct url_db *db)
{
  db->size = bfilesize(db->fb);
  if (db->size && ((db->size < (uns)sizeof(struct url_db_hdr)) || (db->size & 3)))
    die("URL database %s: Invalid size (%llx bytes)", db->fb->name, (unsigned long long) db->size);
  return db->size;
}

void
url_db_check_header(struct url_db_hdr *hdr)
{
  if (hdr->magic != URL_DB_MAGIC)
    die("Invalid format of URL database header");
  if (hdr->version != URL_DB_VERSION)
    die("Invalid version of URL database");
}

struct url_db *
url_db_open_file(byte *file_name, int mode, uns open_try)
{
  /* Open the file */
  if (!file_name || !*file_name)
    if (open_try)
      return NULL;
    else
      die("Undefined path to URL database");
  struct url_db *db = xmalloc_zero(sizeof(*db));
  db->fd = ucw_open(file_name, mode, 0666);
  if (db->fd < 0)
    if (open_try && db->fd == ENOENT)
      {
	xfree(db);
	return NULL;
      }
    else
      die("URL database %s: open: %m", file_name);
  db->fb = bopen(file_name, O_RDONLY, 65536);

  struct url_db_hdr hdr;
  if (!url_db_get_size(db))
    {
      /* Note: Database creation is not atomic */
      DBG("Creating a new URL database %s", file_name);
      bzero(&hdr, sizeof(hdr));
      hdr.magic = URL_DB_MAGIC;
      hdr.version = URL_DB_VERSION;
      hdr.time = time(NULL);
      ssize_t written = write(db->fd, &hdr, sizeof(hdr));
      if (written != sizeof(hdr))
        if (written < 0)
          die("URL database %s: write: %m", db->fb->name);
        else
          die("URL database %s: short write", db->fb->name);
      db->size = sizeof(hdr);
    }
  else
    {
      DBG("Opening URL database %s", file_name);
      breadb(db->fb, &hdr, sizeof(hdr));
      url_db_check_header(&hdr);
      if ((mode & ~(O_CREAT | O_TRUNC | O_APPEND)) != O_WRONLY)
        if (url_db_find_last(db))
          db->min_oid = db->rec.oid + 1;
      url_db_rewind(db);
    }
  return db;
}

struct url_db *
url_db_open(int mode, uns try_open)
{
  return url_db_open_file(url_database_file, mode, try_open);
}

void
url_db_close(struct url_db *db)
{
  bclose(db->fb);
  close(db->fd);
}

#define SORT_PREFIX(x) url_records_sort_##x
#define SORT_KEY struct url_record
#define SORT_DATA_SIZE(k) ALIGN_TO((k).len, 4)
#define SORT_UNIQUE
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

static int
url_records_sort_compare(struct url_record *a, struct url_record *b)
{
  int cmp = fp_cmp(&a->fp, &b->fp);
  if (cmp)
    return cmp;
  COMPARE(a->oid, b->oid);
  return 0;
}

static int
url_records_sort_read_key(struct fastbuf *fb, struct url_record *k)
{
  if (!breadb(fb, k, sizeof(*k)))
    return 0;
  url_record_decode(k);
  return 1;
}

static void
url_records_sort_write_key(struct fastbuf *fb, struct url_record *k)
{
  struct url_record k2 = *k;
  url_record_reencode(&k2);
  bwrite(fb, &k2, sizeof(*k));
}

#include "ucw/sorter/sorter.h"

struct fastbuf *
url_db_sort_records(struct fastbuf *fb)
{
  return url_records_sort_sort(fb, NULL);
}

void
url_db_write(struct url_db *db, uns oid, byte *url)
{
  byte buf[RECORD_MAX_SIZE];
  struct url_record *rec = (void *)buf;
  ASSERT(url && *url);
  ASSERT(oid >= db->min_oid);
  rec->len = strlen(url);
  rec->oid = oid;
  db->min_oid = oid + 1;
  int err = url_footprint(&rec->fp, url);
  ASSERT(!err);
  url_record_encode(rec);
  uns aligned = ALIGN_TO(rec->len, 4);
  *(u32 *)(buf + sizeof(*rec) + aligned - 4) = 0;
  memcpy(rec + 1, url, rec->len);
  uns bytes = sizeof(*rec) + aligned;
  ssize_t written = write(db->fd, buf, bytes);
  if (unlikely(written != (ssize_t)bytes))
    if (written < 0)
      die("URL database write: %m");
    else
      die("URL database: short write");
}

static struct url_record *
url_record_seek(byte *buf, byte *end)
{
  while (buf < end && *buf)
    buf += 4;
  return (void *)buf;
}

static struct url_record *
url_record_next(struct url_record *record)
{
  return (void *)(record + 1) + ALIGN_TO(record->len, 4);
}

struct url_record *
url_db_find_next(struct url_db *db)
{
  if (btell(db->fb) >= db->size)
    return NULL;
  breadb(db->fb, &db->rec, sizeof(db->rec));
  if (!url_record_check(&db->rec))
    die("URL database %s: Corrupted record %0llx", db->fb->name, (long long)(btell(db->fb) - sizeof(db->rec)));
  url_record_decode(&db->rec);
  breadb(db->fb, db->rec.url, ALIGN_TO(db->rec.len, 4));
  db->rec.url[db->rec.len] = 0;
  return &db->rec;
}

struct url_record *
url_db_find_first(struct url_db *db)
{
  url_db_rewind(db);
  return url_db_find_next(db);
}

struct url_record *
url_db_find_last(struct url_db *db)
{
  if (db->size == sizeof(struct url_db_hdr))
    return NULL;
  uns size = MIN(db->size - sizeof(struct url_db_hdr), RECORD_MAX_SIZE);
  byte buf[RECORD_MAX_SIZE], *end = buf + size;
  bsetpos(db->fb, db->size - size);
  breadb(db->fb, buf, size);
  struct url_record *next, *rec = url_record_seek(buf, end);
  if ((byte *)rec >= end)
    die("URL database %s: DB inconsistency found while seeking the last record", db->fb->name);
  while ((byte *)(next = url_record_next(rec)) < end)
    rec = next;
  if ((byte *)next != end || !url_record_check(rec))
    die("URL database %s: Corrupted record at %0llx", db->fb->name, (long long)(btell(db->fb) - (end - (byte *)rec)));
  memcpy(&db->rec, rec, end - (byte *)rec);
  url_record_decode(&db->rec);
  return &db->rec;
}

/* Search the interval [l, r) for the first record with OID not greater than <key>.
 * Interval limits must be aligned to record boundaries. <r> should be EOF or
 * position of a record with OID not lower than <key>.
 *
 * Retval: 0=found an equal record; 1=found a greater record; -1=EOF reached */
static int
url_db_find_interval(struct url_db *db, ucw_off_t l, ucw_off_t r, uns key)
{
  byte buf[2 * RECORD_MAX_SIZE], *end = buf + sizeof(buf);
  struct url_record *rec, *next;
  while (l + SPLIT_LIMIT < r)
    {
      ucw_off_t m = ((l + r) / 2) & ~(ucw_off_t)3;
      if (unlikely(ucw_pread(db->fd, buf, sizeof(buf), m) < (int)sizeof(buf)))
        die("URL database %s: short read", db->fb->name);
      rec = url_record_seek(buf, end);
      ASSERT((byte *)(rec + 1) < end);
      if (!url_record_check(rec))
	die("URL database %s: corrupted record at %llx", db->fb->name, (long long)(m + buf - (byte *)rec));
      url_record_decode(rec);
      next = url_record_next(rec);
      ASSERT((byte *)next <= end);
      if (rec->oid < key)
        l = m + ((byte *)next - buf);
      else
        r = m + ((byte *)rec - buf);
    }
  bsetpos(db->fb, l);
  while (url_db_find_next(db))
    if (db->rec.oid == key)
      return 0;
    else if (db->rec.oid > key)
      return 1;
  return -1;
}

struct url_record *
url_db_find(struct url_db *db, uns key, uns *gt)
{
  *gt = url_db_find_interval(db, sizeof(struct url_db_hdr), db->size, key);
  return ((int)*gt >= 0) ? &db->rec : NULL;
}

struct url_record *
url_db_find_forward(struct url_db *db, uns key, uns *gt)
{
  if (btell(db->fb) <= (ucw_off_t)sizeof(struct url_db_hdr))
    return url_db_find(db, key, gt);

  /* Sequential search */
  int forward_limit = FORWARD_LIMIT;
  for (;;)
    {
      if (db->rec.oid >= key)
        {
	  *gt = db->rec.oid > key;
	  return &db->rec;
	}
      if ((forward_limit -= sizeof(struct url_record) + ALIGN_TO(db->rec.len, 4)) <= 0)
	break;
      if (!url_db_find_next(db))
	return NULL;
    }

  /* Exponential skips */
  ucw_off_t skip = SKIP_LIMIT;
  ucw_off_t l = btell(db->fb);
  ucw_off_t r = db->size;
  if (l == r)
    return NULL;
  byte buf[2 * RECORD_MAX_SIZE], *end = buf + sizeof(buf);
  while (l + 2 * skip < r)
    {
      ucw_off_t m = l + skip;
      if (unlikely(ucw_pread(db->fd, buf, sizeof(buf), m) < (int)sizeof(buf)))
        die("URL database: short read");
      struct url_record *rec = url_record_seek(buf, end);
      ASSERT((byte *)(rec + 1) < end);
      url_record_decode(rec);
      struct url_record *next = url_record_next(rec);
      ASSERT((byte *)next <= end);
      if (rec->oid < key)
	l = m + (byte *)next - buf;
      else
        {
	  r = m + (byte *)rec - buf;
	  break;
	}
      skip *= 2;
    }

  /* Binary search */
  *gt = url_db_find_interval(db, l, r, key);
  return ((int)*gt >= 0) ? &db->rec : NULL;
}

ucw_off_t
url_db_tell(struct url_db *db)
{
  return btell(db->fb);
}

#ifdef TEST
#include "ucw/getopt.h"
#include <stdlib.h>
#include <stdio.h>

static void
random_url(byte *url)
{
  url += sprintf(url, "http://");
  for (uns i = random_max(100) + 10; i--; )
    *url++ = 'a' + random_max('z' - 'a');
  *url++ = '/';
  *url = 0;
}

static uns n = 1000, gt;
static struct url_db *db;

static void
check(struct url_record *rec, uns i)
{
  if (i <= ((n - 1) & ~1) && !(i & 1))
    (rec && !gt && rec->oid == i) || ASSERT(0);
  else if (i < ((n - 1) & ~1))
    (rec && gt && rec->oid == i + 1) || ASSERT(0);
  else
    rec && ASSERT(0);
  if (rec)
    if (rec->oid == 10)
      strcmp(rec->url, "http://www.ucw.cz/") && ASSERT(0);
    else if (rec->oid == 100)
      strcmp(rec->url, "http://www.centrum.cz/") && ASSERT(0);
}

int main(int argc, char **argv)
{
  byte name[TEMP_FILE_NAME_LEN];
  srand(time(NULL));

  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 || argc != optind + 1)
    die("See source code for usage");
  byte *test = argv[optind];
  // Generate file name and ensure the file is ours
  close(open_tmp(name, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR));

  if (!strcmp(test, "empty"))
    {
      db = url_db_open_file(name, O_RDWR | O_CREAT | O_TRUNC | O_APPEND, 0);
      url_db_close(db);
      db = url_db_open_file(name, O_RDONLY, 0);
      url_db_find_first(db) && ASSERT(0);
      url_db_find_last(db) && ASSERT(0);
      url_db_find_next(db) && ASSERT(0);
      url_db_find(db, 123, &gt) && ASSERT(0);
      url_db_find_forward(db, 234, &gt) && ASSERT(0);
      url_db_close(db);
    }
  else if (!strcmp(test, "random"))
    {
      db = url_db_open_file(name, O_RDWR | O_CREAT | O_TRUNC | O_APPEND, 0);
      for (uns i = 0; i < n; i += 2)
        {
          byte url_buf[MAX_URL_SIZE];
          byte *url;
          switch (i)
            {
	      case 10: url = "http://www.ucw.cz/"; break;
	      case 100: url = "http://www.centrum.cz/"; break;
              default: random_url(url_buf); url = url_buf; break;
            }
          url_db_write(db, i, url);
        }
      url_db_close(db);
      db = url_db_open_file(name, O_RDONLY, 0);
      check(url_db_find_next(db), 0);
      check(url_db_find_next(db), 2);
      check(url_db_find(db, 700, &gt), 700);
      check(url_db_find(db, 101, &gt), 101);
      check(url_db_find_forward(db, 300, &gt), 300);
      check(url_db_find_forward(db, 601, &gt), 601);
      gt = 0; check(url_db_find_last(db), (n - 1) & ~1);
      check(url_db_find_first(db), 0);
      for (uns i = 0; i < n + 100; i += random_max(5))
	check(url_db_find_forward(db, i, &gt), i);
      for (uns i = 0; i < 100; i++)
        {
	  uns j = random_max(n + 100);
	  check(url_db_find(db, j, &gt), j);
	}
      url_db_close(db);
    }
  else
    die("Unknown test case");

  if (unlink(name))
    die("unlink: %m");
  return 0;
}

#endif
