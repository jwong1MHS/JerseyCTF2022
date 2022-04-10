/*
 *	Sherlock Shepherd -- Manual Control -- Sorted Binary Sources for Mixed Selectors
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/lfs.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/man.h"

#include <fcntl.h>
#include <errno.h>

uns man_binary_forward_limit = 65536;
uns man_binary_first_skip = 65536;
uns man_binary_split_limit = 65536;

struct sel_binary_src {
  struct sel_src src;
  byte *file_name;
  int fd;
  struct fastbuf *fb;
  ucw_off_t header_size;
  uns record_size;
  uns index;
  uns count;
  void *record;
  uns split_limit;
  uns forward_limit;
  uns first_skip;
};

static void
sel_binary_rewind(struct sel_binary_src *s)
{
  s->index = 0;
  brewind(s->fb);
  s->src.record = NULL;
}

void *
sel_binary_find_first(struct sel_binary_src *s)
{
  if (!s->count)
    return s->src.record = NULL;
  sel_binary_rewind(s);
  breadb(s->fb, s->record, s->record_size);
  s->index = 1;
  return s->src.record = s->record;
}

void *
sel_binary_find_next(struct sel_binary_src *s)
{
  if (s->index >= s->count)
    return s->src.record = NULL;
  breadb(s->fb, s->record, s->record_size);
  s->index++;
  return s->src.record = s->record;
}

static int
sel_binary_find_interval(struct sel_binary_src *s, uns l, uns r, void *key)
{
  /* Looks for the first record not less than <key> in the interval [l, r) and returns:
   *   < 0 -- not found (EOF reached)
   *   = 0 -- found an equal record
   *   > 0 -- found a greater record
   *
   * The right limit should be EOF or index a record greater than <key> */

  ASSERT(l <= r && r <= s->count);
  void *record = s->record;
  uns record_size = s->record_size;

  /* Binary search */
  while (l + s->split_limit < r)
    {
      uns m = (l + r) >> 1;
      if (unlikely(ucw_pread(s->fd, record, record_size, (ucw_off_t)m * record_size + s->header_size) < (int)record_size))
	die("Selector source: Short read");
      if (s->src.cmp(record, key) < 0)
	l = m + 1;
      else
	r = m;
    }

  /* Sequential search */
  if (l < r)
    {
      int c;
      bsetpos(s->fb, (ucw_off_t)l * record_size + s->header_size);
      s->index = l;
      do
        {
          breadb(s->fb, record, record_size);
          s->index++;
          c = s->src.cmp(record, key);
          if (c >= 0)
            return c;
        }
      while (s->index < s->count);
    }
  return -1;
}

void *
sel_binary_find(struct sel_binary_src *s, void *key, uns *gt)
{
  *gt = sel_binary_find_interval(s, 0, s->count, key);
  return s->src.record = (((int)*gt >= 0) ? s->record : NULL);
}

void *
sel_binary_find_forward(struct sel_binary_src *s, void *key, uns *gt)
{
  if (!s->index)
    return sel_binary_find(s, key, gt);

  void *record = s->record;
  uns record_size = s->record_size;

  /* Sequential search */
  int lim = s->forward_limit;
  for (;;)
    {
      int c = s->src.cmp(record, key);
      if (c >= 0)
        {
	  *gt = c;
	  return s->src.record = record;
	}
      if (s->index >= s->count)
	return s->src.record = NULL;
      if (--lim <= 0)
	break;
      breadb(s->fb, record, record_size);
      s->index++;
    }

  /* Exponential skips */
  uns skip = s->first_skip;
  uns l = s->index;
  uns r = s->count;
  ASSERT(l < r);
  while (l + 2 * skip < r)
    {
      uns m = l + skip;
      if (unlikely(ucw_pread(s->fd, record, record_size, (ucw_off_t)m * record_size + s->header_size) < (int)record_size))
	die("Selector source: Short read");
      if (s->src.cmp(record, key) < 0)
	l = m + 1;
      else
        {
	  r = m;
	  break;
	}
      skip *= 2;
    }

  /* Binary search */
  *gt = sel_binary_find_interval(s, l, r, key);
  return s->src.record = (((int)*gt >= 0) ? record : NULL);
}

void
sel_binary_close(struct sel_binary_src *s)
{
  bclose(s->fb);
  close(s->fd);
  xfree(s->record);
}

static struct sel_binary_src *
sel_binary_do_open_file(byte *file_name, uns record_size, uns header_size, uns try_open)
{
  if (!file_name || !*file_name)
    if (try_open)
      return NULL;
    else
      die("Undefined path to a selector source");
  struct sel_binary_src *s = xmalloc_zero(sizeof(*s));
  s->fd = ucw_open(file_name, O_RDONLY, 0666);
  if (s->fd < 0)
    if (try_open && errno == ENOENT)
      {
	xfree(s);
        return NULL;
      }
    else
      die("Selector source %s: open: %m", file_name);
  s->fb = bfdopen_shared(s->fd, 65536);
  s->file_name = file_name;
  ucw_off_t size = bfilesize(s->fb);
  ASSERT(size >= header_size);
  size -= header_size;
  ASSERT(!(size % record_size));
  s->count = size / record_size;
  s->record_size = record_size;
  s->header_size = header_size;
  s->src.close = (void (*)(struct sel_src *src))sel_binary_close;
  s->src.find_first = (void *(*)(struct sel_src *src))sel_binary_find_first;
  s->src.find_next = (void *(*)(struct sel_src *src))sel_binary_find_next;
  s->src.find = (void *(*)(struct sel_src *src, void *key, uns *gt))sel_binary_find;
  s->src.find_forward = (void *(*)(struct sel_src *src, void *key, uns *gt))sel_binary_find_forward;
  s->src.cmp = (int (*)(void *, void *))fp_cmp;
  s->record = xmalloc(record_size);
  s->forward_limit = MAX(1, man_binary_forward_limit / record_size);
  s->first_skip = MAX(1, man_binary_first_skip / record_size);
  s->split_limit = MAX(1, man_binary_split_limit / record_size);
  s->src.record = NULL;
  return s;
}

struct sel_binary_src *
sel_binary_open_file(byte *file_name, uns record_size, uns header_size)
{
  return sel_binary_do_open_file(file_name, record_size, header_size, 0);
}

struct sel_binary_src *
sel_binary_try_open_file(byte *file_name, uns record_size, uns header_size)
{
  return sel_binary_do_open_file(file_name, record_size, header_size, 1);
}

static void
sel_binary_index_dump(struct sel_src *src, struct fastbuf *out)
{
  struct url_state *s = src->record;
  byte buf[MAX_URL_SIZE];
  bputs(out, "Shepherd: ");

  if (man_opt.show_plan)
    list_plan_header(out);
  bputc(out, ' ');
  list_index_header(out);
  bputc(out, '\n');

  bputs(out, "          ");
  if (man_opt.show_plan)
    list_plan_entry(out, s);
  bputc(out, ' '), list_index_entry(out, s);
  if (resolve_non_url(buf, s))
    bprintf(out, " %s", buf);
  bputc(out, '\n');
}

struct sel_binary_src *
sel_binary_open_index(byte *file_name)
{
  struct sel_binary_src *src = sel_binary_open_file(file_name, sizeof(struct url_state), 0);
  src->src.dump = sel_binary_index_dump;
  return src;
}

uns
sel_binary_count(struct sel_binary_src *src)
{
  return src->count;
}

#ifdef TEST
#include "ucw/getopt.h"
#include <stdlib.h>
#include <string.h>

static struct footprint fp, *fps;
static struct sel_binary_src *src;
static uns n = 1000, gt;

static void
check(struct url_state *rec, uns i)
{
  rec == src->src.record || ASSERT(0);
  if (i != ~0U)
    (rec && !gt && !fp_cmp(&rec->fp, fps + i) && rec->oid == i) || ASSERT(0);
  else if (!rec)
    fp_cmp(&fp, fps + n - 1) > 0 || ASSERT(0);
  else if ((i = rec->oid) >= n)
    ASSERT(0);
  else if (!gt)
    fp_cmp(&fp, fps + i) && ASSERT(0);
  else if (i)
    (fp_cmp(&fp, fps + i) < 0 && fp_cmp(&fp, fps + i - 1) > 0) || ASSERT(0);
  else
    fp_cmp(&fp, fps + i) < 0 || ASSERT(0);
}

int main(int argc, char **argv)
{
  struct fastbuf *fb;
  uns i;
  srand(time(NULL));

  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 || argc != optind + 1)
    die("See source code for usage");
  byte *test = argv[optind];

  if (!strcmp(test, "empty"))
    {
      fb = bopen_tmp(4096);
      src = sel_binary_open_index(fb->name);
      sel_binary_find_first(src) && ASSERT(0);
      sel_binary_find_next(src) && ASSERT(0);
      random_footprint(&fp);
      sel_binary_find(src, &fp, &gt) && ASSERT(0);
      sel_binary_find_forward(src, &fp, &gt) && ASSERT(0);
      sel_binary_close(src);
      bclose(fb);
    }
  else if (!strcmp(test, "random"))
    {
      fb = bopen_tmp(4096);
      fps = xmalloc(n * sizeof(fp));
      do
        {
	  for (i = 0; i < n; i++)
	    random_footprint(fps + i);
	  footprint_array_sort(n, fps);
	  for (i = 1; i < n; i++)
	    if (!fp_cmp(fps + i, fps + i - 1))
	      break;
	}
      while (i < n);
      for (i = 0; i < n; i++)
        {
	  static struct url_state s;
	  s.oid = i;
	  s.fp = fps[i];
	  bwrite(fb, &s, sizeof(s));
	}
      bflush(fb);
      src = sel_binary_open_index(fb->name);
      check(sel_binary_find_next(src), 0);
      check(sel_binary_find_next(src), 1);
      check(sel_binary_find(src, fps + 700, &gt), 700);
      check(sel_binary_find(src, fps + 100, &gt), 100);
      check(sel_binary_find_forward(src, fps + 300, &gt), 300);
      check(sel_binary_find_forward(src, fps + 600, &gt), 600);
      gt = 0; check(sel_binary_find_first(src), 0);
      for (i = 0; i < n; i += random_max(5))
	check(sel_binary_find_forward(src, fps + i, &gt), i);
      for (i = 0; i < 100; i++)
        {
	  random_footprint(&fp);
	  check(sel_binary_find(src, &fp, &gt), ~0U);
	}
      sel_binary_close(src);
      bclose(fb);
      xfree(fps);
    }
  else
    die("Unknown test case");

  return 0;
}

#endif
