/*
 *	Sherlock Shepherd -- Manual Control -- Sorted Textual Sources for Mixed Selectors
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "ucw/lfs.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/url.h"
#include "ucw/lizard.h"
#include "ucw/bbuf.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/man.h"

#include <string.h>
#include <unistd.h>
#include <fcntl.h>

/* See doc/file-formats for the format of the database. */

uns man_text_block_limit = 16 * 1024;

struct sel_text_hdr {
  u32 count;		/* number of records */
  u32 idx_count;	/* number of index entries */
  u64 idx_pos;		/* offset of the index (array of struct sel_text_idx) */
};

struct sel_text_block {
  u32 size;             /* size of the block in bytes (without the header) */
  u32 buf_size;		/* size of uncompressed objects in bytes */
};

struct sel_text_idx {
  struct footprint fp;	/* FP of the last record in the block */
  u64 pos;		/* offset of the block */
};

struct sel_text_src {
  struct sel_src src;
  byte *file_name;
  struct fastbuf *fb;
  struct lizard_buffer *lizard;
  bb_t bb;
  struct sel_text_block block_hdr;
  struct fastbuf block_fb;
  struct sel_text_record record;
  struct mempool *record_pool;
  struct sel_text_hdr hdr;
  uns idx_index;
  struct sel_text_idx *idx;
};

static uns
sel_text_read_record(struct sel_text_src *s)
{
  mp_flush(s->record_pool);
  if (!breadb(&s->block_fb, &s->record.fp, sizeof(s->record.fp)))
    return 0;
  s->record.o = obj_new(s->record_pool);
  obj_read(&s->block_fb, s->record.o);
  DBG("Reading record %08x%08x:%08x%08x", FP_QUAD(s->record.fp));
  return 1;
}

static void
sel_text_read_block(struct sel_text_src *s, uns index)
{
  struct sel_text_idx *idx = s->idx + index;
  ASSERT(index < s->hdr.idx_count);
  if (index == s->idx_index)
    {
      DBG("Rewound block %u", index);
      brewind(&s->block_fb);
      if (!sel_text_read_record(s))
	ASSERT(0);
      return;
    }
  s->idx_index = index;
  bsetpos(s->fb, idx->pos);
  breadb(s->fb, &s->block_hdr, sizeof(s->block_hdr));
  bb_grow(&s->bb, s->block_hdr.size);
  breadb(s->fb, s->bb.ptr, s->block_hdr.size);
  byte *buf = lizard_decompress_safe(s->bb.ptr, s->lizard, s->block_hdr.buf_size);
  if (!buf)
    die("Cannot decompress text block");
  fbbuf_init_read(&s->block_fb, buf, s->block_hdr.buf_size, 0);
  DBG("Decompressed block %u", index);
  if (!sel_text_read_record(s))
    ASSERT(0);
}

struct sel_text_record *
sel_text_find_first(struct sel_text_src *s)
{
  if (!s->hdr.count)
    return s->src.record = NULL;
  sel_text_read_block(s, 0);
  return s->src.record = &s->record;
}

struct sel_text_record *
sel_text_find_next(struct sel_text_src *s)
{
  if (unlikely(s->idx_index == ~0U))
    return sel_text_find_first(s);
  if (!sel_text_read_record(s))
    if (s->idx_index + 1 != s->hdr.idx_count)
      sel_text_read_block(s, s->idx_index + 1);
    else
      return s->src.record = NULL;
  return s->src.record = &s->record;
}

static int
sel_text_find_interval(struct sel_text_src *s, uns l, uns r, struct footprint *key)
{
  /* Binary search in the index */
  while (l < r)
    {
      uns m = (l + r) >> 1;
      if (fp_cmp(&s->idx[m].fp, key) < 0)
	l = m + 1;
      else
	r = m;
    }
  if (l == s->hdr.idx_count)
    return -1;

  /* Sequential search in the block */
  int c;
  sel_text_read_block(s, l);
  while ((c = fp_cmp(&s->record.fp, key)) < 0)
    sel_text_read_record(s);
  return c;
}

struct sel_text_record *
sel_text_find(struct sel_text_src *s, struct footprint *key, uns *gt)
{
  *gt = sel_text_find_interval(s, 0, s->hdr.idx_count, key);
  return s->src.record = (((int)*gt >= 0) ? &s->record : NULL);
}

struct sel_text_record *
sel_text_find_forward(struct sel_text_src *s, struct footprint *key, uns *gt)
{
  if (s->idx_index == ~0U)
    return sel_text_find(s, key, gt);

  /* Sequential search in the block */
  int c;
  while ((c = fp_cmp(&s->record.fp, key)) < 0)
  {
    if (!sel_text_read_record(s))
      {
	/* Binary search */
	c = sel_text_find_interval(s, s->idx_index + 1, s->hdr.idx_count, key);
	break;
      }
  }
  *gt = c;
  return s->src.record = ((c >= 0) ? &s->record : NULL);
}

void
sel_text_close(struct sel_text_src *s)
{
  bclose(s->fb);
  lizard_free(s->lizard);
  bb_done(&s->bb);
  mp_delete(s->record_pool);
}

static struct sel_text_src *
sel_text_do_open_file(byte *file_name, uns try_open)
{
  if (!file_name || !*file_name)
    if (try_open)
      return NULL;
    else
      die("Undefined path to a selector source");
  struct sel_text_src *s = xmalloc_zero(sizeof(*s));
  s->fb = bopen_try(file_name, O_RDONLY, 65536);
  if (!s->fb)
    {
      if (!try_open)
	die("Cannot open source %s", file_name);
      xfree(s);
      return NULL;
    }
  s->file_name = file_name;
  breadb(s->fb, &s->hdr, sizeof(s->hdr));
  ASSERT((u64)s->hdr.idx_count * sizeof(struct sel_text_idx) < ~0U);
  bsetpos(s->fb, s->hdr.idx_pos);
  s->idx = xmalloc(s->hdr.idx_count * sizeof(struct sel_text_idx));
  s->idx_index = ~0U;
  breadb(s->fb, s->idx, s->hdr.idx_count * sizeof(struct sel_text_idx));
  s->lizard = lizard_alloc();
  bb_init(&s->bb);
  s->record_pool = mp_new(4096);
  s->src.close = (void (*)(struct sel_src *src))sel_text_close;
  s->src.find_first = (void *(*)(struct sel_src *src))sel_text_find_first;
  s->src.find_next = (void *(*)(struct sel_src *src))sel_text_find_next;
  s->src.find = (void *(*)(struct sel_src *src, void *key, uns *gt))sel_text_find;
  s->src.find_forward = (void *(*)(struct sel_src *src, void *key, uns *gt))sel_text_find_forward;
  s->src.cmp = (int (*)(void *record, void *key))fp_cmp;
  DBG("Opened text source %s", file_name);
  return s;
}

struct sel_text_src *
sel_text_open_file(byte *file_name)
{
  return sel_text_do_open_file(file_name, 0);
}

struct sel_text_src *
sel_text_try_open_file(byte *file_name)
{
  return sel_text_do_open_file(file_name, 1);
}

static byte *
sel_text_urls_resolve(struct sel_src *src)
{
  return obj_find_aval(((struct sel_text_record *)src->record)->o, 'U');
}

struct sel_text_src *
sel_text_open_urls(byte *file_name)
{
  struct sel_text_src *src = sel_text_open_file(file_name);
  src->src.resolve = sel_text_urls_resolve;
  return src;
}

struct sel_text_writer {
  struct fastbuf *fb;
  struct footprint last_fp;
  struct fastbuf *block_fb;
  bb_t buf;
  bb_t lizard;
  bb_t idx;
  uns idx_size;
  uns block_limit;
  uns count;
  uns merge_dups;
};

struct sel_text_writer *
sel_text_create(void)
{
  struct sel_text_writer *s = xmalloc_zero(sizeof(*s));
  s->fb = bopen_tmp(65536);
  struct sel_text_hdr hdr;
  bzero(&hdr, sizeof(hdr));
  bwrite(s->fb, &hdr, sizeof(hdr));
  s->block_fb = fbgrow_create(24 * 1024);
  bb_init(&s->buf);
  bb_init(&s->lizard);
  bb_init(&s->idx);
  s->block_limit = man_text_block_limit;
  return s;
}

void
sel_text_merge_dups(struct sel_text_writer *s)
{
  s->merge_dups = 1;
}

static void
sel_text_flush(struct sel_text_writer *s)
{
  if (!btell(s->block_fb))
    return;

  struct sel_text_idx idx;
  memcpy(&idx.fp, &s->last_fp, sizeof(idx.fp));
  idx.pos = btell(s->fb);
  bb_grow(&s->idx, s->idx_size += sizeof(idx));
  memcpy(s->idx.ptr + s->idx_size - sizeof(idx), &idx, sizeof(idx));

  struct sel_text_block block;
  fbgrow_rewind(s->block_fb);
  block.buf_size = bfilesize(s->block_fb);
  bb_grow(&s->buf, block.buf_size + LIZARD_NEEDS_CHARS);
  breadb(s->block_fb, s->buf.ptr, block.buf_size);
  bb_grow(&s->lizard, block.buf_size * LIZARD_MAX_MULTIPLY + LIZARD_MAX_ADD);
  block.size = lizard_compress(s->buf.ptr, block.buf_size, s->lizard.ptr);
  bwrite(s->fb, &block, sizeof(block));
  bwrite(s->fb, s->lizard.ptr, block.size);

  fbgrow_reset(s->block_fb);
}

void
sel_text_write(struct sel_text_writer *s, struct footprint *fp, struct odes *o)
{
  ASSERT(fp_cmp(fp, &s->last_fp) >= 0);
  if (s->merge_dups && s->count && !fp_cmp(fp, &s->last_fp))
    return;
  s->last_fp = *fp;
  bwrite(s->block_fb, fp, sizeof(*fp));
  bput_object(s->block_fb, o);
  bputc(s->block_fb, '\n');
  s->count++;
  if (btell(s->block_fb) > s->block_limit)
    sel_text_flush(s);
}

struct fastbuf *
sel_text_finish(struct sel_text_writer *s)
{
  sel_text_flush(s);

  struct sel_text_hdr hdr;
  bzero(&hdr, sizeof(hdr));
  hdr.count = s->count;
  hdr.idx_count = s->idx_size / sizeof(struct sel_text_idx);
  hdr.idx_pos = btell(s->fb);
  bwrite(s->fb, s->idx.ptr, s->idx_size);
  brewind(s->fb);
  bwrite(s->fb, &hdr, sizeof(hdr));

  bb_done(&s->buf);
  bb_done(&s->lizard);
  bb_done(&s->idx);
  bclose(s->block_fb);
  struct fastbuf *fb = s->fb;
  xfree(s);
  return fb;
}

#ifdef TEST
#include "ucw/getopt.h"
#include <stdlib.h>

static struct sel_text_src *src;
static struct footprint fp, *fps;
static uns n = 1000, gt;

static void
check(struct sel_text_record *rec, uns i)
{
  rec == src->src.record || ASSERT(0);
  if (i != ~0U)
    (rec && !gt && !fp_cmp(&rec->fp, fps + i) && obj_find_anum(rec->o, 'i', ~0U) == i) || ASSERT(0);
  else if (!rec)
    fp_cmp(&fp, fps + n - 1) > 0 || ASSERT(0);
  else if ((i = obj_find_anum(rec->o, 'i', ~0U)) >= n)
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
  struct sel_text_writer *writer;
  struct fastbuf *fb;
  struct mempool *pool;
  struct odes *o;
  uns i;
  srand(time(NULL));

  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 || argc != optind + 1)
    die ("See source code for usage");
  byte *test = argv[optind];

  if (!strcmp(test, "empty"))
    {
      writer = sel_text_create();
      fb = sel_text_finish(writer);
      bflush(fb);
      src = sel_text_open_file(fb->name);
      sel_text_find_first(src) && ASSERT(0);
      sel_text_find_next(src) && ASSERT(0);
      random_footprint(&fp);
      sel_text_find(src, &fp, &gt) && ASSERT(0);
      sel_text_find_forward(src, &fp, &gt) && ASSERT(0);
      sel_text_close(src);
      bclose(fb);
    }
  else if (!strcmp(test, "random"))
    {
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
      writer = sel_text_create();
      pool = mp_new(4096);
      for (i = 0; i < n; i++)
        {
          mp_flush(pool);
          o = obj_new(pool);
          obj_add_attr(o, 'i', mp_printf(pool, "%u", i));
          sel_text_write(writer, fps + i, o);
        }
      mp_delete(pool);
      fb = sel_text_finish(writer);
      bflush(fb);
      src = sel_text_open_file(fb->name);
      check(sel_text_find_next(src), 0);
      check(sel_text_find_next(src), 1);
      check(sel_text_find(src, fps + 700, &gt), 700);
      check(sel_text_find(src, fps + 100, &gt), 100);
      check(sel_text_find_forward(src, fps + 300, &gt), 300);
      check(sel_text_find_forward(src, fps + 600, &gt), 600);
      gt = 0; check(sel_text_find_first(src), 0);
      for (i = 0; i < n; i += random_max(5))
	check(sel_text_find_forward(src, fps + i, &gt), i);
      for (i = 0; i < 100; i++)
        {
	  random_footprint(&fp);
	  check(sel_text_find(src, &fp, &gt), ~0U);
	}
      sel_text_close(src);
      bclose(fb);
      xfree(fps);
    }
  else
    die("Unknown test case");

  return 0;
}

#endif
