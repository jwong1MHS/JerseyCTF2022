/*
 *	Resolver for fingerprints
 *
 *	(c) 2003--2007, Robert Spalek <robert@ucw.cz>
 *	(c) 2008, Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/clists.h"
#include "ucw/workqueue.h"
#include "ucw/stkstring.h"
#include "ucw/sorter/common.h"
#include "indexer/indexer.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

/* Configuration */

static struct fastbuf *splits, *fps, *out, *resolve_in;
static struct card_print *table;
static uns resolve_record_size, resolve_in_size, resolve_flags, resolve_first_split;
static uns table_shift, table_mask;
static uns batch_size;

static uns num_jobs;
static struct job {
  struct work w;
  cnode n;
  void *buf, *end;
  u64 resolved_cnt, resolved_skel, resolved_unknown;
} *jobs;
static struct worker_pool job_pool;
static struct work_queue job_queue;
static clist free_jobs;

static void
read_table(uns prefix, uns size)
{
  uns bits, size2 = size / resolve_max_hash_density + 1;
  ASSERT(size2 >= size);
  for (bits = 4; size2 >> bits; bits++);
  table_shift = 32 - bits - prefix;
  table_mask = (1 << bits) - 1;
  table = big_alloc((table_mask + 1) * sizeof(*table));
  uns j = -1;
  struct card_print cp;
  UNUSED uns max_chain = 0, dups = 0;
  UNUSED long long sum_chains = 0;
  for (uns i = 0; i < size; i++)
  {
    if (!breadb(fps, &cp, sizeof(cp)))
      ASSERT(0);
    uns h = (fp_hash(&cp.fp) >> table_shift) & table_mask;
    if ((int)h > (int)j)
      while (h > ++j)
	table[j].cardid = ~0U;
    else if (!memcmp(&table[j & table_mask].fp, &cp.fp, sizeof(cp.fp)))
    {
      dups++;
      continue;
    }
    else
      j++;
    if (j > table_mask)
      while ((s32)table[j & table_mask].cardid >= 0)
        j++;
    table[j & table_mask] = cp;
#ifdef LOCAL_DEBUG
    uns chain = j - h + 1;
    max_chain = MAX(max_chain, chain);
    sum_chains += chain;
#endif
  }
  while (++j <= table_mask)
    table[j].cardid = ~0U;
  DBG("Loaded hash table of %u fingerprints (bits=%u+%u, dups=%u, chain: max=%u, avg=%.1f)",
      size, prefix, bits, dups, max_chain, size ? (double)sum_chains / (size - dups) : 0.0);
}

static void
free_table(void)
{
  big_free(table, (table_mask + 1) * sizeof(*table));
}

static inline uns
lookup_table(struct fingerprint *fp)
{
  uns h = (fp_hash(fp) >> table_shift);
  while (1)
  {
    struct card_print *p = &table[h & table_mask];
    if (p->cardid == (u32)~0U || !memcmp(&p->fp, fp, sizeof(*fp)))
      return p->cardid;
    h++;
  }
}

static void process_block(struct fastbuf *in, uns prefix);

static void
process_blocks(struct fastbuf **in, uns prefix, uns split_bits)
{
  uns mask = (1 << split_bits) - 1;
  char name[mask + 1][TEMP_FILE_NAME_LEN];
  for (uns i = 0; i <= mask; i++)
  {
    /* Close fastbufs to save some memory */
    strcpy(name[i], in[i]->name);
    bconfig(in[i], BCONFIG_IS_TEMP_FILE, 0);
    bclose(in[i]);
  }
  for (uns i = 0; i <= mask; i++)
  {
    struct fastbuf *fb = bopen_file(name[i], O_RDONLY, &sorter_fb_params);
    bconfig(fb, BCONFIG_IS_TEMP_FILE, 1);
    process_block(fb, prefix + split_bits);
  }
}

static void
split_block(struct fastbuf *in, uns prefix, uns split_bits)
{
  uns shift = 32 - prefix - split_bits;
  uns mask = (1 << split_bits) - 1;
  struct fastbuf *out[mask + 1];
  for (uns i = 0; i <= mask; i++)
    out[i] = index_bopen_tmp(1);
  struct fingerprint fp;
  while (bread(in, &fp, sizeof(fp)))
  {
    uns h = (fp_hash(&fp) >> shift) & mask;
    bwrite(out[h], &fp, sizeof(fp));
    bbcopy(in, out[h], resolve_record_size);
  }
  if (resolve_trace)
    log(L_INFO, "%llu records split to %u blocks", (long long)(btell(in) / resolve_in_size), mask + 1);
  bclose(in);
  process_blocks(out, prefix, split_bits);
}

static void
job_flush(struct job *j)
{
  DBG("Flushing job %p", j);
  bwrite(out, j->buf, j->end - j->buf);
  clist_add_head(&free_jobs, &j->n);
}

static void
job_sync(void)
{
  struct job *j;
  if (resolve_threads)
    while (j = (void *)work_wait(&job_queue))
      job_flush(j);
}

static struct job *
job_alloc(void)
{
  struct job *j, *f;
  if (clist_empty(&free_jobs))
  {
    j = (void *)work_wait(&job_queue);
    job_flush(j);
  }
  else
    j = SKIP_BACK(struct job, n, clist_head(&free_jobs));
  if (resolve_threads)
    while (f = (void *)work_try_wait(&job_queue))
      job_flush(f);
  DBG("Allocated job %p", j);
  return j;
}

static uns
job_read(struct job *j, struct fastbuf *in)
{
  DBG("Reading job %p", j);
  uns r = bread(in, j->buf, batch_size);
  j->end = j->buf + r;
  ASSERT(!(r % resolve_in_size));
  return r / resolve_in_size;
}

static void
job_resolve(struct job *j)
{
  DBG("Resolving job %p", j);
  struct fingerprint fp;
  void *in = j->buf, *end = j->end, *out = j->buf;
  while (in < end)
  {
    memcpy(&fp, in, sizeof(fp));
    in += resolve_in_size;
    uns cardid = lookup_table(&fp);
    if (cardid == (u32)~0U)
    {
      j->resolved_unknown++;
      if (resolve_flags & RESOLVE_SKIP_UNKNOWN)
	continue;
    }
    else if (cardid >= FIRST_ID_SKEL)
    {
      j->resolved_skel++;
      if (resolve_flags & RESOLVE_SKIP_SKEL)
	continue;
    }
    else
      j->resolved_cnt++;
    put_u32(out, cardid);
    out += 4;
    memcpy(out, in - resolve_record_size, resolve_record_size);
    out += resolve_record_size;
  }
  j->end = out;
}

static void
job_go(struct worker_thread *t UNUSED, struct work *w)
{
  job_resolve((struct job *)w);
}

static void
job_submit(struct job *j)
{
  if (resolve_threads)
  {
    clist_remove(&j->n);
    j->w.go = job_go;
    j->w.priority = 0;
    work_submit(&job_queue, &j->w);
  }
  else
  {
    job_resolve(j);
    job_flush(j);
  }
}

static void
resolve_block(struct fastbuf *in, uns prefix, uns table_size)
{
  resolve_in = in;
  read_table(prefix, table_size);
  long long count = 0, batches = 0;
  while (1)
  {
    struct job *j = job_alloc();
    uns n = job_read(j, in);
    count += n;
    job_submit(j);
    if (!n)
      break;
    batches++;
  }
  job_sync();
  free_table();
  bclose(in);
  if (resolve_trace)
    msg(L_INFO, "Resolved a block of %llu records in %llu batches", count, batches);
}

static void
process_block(struct fastbuf *in, uns prefix)
{
  uns split = bgetl(splits);
  if (!(split & RESOLVE_SPLIT))
    resolve_block(in, prefix, split);
  else
    split_block(in, prefix, split & 0xff);
}

static void
resolve_init(uns flags, uns record_size)
{
  splits = index_bopen(fn_fp_splits, O_RDONLY, 1);
  fps = index_bopen(fn_fingerprints, O_RDONLY, 1);
  out = index_bopen_tmp(1);
  resolve_flags = flags;
  resolve_record_size = record_size;
  resolve_in_size = record_size + sizeof(struct fingerprint);
  batch_size = resolve_in_size * MAX(1, resolve_batch_size / resolve_in_size);
  num_jobs = resolve_threads ? MAX(1, resolve_prefetch) : 1;
  jobs = xmalloc_zero(num_jobs * sizeof(*jobs));
  void *buf = big_alloc(num_jobs * batch_size);
  clist_init(&free_jobs);
  for (uns i = 0; i < num_jobs; i++)
  {
    struct job *j = jobs + i;
    clist_add_tail(&free_jobs, &j->n);
    j->buf = buf + i * batch_size;
  }
  if (resolve_threads)
  {
    job_pool.num_threads = resolve_threads;
    worker_pool_init(&job_pool);
    work_queue_init(&job_pool, &job_queue);
  }
}

static struct fastbuf *
resolve_done(void)
{
  bclose(fps);
  bclose(splits);
  brewind(out);
  if (resolve_threads)
  {
    work_queue_cleanup(&job_queue);
    worker_pool_cleanup(&job_pool);
  }
  big_free(jobs[0].buf, num_jobs * batch_size);
  long long cnt = 0, skel = 0, unknown = 0;
  for (uns i = 0; i < num_jobs; i++)
  {
    struct job *j = jobs + i;
    cnt += j->resolved_cnt;
    skel += j->resolved_skel;
    unknown += j->resolved_unknown;
  }
  xfree(jobs);
  log(L_INFO, "Resolver statistics: %llu + %llu skeletons + %llu unknown", cnt, skel, unknown);
  return out;
}

uns
resolve_start(uns flags, uns record_size)
{
  resolve_init(flags, record_size);
  resolve_first_split = bgetl(splits);
  return (resolve_first_split & RESOLVE_SPLIT) ? resolve_first_split & 0xff : 0;
}

struct fastbuf *
resolve_finish(struct fastbuf **in)
{
  if (resolve_first_split & RESOLVE_SPLIT)
    process_blocks(in, 0, resolve_first_split & 0xff);
  else
    resolve_block(*in, 0, resolve_first_split);
  return resolve_done();
}

struct fastbuf *
resolve_fastbuf(struct fastbuf *in, uns flags, uns record_size)
{
  resolve_init(flags, record_size);
  process_block(in, 0);
  return resolve_done();
}
