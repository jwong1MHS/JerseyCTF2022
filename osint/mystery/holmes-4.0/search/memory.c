/*
 *	Sherlock Search Engine -- Memory Mappings
 *
 *	(c) 2001--2005 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"
#include "ucw/lizard.h"
#include "search/sherlockd.h"

#include <stdlib.h>
#include <sys/mman.h>

static uintptr_t mem_map_zone_start, mem_map_zone_end;
struct lizard_buffer *liz_buf;

void
memory_init(void)
{
  uns len = mem_map_zone_size << 20;
  void *zone = mmap(NULL, len, PROT_READ, MAP_ANON | MAP_PRIVATE, -1, 0);

  if (zone == MAP_FAILED)
    die("Unable to find %dMB of mmap space (%m)", mem_map_zone_size);
  mem_map_zone_start = (uintptr_t) zone;
  mem_map_zone_end = mem_map_zone_start + len;
  log(L_INFO, "Memory mapping zone: %08llx-%08llx", (long long) mem_map_zone_start, (long long) mem_map_zone_end - 1);

  liz_buf = lizard_alloc();
}

void
memory_setup(struct query *q)
{
  q->last_mapping = mem_map_zone_start;
}

void
memory_flush(struct query *q)
{
  if (q->last_mapping > mem_map_zone_start)
    {
      if (mmap((void *) mem_map_zone_start, mem_map_zone_end - mem_map_zone_start, PROT_READ, MAP_FIXED | MAP_ANON | MAP_PRIVATE, -1, 0) == MAP_FAILED)
	die("Clearing of memory maps failed");
      q->last_mapping = mem_map_zone_start;
    }
}

/*
 *  We order all mmap requests by their file offset, which gives
 *  an approximation of the real ordering of the corresponding disk
 *  blocks.  A surprisingly good one, it turns out -- on ext3,
 *  large files are usually stored monotonously.
 */

static int
mmap_compare(const void *A, const void *B)
{
  const struct mmap_request *a = A, *b = B;

  if (a->u.req.fd < b->u.req.fd)
    return -1;
  if (a->u.req.fd > b->u.req.fd)
    return 1;
  if (a->u.req.start < b->u.req.start)
    return -1;
  if (a->u.req.start > b->u.req.start)
    return 1;
  return 0;
}

static void
mmap_prefetch(struct mmap_request *reqs, int count)
{
  uintptr_t mask = CPU_PAGE_SIZE - 1;
  uintptr_t start, end;
  volatile byte x;

  while (count--)
    {
      start = ((uintptr_t) reqs->u.map.start) & ~mask;
      end = ((uintptr_t) reqs->u.map.end + mask) & ~mask;
      while (start < end)
	{
	  x = *(byte *)start;
	  start += CPU_PAGE_SIZE;
	}
      reqs++;
    }
}

int
mmap_regions(struct query *q, struct mmap_request *reqs, int count)
{
  int i, j;
  ucw_off_t start, end, size, total_size;
  ucw_off_t mask = (ucw_off_t)(CPU_PAGE_SIZE-1);
  void *x;
  uintptr_t addr;

  qsort(reqs, count, sizeof(struct mmap_request), mmap_compare);
  total_size = 0;
  uintptr_t first_addr = q->last_mapping;
  for (i=0; i<count; )
    {
      j = i;
      start = reqs[i].u.req.start;
      end = start;
      while (i < count && reqs[i].u.req.fd == reqs[j].u.req.fd && reqs[i].u.req.start <= end + (ucw_off_t)mem_map_elide_gaps)
	{
	  end = MAX(end, reqs[i].u.req.end);
	  i++;
	}
      start &= ~mask;
      end = (end + mask) & ~mask;
      size = end - start;
      if (q->last_mapping + size > mem_map_zone_end)
	{
	  log(L_ERROR, "Memory map area overflow");
	  return -1;
	}
      log_fetch(q, reqs[j].u.req.fd, start, size);
      x = ucw_mmap((void *) q->last_mapping, size, PROT_READ, MAP_FIXED | MAP_SHARED, reqs[j].u.req.fd, start);
      if (x == MAP_FAILED)
	{
	  log(L_ERROR, "mmap failed: %m");
	  return -1;
	}
      addr = (uintptr_t) q->last_mapping;
      q->last_mapping += size;
      total_size += size;
      while (j < i)
	{
	  uintptr_t s = addr + (reqs[j].u.req.start - start);
	  uintptr_t e = addr + (reqs[j].u.req.end - start);
	  DBG("Map fd %d block %d @%llx-%llx -> mem @%lx-%lx (join %d)", reqs[j].u.req.fd, j, (long long) reqs[j].u.req.start, (long long) reqs[j].u.req.end, s, e, i);
	  reqs[j].u.map.start = (void *) s;
	  reqs[j].u.map.end = (void *) e;
	  j++;
	}
    }
  madvise((void *) first_addr, total_size, MADV_SEQUENTIAL);
  madvise((void *) first_addr, total_size, MADV_WILLNEED);
  if ((total_size >> 20U) <= (ucw_off_t)mem_map_prefetch)
    mmap_prefetch(reqs, count);
  else
    log(L_DEBUG, "Zone too large for prefetch (%dMB)", (int)(total_size >> 20U));
  return count;
}

void *
mmap_region(struct query *q, int fd, ucw_off_t start, ucw_off_t end)
{
  struct mmap_request r;

  r.u.req.fd = fd;
  r.u.req.start = start;
  r.u.req.end = end;
  if (mmap_regions(q, &r, 1) < 0)
    return NULL;
  else
    return r.u.map.start;
}

void
log_fetch(struct query *q, int fd, ucw_off_t start, ucw_off_t size)
{
  if (q->debug & DEBUG_FETCH)
    add_reply(".FFetching block %llx+%llx from fd %d", (long long) start, (long long) size, fd);
  if (log_fetches)
    log(L_DEBUG, "FETCH %llx %llx %d", (long long) start, (long long) size, fd);
}
