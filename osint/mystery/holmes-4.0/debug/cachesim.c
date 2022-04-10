/*
 *	A Simple-Minded Simulator of Disk Cache
 *
 *	(c) 2007 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/clists.h"
#include "ucw/fastbuf.h"

#include <stdio.h>
#include <stdlib.h>

struct page {
  cnode lru;
  u64 pos;
};

static clist lru;
static uns alloc, cache_pages;

#define HASH_NODE struct page
#define HASH_PREFIX(x) h_##x
#define HASH_KEY_ATOMIC pos
#define HASH_ATOMIC_TYPE u64
#define HASH_WANT_FIND
#define HASH_WANT_NEW
#define HASH_WANT_REMOVE
#include "ucw/hashtable.h"

static int lookup(u64 pos)
{
  struct page *p = h_find(pos);
  if (p)
    {
      clist_remove(&p->lru);
      clist_add_tail(&lru, &p->lru);
      return 1;
    }
  if (alloc >= cache_pages)
    {
      p = clist_head(&lru);
      ASSERT(p);
      clist_remove(&p->lru);
      h_remove(p);
    }
  else
    alloc++;
  p = h_new(pos);
  clist_add_tail(&lru, &p->lru);
  p->pos = pos;
  return 0;
}

int main(int argc, char **argv)
{
  ASSERT(argc == 2);
  cache_pages = atol(argv[1]) * 1024 / (CPU_PAGE_SIZE/1024);	// Cache size in MB
  uns rqs = 0;
  uns misses = 0;
  clist_init(&lru);
  h_init();

  struct fastbuf *in = bfdopen(0, 4096);
  char line[4096];
  while (bgets(in, line, sizeof(line)))
    {
      char *p = strstr(line, "FETCH ");
      if (!p || line[0] != 'D')
	continue;
      long long pos, len;
      int fd;
      if (sscanf(p+6, "%llx%llx%d", &pos, &len, &fd) != 3)
	die("Bad format");
      len += pos % CPU_PAGE_SIZE;
      pos -= pos % CPU_PAGE_SIZE;
      ASSERT(fd < CPU_PAGE_SIZE);
      while (len > 0)
	{
	  rqs++;
	  if (!lookup(pos+fd))
	    misses++;
	  pos += CPU_PAGE_SIZE;
	  len -= CPU_PAGE_SIZE;
	}
    }

  log(L_INFO, "%dMB cache: %d requests, %d cache misses (%.2f%%)", atoi(argv[1]), rqs, misses, (rqs ? (double)misses/rqs*100 : (double)0));
  return 0;
}
