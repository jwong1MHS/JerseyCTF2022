/*
 *	Sherlock Shepherd Daemon -- Areas
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "gather/shepherd/shepherd.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>

static int areas_fd;
static int areas_rw;
static struct area_info *areas_base;
static uns areas_size;
uns areas_max_id;

static void
areas_map(void)
{
  areas_base = mmap(NULL, areas_size, areas_rw ? (PROT_READ | PROT_WRITE) : PROT_READ, MAP_SHARED, areas_fd, 0);
  if (areas_base == MAP_FAILED)
    die("mmap on area file failed: %m");
}

static void
areas_unmap(void)
{
  if (areas_base)
    {
      munmap(areas_base, areas_size);
      areas_base = NULL;
    }
}

void
areas_init(byte *state, int writeable)
{
  byte *f = state_file_name(state, "areas");
  areas_rw = writeable;
  areas_fd = open(f, (areas_rw ? O_RDWR : O_RDONLY));
  if (areas_fd < 0)
    die("Unable to open %s: %m", f);
  areas_size = lseek(areas_fd, 0, SEEK_END);
  ASSERT(areas_size && !(areas_size % sizeof(struct area_info)));
  areas_max_id = areas_size / sizeof(struct area_info);
  DBG("areas: Loaded area list: %d areas, %d bytes", areas_max_id, areas_size);
  areas_map();
}

void
areas_cleanup(void)
{
  DBG("areas: Cleanup");
  areas_unmap();
  close(areas_fd);
}

static void
areas_expand(area_t id)
{
  if (id >= (area_t)(0x80000000 / sizeof(struct area_info)))
    die("Area ID too large, area file must fit in 2GB");
  areas_unmap();
  uns new_size = (id+1) * sizeof(struct area_info);
  new_size = ALIGN_TO(new_size, CPU_PAGE_SIZE);
  areas_max_id = new_size / sizeof(struct area_info);
  new_size = areas_max_id * sizeof(struct area_info);
  DBG("areas: Expanding to %d records (want id %d)", areas_max_id, id);
  byte buf[CPU_PAGE_SIZE];
  bzero(buf, sizeof(buf));
  while (areas_size < new_size)
    {
      int l = new_size - areas_size;
      l = MIN(l, (int)sizeof(buf));
      int e = write(areas_fd, buf, l);
      if (e < l)
	die("Error expanding area file: %m");
      areas_size += l;
    }
  areas_map();
}

struct area_info *
area_lookup(area_t id, int what_if_nonex)
{
  if (unlikely(id >= areas_max_id))
    {
      if (what_if_nonex < 0)
	return NULL;
      else if (what_if_nonex > 0)
	areas_expand(id);
      else
	id = 0;
    }
  return &areas_base[id];
}
