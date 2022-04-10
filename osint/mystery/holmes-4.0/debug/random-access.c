/*
 *	Measure Timing of Random File Accesses
 *
 *	(c) 2001 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"
#include "ucw/semaphore.h"
#include "ucw/threads.h"
#include "ucw/getopt.h"
#include "ucw/conf.h"
#include "ucw/string.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <alloca.h>
#include <time.h>
#include <sys/mman.h>
#include <pthread.h>

enum method {
  M_SEEK_READ = 0,
  M_DIRECT_SEEK_READ = 1,
  M_PREAD = 2,
  M_DIRECT_PREAD = 3,
  M_MMAP = 4,
  M_PWRITE = 5,
  M_DIRECT_PWRITE = 6,
};

#define O_DIRECT_ALIGN 9

static enum method method = M_SEEK_READ;
static uns attempts = 1000;
static uns attempt;
static uns num_threads = 1;
static char *file;
static uns align = 0;
static uns chunks_sum = 1;
static uns *chunks = (uns []) { 1 };
static uns num_chunks = 1;
static int gfd;
static byte *map = NULL;
static u64 rsize;

static timestamp_t timer;

static uns
parse_int(byte *s)
{
  byte *err;
  uns value;
  if (err = cf_parse_int(s, &value))
    die("%s", err);
  return value;
}

static u64
parse_u64(byte *s)
{
  byte *err;
  u64 value;
  if (err = cf_parse_u64(s, &value))
    die("%s", err);
  return value;
}

static int
open_fd(int flags)
{
  int fd = ucw_open(file, flags, 0);
  if (fd < 0)
    die("open: %m");
  return fd;
}

static pthread_mutex_t attempts_mutex = PTHREAD_MUTEX_INITIALIZER;
static sem_t *init_sem;
static sem_t *go_sem;

static void *
thread_go(void *p UNUSED)
{
  // Thread initialization
  int fd = gfd;
  byte *buf = big_alloc(chunks_sum);
  switch (method)
    {
    case M_SEEK_READ:
      fd = open_fd(O_RDONLY);
      break;
    case M_DIRECT_SEEK_READ:
#ifdef CONFIG_DIRECT_IO
      fd = open_fd(O_RDONLY | O_DIRECT);
#else
      die("O_DIRECT not supported on this platform or direct I/O disabled by configure -CONFIG_DIRECT_IO");
#endif
      break;
    case M_PWRITE:
    case M_DIRECT_PWRITE:
      for (uns i = 0; i < chunks_sum; i++)
        buf[i] = i;
      break;
    default:
      break;
    }

  if (num_threads > 1)
    {
      sem_post(init_sem);
      sem_wait(go_sem);
    }

  // Main loop
  while (1)
    {
      // Allocate job
      pthread_mutex_lock(&attempts_mutex);
      if (attempt == attempts)
        {
          pthread_mutex_unlock(&attempts_mutex);
	  break;
	}
      attempt++;
      ucw_off_t pos = random_max_u64(rsize) << align;
      pthread_mutex_unlock(&attempts_mutex);

      // Process job
      ucw_off_t res;
      u64 ofs = 0;
      switch (method)
	{
	case M_SEEK_READ:
	case M_DIRECT_SEEK_READ:
	  res = ucw_seek(fd, pos, SEEK_SET);
	  if (res < 0)
	    die("seek: %m");
	  for (uns i = 0; i < num_chunks; i++)
	    {
	      res = read(fd, buf + ofs, chunks[i]);
	      ofs += chunks[i];
	      if (res != (ucw_off_t)chunks[i])
	        {
	          if (res < 0)
	            die("read: %m");
	          else
		    die("read: short read");
	        }
	    }
	  break;
	case M_MMAP:
	  for (uns i = 0; i < num_chunks; i++)
	    {
	      memcpy(buf + ofs, map + pos + ofs, chunks[i]);
	      ofs += chunks[i];
	    }
	  break;
	case M_PREAD:
	case M_DIRECT_PREAD:
	  for (uns i = 0; i < num_chunks; i++)
	    {
	      res = ucw_pread(fd, buf + ofs, chunks[i], pos + ofs);
	      ofs += chunks[i];
	      if (res != (ucw_off_t)chunks[i])
	        {
	          if (res < 0)
	            die("pread: %m");
	          else
		    die("pread: short read");
	        }
	    }
	  break;
	case M_PWRITE:
	case M_DIRECT_PWRITE:
	  for (uns i = 0; i < num_chunks; i++)
	    {
	      res = ucw_pwrite(fd, buf + ofs, chunks[i], pos + ofs);
	      ofs += chunks[i];
	      if (res != (ucw_off_t)chunks[i])
	        {
	          if (res < 0)
	            die("pwrite: %m");
	          else
		    die("pwrite: short write");
	        }
	    }
	  break;
	default:
	  ASSERT(0);
	}
    }
  return NULL;
}

static char *shortopts = "mpdDwWn:s:a:c:r:t:" CF_SHORT_OPTS;
static struct option longopts[] =
{
  CF_LONG_OPTS
  { NULL, 0, 0, 0 }
};

static char *help = "\
Usage: random-access [<options>] <file>\n\
\n\
Options:\n"
CF_USAGE
"\
-m                      Use mmap (default=seek+read)\n\
-p                      Use pread\n\
-d                      Use seek+read with O_DIRECT\n\
-D                      Use pread with O_DIRECT\n\
-w			Use pwrite\n\
-W			Use pwrite with O_DIRECT\n\
-n <num>                Number of attempts (default=1000)\n\
-s <num>                Access only first <num> bytes of the file\n\
-a <num>                Align reads to multiples of (1 << <num>) bytes (default=0)\n\
-c <num>[,<num>[,...]]  Read chunks (or sets of adjacent chunks) of given numbers of bytes in each attempt (default=1)\n\
-r <num>                Set custom random seed\n\
-t <num>                Run in <num> threads\n\
";

static void NONRET
usage(void)
{
  fputs(help, stderr);
  exit(1);
}

int
main(int argc, char **argv)
{
  u64 max_size = 0;
  uns tim, seed = time(NULL) ^ getpid();
  int opt;

  cf_def_file = NULL;
  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, shortopts, longopts, NULL)) >= 0)
    switch (opt)
      {
      case 'n':
	attempts = parse_int(optarg);
	break;
      case 's':
	max_size = parse_u64(optarg);
	break;
      case 'm':
	method = M_MMAP;
	break;
      case 'p':
	method = M_PREAD;
	break;
      case 'd':
	method = M_DIRECT_SEEK_READ;
	break;
      case 'D':
	method = M_DIRECT_PREAD;
	break;
      case 'w':
        method = M_PWRITE;
	break;
      case 'W':
        method = M_DIRECT_PWRITE;
	break;
      case 'a':
	align = parse_int(optarg);
	if (align > 63)
	  die("Too large align size");
	break;
      case 'c':
	{
	  num_chunks = 1;
	  for (uns i = strlen(optarg) - 1; i--; )
	    num_chunks += optarg[i] == ',';
	  chunks = xmalloc(num_chunks * sizeof(*chunks));
	  char *rec[num_chunks];
	  uns i = str_sepsplit(optarg, ',', rec, num_chunks);
	  ASSERT(i == num_chunks);
	  for (chunks_sum = i = 0; i < num_chunks; i++)
	    chunks_sum += chunks[i] = parse_int(rec[i]);
	}
	break;
      case 'r':
	seed = parse_int(optarg);
	break;
      case 't':
	num_threads = parse_int(optarg);
	break;
      default:
	usage();
      }
  if (optind != argc - 1)
    usage();
  file = argv[optind];
  srand(seed);

  gfd = open_fd(O_RDONLY);
  u64 size = ucw_seek(gfd, 0, SEEK_END);
  if (max_size && size > max_size)
    size = max_size;

  printf("Measuring random accesses to %s (%llu bytes)\n", file, (unsigned long long) size);
  if (method == M_DIRECT_SEEK_READ || method == M_DIRECT_PREAD)
    {
      if (align < O_DIRECT_ALIGN)
        {
	  printf("Increasing align size to O_DIRECT's minimum\n");
	  align = O_DIRECT_ALIGN;
	}
      for (uns i = 0; i < num_chunks; i++)
        if (chunks[i] & ((1ULL << O_DIRECT_ALIGN) - 1))
          {
	    printf("Aligning chunk size to the nearest multiple of O_DIRECT's block size\n");
            uns inc = (1ULL << O_DIRECT_ALIGN) - (chunks[i] & ((1ULL << O_DIRECT_ALIGN) - 1));
            chunks_sum += inc;
	    chunks[i] += inc;
	  }
    }
  if (size < chunks_sum)
    die("The file is too small (file size < chunk size)");
  rsize = (size - chunks_sum) >> align;
  rsize = MAX(rsize, 1);
  printf("%s %u chunks of %u", (method != M_PWRITE) ? "Reading" : "Writing", attempts, chunks[0]);
  for (uns i = 1; i < num_chunks; i++)
    printf("+%u", chunks[i]);
  printf(" bytes aligned at %llu bytes (there are %llu of them)\n", 1ULL << align, (unsigned long long) rsize);
  
  switch (method)
    {
    case M_SEEK_READ:
      puts("Using seek() and read() to read the file");
      close(gfd);
      break;
    case M_DIRECT_SEEK_READ:
      puts("Using seek() and read() in O_DIRECT mode to read the file");
      close(gfd);
      break;
    case M_PREAD:
      puts("Using pread() to read the file");
      break;
    case M_DIRECT_PREAD:
      puts("Using pread() in O_DIRECT mode to read the file");
      close(gfd);
#ifdef CONFIG_DIRECT_IO
      gfd = open_fd(O_RDONLY | O_DIRECT);
#else
      die("O_DIRECT not supported on this platform");
#endif
      break;
    case M_MMAP:
      puts("Using mmap() to read the file");
      map = ucw_mmap(NULL, size, PROT_READ, MAP_SHARED, gfd, 0);
      if (map == MAP_FAILED)
	die("mmap: %m");
      break;
    case M_PWRITE:
      puts("Using pwrite() to write the file");
      close(gfd);
      gfd = open_fd(O_WRONLY);
      break;
    case M_DIRECT_PWRITE:
      puts("Using pwrite() in O_DIRECT mode to write the file");
      close(gfd);
#ifdef CONFIG_DIRECT_IO
      gfd = open_fd(O_WRONLY | O_DIRECT);
#else
      die("O_DIRECT not supported on this platform");
#endif
      break;
    default:
      ASSERT(0);
    }

  if (num_threads <= 1)
    {
      init_timer(&timer);
      thread_go(NULL);
    }
  else
    {
      printf("Using %u threads\n", num_threads);
      pthread_t threads[num_threads];
      pthread_attr_t attr;
      if (pthread_attr_init(&attr) < 0 ||
          pthread_attr_setstacksize(&attr, ucwlib_thread_stack_size) < 0)
        ASSERT(0);
      init_sem = sem_alloc();
      go_sem = sem_alloc();
      for (uns i = 0; i < num_threads; i++)
        {
          if (pthread_create(&threads[i], &attr, thread_go, NULL) < 0)
            die("Unable to create thread: %m");
	  sem_wait(init_sem);
	}
      init_timer(&timer);
      for (uns i = 0; i < num_threads; i++)
	sem_post(go_sem);
      for (uns i = 0; i < num_threads; i++)
        {
          if (pthread_join(threads[i], NULL) < 0)
            die("Cannot join thread: %m");
	}
    }

  if (method == M_PWRITE)
    {
      puts("Fsyncing");
      fsync(gfd);
    }
  tim = get_timer(&timer);

  printf("It took %d ms -> %.3f ms per access\n", tim, (double)tim / attempts);
  return 0;
}
