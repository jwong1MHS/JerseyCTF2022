/*
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/lfs.h"
#include "ucw/conf.h"
#include "ucw/string.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

static char *ifile;
static char *ofile;
static int iflag = O_RDONLY;
static int oflag = O_WRONLY | O_CREAT;
static int irandom;
static int block = 4096;
static ucw_off_t count;
static ucw_off_t size;
static int ifd = -1;
static int ofd = -1;
static char *buf;

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shdd [options]\n\
\n\
Options:\n\
if=path        path to input file\n\
of=path        path to output file\n\
bs=num         block size in bytes\n\
count=num      number of blocks to process\n\
size=num       count=size/block\n\
iflag=direct   read in O_DIRECT mode\n\
oflag=direct   write in O_DIRECT mode\n\
     =append   O_APPEND\n\
     =trunc    O_TRUNC\n\
     =sync     O_SYNC\n\
     =random   generate random data if no input file is given\n\
");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);

  u64 l;
  for (int i = 1; i < argc; i++)
    if (!strncmp(argv[i], "if=", 3))
      ifile = argv[i] + 3;
    else if (!strncmp(argv[i], "of=", 3))
      ofile = argv[i] + 3;
    else if (!strncmp(argv[i], "iflag=", 6))
      {
	char *list[16];
	int n;
	if ((n = str_sepsplit(argv[i] + 6, ',', list, ARRAY_SIZE(list))) < 0)
	  die("Invalid iflag");
	while (n--)
	  if (!strcmp(list[n], "direct"))
#ifdef CONFIG_DIRECT_IO
	    iflag |= O_DIRECT;
#else
	    die("O_DIRECT not supported on this platform or direct I/O disabled by configure -CONFIG_DIRECT_IO");
#endif
	  else
	    usage();
      }
    else if (!strncmp(argv[i], "oflag=", 6))
      {
	char *list[16];
	int n;
	if ((n = str_sepsplit(argv[i] + 6, ',', list, ARRAY_SIZE(list))) < 0)
	  die("Invalid oflag");
	while (n--)
	  if (!strcmp(list[n], "direct"))
#ifdef CONFIG_DIRECT_IO
	    oflag |= O_DIRECT;
#else
	    die("O_DIRECT not supported on this platform or direct I/O disabled by configure -CONFIG_DIRECT_IO");
#endif
	  else if (!strcmp(list[n], "append"))
	    oflag |= O_APPEND;
	  else if (!strcmp(list[n], "trunc"))
	    oflag |= O_TRUNC;
	  else if (!strcmp(list[n], "sync"))
	    oflag |= O_SYNC;
	  else if (!strcmp(list[n], "random"))
	    irandom = 1;
	  else
	    usage();
      }
    else if (!strncmp(argv[i], "bs=", 3))
      {
	if (cf_parse_u64(argv[i] + 3, &l) || !l || l > ~0U / 2)
	  die("Invalid block size");
	block = l;
      }
    else if (!strncmp(argv[i], "count=", 6))
      {
	if (cf_parse_u64(argv[i] + 6, &l) || !l)
	  die("Invalid count");
	count = l;
      }
    else if (!strncmp(argv[i], "size=", 5))
      {
	if (cf_parse_u64(argv[i] + 5, &l) || !l)
	  die("Invalid size");
	size = l;
      }
    else
      usage();

  if ((!ifile && !ofile) || !block || (size && count))
    usage();
  if (!size && !count)
    count = ~0ULL;
  else
    {
      if (!count)
        count = size / block;
      if (!count)
        usage();
    }
  if (ifile)
    if ((ifd = open(ifile, iflag)) < 0)
      die("open(%s): %m", ifile);
  if (ofile)
    if ((ofd = open(ofile, oflag, 0644)) < 0)
      die("open(%s): %m", ofile);
  buf = big_alloc(block);
  if (ifd < 0)
    if (irandom)
      for (int i = 0; i < block; i++)
	buf[i] = random_max(256);
    else
      bzero(buf, block);

  ucw_off_t i = count, n = 0;
  msg(L_INFO, "Starting...");
  timestamp_t timer;
  init_timer(&timer);
  while (i--)
    {
      int e = 0;
      int readed = block;
      if (ifd >= 0)
        {
          readed = e = read(ifd, buf, block);
          if (e < 0)
	    die("read: %m");
	  if (e != block)
	    msg(L_ERROR, "0x%llx: cannot read next block or reached EOF", (long long)n);
	}
      if (ofd >= 0 && readed)
        {
	  e = write(ofd, buf, readed);
	  if (e < 0)
	    die("write: %m");
	  if (e != readed)
	    msg(L_ERROR, "0x%llx: cannot write next block", (long long)n);
	}
      n += e;
      if (e != block)
	break;
    }
  if (ofd >= 0 && fsync(ofd) && errno != EROFS && errno != EINVAL)
    die("fsync: %m");
  uns t = get_timer(&timer);
  msg(L_INFO, "Processed %.2fMB in %.2f seconds (%.2fMB/s)", n / 1024.0 / 1024, t / 1000.0, t ? (n / 1024.0 / 1024 / t * 1000) : 0.0);

  big_free(buf, block);
  if (ifd >= 0)
    close(ifd);
  if (ofd >= 0)
    close(ofd);

  return 0;
}
