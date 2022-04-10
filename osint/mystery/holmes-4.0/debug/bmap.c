/*
 *	Map Disk Blocks of a Given File
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

int main(int argc, char **argv)
{
  log_init("bmap");

  int verbose = 0;
  if (argc > 1 && !strcmp(argv[1], "-v"))
    {
      verbose = 1;
      argv++;
      argc--;
    }
  if (argc != 2)
    die("Usage: bmap [-v] <file>");

  int fd = ucw_open(argv[1], O_RDONLY);
  if (fd < 0)
    die("open(%s): %m", argv[1]);
  ucw_stat_t st;
  if (ucw_fstat(fd, &st) < 0)
    die("fstat(%s): %m", argv[1]);

  uns bsize;
  if (ioctl(fd, FIGETBSZ, &bsize) < 0)
    die("FIGETBSZ: %m");

  uns nblks = (st.st_size + bsize - 1) / bsize;
  printf("### %d blocks per %d bytes\n", nblks, (int) st.st_blksize);

  int last = -1;
  int jumps = 0, back = 0;
  for (uns i=0; i<nblks; i++)
    {
      int j = i;
      if (ioctl(fd, FIBMAP, &j) < 0)
	die("FIBMAP: %m");
      if (verbose)
	printf("%d\t%d", i, j);
      if (last >= 0)
	{
	  if (j != last+1)
	    {
	      if (verbose)
		printf("\t%d", j-last);
	      jumps++;
	    }
	  if (j < last)
	    back++;
	}
      last = j;
      if (verbose)
	putchar('\n');
    }
  printf("### %d jumps, %d backwards\n", jumps, back);

  close(fd);
  return 0;
}
