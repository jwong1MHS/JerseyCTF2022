/*
 *	Get Index Version
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"
#include "ucw/md5.h"
#include "ucw/string.h"
#include "sherlock/index.h"
#include "indexer/params.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

int
main(int argc, char **argv)
{
  md5_context mc;
  struct index_params para;
  int fd;

  if (argc < 2)
    die("Usage: index-version <index-dir> <files...>");
  if (chdir(argv[1]) < 0)
    {
      printf("<no-directory>\n");
      return 1;
    }

  fd = open("parameters", O_RDONLY);
  if (fd < 0)
    {
      printf("<no-params>\n");
      return 1;
    }
  if (read(fd, &para, sizeof(para)) != sizeof(para))
    {
      printf("<incompatible-params>\n");
      return 1;
    }
  if (!para.version)
    {
      printf("<incomplete>\n");
      return 1;
    }
  if (para.version != INDEX_VERSION)
    {
      printf("<unknown-version>\n");
      return 1;
    }
  close(fd);
  md5_init(&mc);
  md5_update(&mc, (byte*)&para, sizeof(para));

  for (int i=2; i<argc; i++)
    {
      fd = ucw_open(argv[i], O_RDONLY, 0);
      if (fd < 0)
	{
	  printf("<missing-%s>\n", argv[i]);
	  return 1;
	}
      ucw_off_t len = ucw_seek(fd, 0, SEEK_END);
      md5_update(&mc, (byte*)&len, sizeof(len));
      close(fd);
    }

  byte hex[MD5_HEX_SIZE];
  mem_to_hex(hex, md5_final(&mc), MD5_SIZE, MEM_TO_HEX_UPCASE);
  printf("%08x-%.16s\n", para.ref_time, hex);

  return 0;
}
