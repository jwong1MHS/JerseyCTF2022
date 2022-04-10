/*
 *	Sherlock Utilities -- A Simple File Sizing Utility
 *
 *	(c) 2002 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"

#include <stdio.h>
#include <string.h>

int
main(int argc, char **argv)
{
  int i;
  byte buf[256], *p;
  int err=0;

  log_init("sizer");
  if (argc < 2)
    die("Usage: sizer <file> ...");
  p = buf;
  for (i=1; i<argc; i++)
    {
      int fd;
      if (p + strlen(argv[i]) + 32 > buf + sizeof(buf))
	{
	  *p = 0;
	  log(L_INFO, "%s", buf+1);
	  p = buf;
	}
      p += sprintf(p, " %s=", argv[i]);
      fd = ucw_open(argv[i], O_RDONLY, 0);
      if (fd < 0)
	{
	  *p++ = '?';
	  err = 1;
	}
      else
	{
	  ucw_off_t len = ucw_seek(fd, 0, SEEK_END);
	  if (len < 0)
	    {
	      *p++ = '?';
	      err = 1;
	    }
	  else
	    p += sprintf(p, "%lld", (long long) len);
	  close(fd);
	}
    }
  if (p > buf)
    {
      *p = 0;
      log(L_INFO, "%s", buf+1);
    }
  return err;
}
