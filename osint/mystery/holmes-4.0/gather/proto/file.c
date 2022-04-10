/*
 *	Sherlock File Downloader
 *
 *	(c) 1997--2001 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/conf.h"
#include "gather/gather.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>

static uns allow_dirs = 1;

static struct cf_section file_config = {
  CF_ITEMS {
    CF_UNS( "AllowDirs", &allow_dirs),
    CF_END
  }
};

static void CONSTRUCTOR file_init_config(void)
{
  cf_declare_section("File", &file_config, 0);
}

static void NONRET
syserr(byte *func)
{
  gerror(2131, "File access error on %s: %m", func);
}

static void
do_file(byte *name, struct stat *st)
{
  int fd, cnt;
  uns block, remains;
  byte buf[4096];

  if ((fd = open(name, O_RDONLY)) < 0)
    syserr("open");
  remains = st->st_size;
  if (remains > max_obj_size)
    {
      remains = max_obj_size;
      gobj_truncate();
    }
  gthis->orig_size = remains;
  while (remains)
    {
      block = (remains > sizeof(buf)) ? sizeof(buf) : remains;
      cnt = read(fd, buf, block);
      if (cnt <= 0)
	{
	  close(fd);
	  syserr("read");
	}
      bwrite(gthis->temp, buf, cnt);
      remains -= cnt;
    }
  close(fd);
}

static int
mergelink(byte *to, byte *from, byte *with)
{
  byte *zz = to + MAX_URL_SIZE - 20;	/* Must leave space for "file://localhost" */
  byte *p;
  uns z;

  if (*with == '/')
    {
      *to++ = *with++;
      p = to;
    }
  else
    {
      strcpy(to, from);
      p = strrchr(to, '/') + 1;
    }
  while (z = *with)
    {
      if (z == '/')
	{
	  with++;
	  continue;
	}
      if (z == '.')
	{
	  if (with[1] == '/' || !with[1])
	    {
	      with++;
	      continue;
	    }
	  if (with[1] == '.' && (with[2] == '/' || !with[2]))
	    {
	      with += 2;
	      if (p == to)
		return 0;
	      p--;			/* Slash */
	      while (p > to && p[-1] != '/')
		p--;
	      continue;
	    }
	}
      while (*with && *with != '/')
	if (p >= zz)
	  return 0;
	else
	  *p++ = *with++;
      if (*with)
	*p++ = *with++;
    }
  *p = 0;
  return 1;
}

static void
do_dir(byte *path, byte *sep)
{
  DIR *d;
  struct dirent *e;
  struct stat st;
  byte xbuf[MAX_URL_SIZE], ybuf[MAX_URL_SIZE], lbuf[MAX_URL_SIZE];
  byte *name;
  int l;

  strcpy(xbuf, path);
  sep = xbuf + (sep - path) + 1;
  d = opendir(path);
  if (!d)
    syserr("opendir");
  while (e = readdir(d))
    {
      if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
	continue;
      if (sep + strlen(e->d_name) + 1 >= xbuf + MAX_URL_SIZE - 20)
	continue;			/* Would be too long! (remember file://localhost/ prefix) */
      strcpy(sep, e->d_name);
      if (lstat(xbuf, &st))
	continue;
      if (S_ISLNK(st.st_mode))
	{
	  if (stat(xbuf, &st))
	    continue;
	  if ((l = readlink(xbuf, lbuf, sizeof(lbuf))) <= 0)
	    continue;
	  lbuf[l] = 0;
	  if (!mergelink(ybuf, xbuf, lbuf))
	    continue;
	  name = ybuf;
	}
      else
	name = xbuf;
      if (S_ISDIR(st.st_mode))
	{
	  byte buf[MAX_URL_SIZE];
	  sprintf(buf, "%s/", name);	/* We know there's enough space */
	  gobj_add_ref('R', buf);
	}
      else if (S_ISREG(st.st_mode))
	gobj_add_ref('R', name);
    }
  closedir(d);
}

static void
do_path(byte *path)
{
  byte b[MAX_URL_SIZE];
  struct stat st;

  if (path[0] != '/' || !mergelink(b, "/", path))
    gerror(2127, "Invalid path");
  if (strcmp(path, b))
    {
      gobj_add_ref('Y', b);	/* gobj_add_ref fortunately accepts relative redirects */
      gobj_set_redirect("Name not normalized, simulating redirect");
      return;
    }

  if (stat(path, &st))
    syserr("stat");
  gthis->lastmod_time = st.st_mtime;
  if (gthis->lastmod_time <= gthis->if_modified_since_time)
    gerror(118, "Not modified");
  if (S_ISREG(st.st_mode))
    do_file(path, &st);
  else if (S_ISDIR(st.st_mode))
    {
      byte *z = strrchr(path, '/');
      if (!allow_dirs)
	gerror(2125, "Indexing of directories forbidden");
      if (!z[1])
	do_dir(path, z);
      else
	{
	  sprintf(b, "%s/", path);	/* We know there's enough free space for the slash */
	  gobj_add_ref('Y', b);
	  gobj_set_redirect("Directory redirect");
	  return;
	}
    }
  else
    gerror(2132, "Special files are not allowed");
}

void
file_download(void)
{
  if (strcasecmp(gthis->url_s.host, "localhost"))
    gerror(2124, "Host name must be `localhost' for the file URL scheme");
  if (gthis->url_s.user)
    gerror(2101, "Authentication not supported");
  do_path(gthis->url_s.rest);
}
