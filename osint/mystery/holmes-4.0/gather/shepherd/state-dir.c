/*
 *	Sherlock Shepherd Daemon -- State Directories
 *
 *	(c) 2003--2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

byte *
create_new_state(void)
{
  int retry = 0;
  byte *name = xmalloc(strlen(state_dir) + 1 + 16);
  for (;;)
    {
      time_t now = time(NULL);
      struct tm *tm = gmtime(&now);
      byte *t = name + sprintf(name, "%s/", state_dir);
      int len = strftime(t, 16, "%Y%m%d-%H%M%S", tm);
      ASSERT(len);
      if (!mkdir(name, 0777))
	return name;
      if (errno != EEXIST)
	die("Unable to create new state directory %s: %m", name);
      if (retry++ > 2)
	die("State already exists, probably because of time skew. Please fix manually.");
      /* log(L_WARN, "States appearing too quickly, waiting for 1 sec."); */
      sleep(1);
    }
}

void
delete_state(byte *state)
{
  DIR *d = opendir(state);
  if (!d)
    die("Cannot delete state %s: %m", state);
  struct dirent *e;
  while (e = readdir(d))
    if (strcmp(e->d_name, ".") && strcmp(e->d_name, ".."))
      {
	byte fn[strlen(state) + 1 + strlen(e->d_name) + 1];
	sprintf(fn, "%s/%s", state, e->d_name);
	if (unlink(fn) < 0)
	  die("Cannot unlink %s: %m", fn);
      }
  closedir(d);
  if (rmdir(state) < 0)
    die("Cannot delete state %s: %m", state);
}

void
clone_state(byte *old, byte *new)
{
  DIR *d = opendir(old);
  if (!d)
    die("Cannot scan state %s: %m", old);
  struct dirent *e;
  while (e = readdir(d))
    if (!strcmp(e->d_name, "index") ||
	!strcmp(e->d_name, "sites") ||
	!strcmp(e->d_name, "areas") ||
	!strcmp(e->d_name, "contrib") ||
	!strcmp(e->d_name, "params"))
      {
	byte old_fn[strlen(old) + 1 + strlen(e->d_name) + 1];
	byte new_fn[strlen(new) + 1 + strlen(e->d_name) + 1];
	sprintf(old_fn, "%s/%s", old, e->d_name);
	sprintf(new_fn, "%s/%s", new, e->d_name);
	if (link(old_fn, new_fn) < 0)
	  die("Cannot link %s to %s: %m", old_fn, new_fn);
      }
  closedir(d);
}
