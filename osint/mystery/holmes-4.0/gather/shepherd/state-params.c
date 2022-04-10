/*
 *	Sherlock Shepherd Daemon -- State Parameters
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *	(c) 2007 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/shepherd.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

static struct state_params cached_params;
static byte *cached_params_state = "";

void
state_params_init(struct state_params *par)
{
  bzero(par, sizeof(*par));
  par->params_magic = PARAMS_MAGIC;
  par->format_version = PARAMS_VERSION_CURRENT;
}

void
state_params_new(byte *state)
{
  struct state_params par;
  state_params_init(&par);
  state_params_write(state, &par);
}

char *
state_params_try_read(byte *fname, struct state_params *par)
{
  int fd = open(fname, O_RDONLY);
  if (fd < 0)
    return strerror(errno);
  int l = read(fd, par, sizeof(*par));
  close(fd);
  if (l < 0)
    return strerror(errno);
  if (l != sizeof(*par) || par->params_magic != PARAMS_MAGIC)
    return "Invalid file format";
  /* Beware, format version is not checked here. */
  return NULL;
}

void
state_params_read(byte *state, struct state_params *par)
{
  if (!strcmp(cached_params_state, state))
    {
      *par = cached_params;
      return;
    }

  byte *name = state_file_name(state, "params");
  byte *err = state_params_try_read(name, par);
  if (err)
    die("Error reading %s: %s", name, err);
  if (par->format_version != PARAMS_VERSION_CURRENT)
    die("State %s has unknown version %08x, need to run shep-upgrade", state, par->format_version);

  cached_params_state = cf_strdup(state);
  cached_params = *par;
}

void
state_params_write(byte *state, struct state_params *par)
{
  if (strcmp(cached_params_state, state))
    cached_params_state = cf_strdup(state);
  else if (!memcmp(&cached_params, par, sizeof(*par)))
    return;
  cached_params = *par;

  struct fastbuf *f = bopen_tmp(sizeof(*par));
  bwrite(f, par, sizeof(*par));
  put_state_file(state, "params", f, 0);
}

void
state_flags_change(byte *state, uns mask, uns set)
{
  ASSERT(!(~mask & set));
  if (!mask)
    return;
  struct state_params par;
  state_params_read(state, &par);
  par.flags = (par.flags & ~mask) | set;
  state_params_write(state, &par);
}

void
state_flags_set(byte *state, uns mask)
{
  state_flags_change(state, mask, mask);
}

void
state_flags_clear(byte *state, uns mask)
{
  state_flags_change(state, mask, 0);
}

uns
state_flags_get(byte *state)
{
  struct state_params par;
  state_params_read(state, &par);
  return par.flags;
}
