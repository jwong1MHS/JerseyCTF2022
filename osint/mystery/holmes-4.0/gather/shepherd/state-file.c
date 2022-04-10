/*
 *	Sherlock Shepherd Daemon -- State Files
 *
 *	(c) 2003--2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

byte *
state_file_name(byte *state, byte *name)
{
  return cf_printf("%s/%s", state, name);
}

struct fastbuf *
read_state_file(byte *state, byte *name)
{
  return bopen(state_file_name(state, name), O_RDONLY, 65536);
}

struct fastbuf *
create_state_file(byte *state, byte *name)
{
  return bopen(state_file_name(state, name), O_WRONLY|O_CREAT|O_TRUNC, 65536);
}

struct fastbuf *
append_state_file(byte *state, byte *name)
{
  return bopen(state_file_name(state, name), O_WRONLY|O_CREAT|O_APPEND, 65536);
}

struct fastbuf *
temp_state_file(void)
{
  return bopen_tmp(65536);
}

void
put_state_file(byte *state, byte *name, struct fastbuf *fb, uns clear_flags)
{
  bfix_tmp_file(fb, state_file_name(state, name));
  state_flags_clear(state, clear_flags);
}
