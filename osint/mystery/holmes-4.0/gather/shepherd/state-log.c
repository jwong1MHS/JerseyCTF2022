/*
 *	Sherlock Shepherd Daemon -- State Log
 *
 *	(c) 2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/shepherd.h"

#include <fcntl.h>
#include <unistd.h>

static struct fastbuf *state_log_fb;

void
state_log_open(byte *state)
{
  struct site *s = NULL;
  uns cnt = 0;
  while (s = site_next(s))
    if (s->monitor)
      cnt++;

  if (cnt)
    {
      log(L_INFO, "Requested state monitoring for %d sites", cnt);
      state_log_fb = append_state_file(state, "state-log");
    }
}

void
state_log_close(void)
{
  bclose(state_log_fb);
  state_log_fb = NULL;
}

void
do_state_log(struct state_log_entry *le)
{
  ASSERT(state_log_fb);
  bwrite(state_log_fb, le, sizeof(*le));
}
