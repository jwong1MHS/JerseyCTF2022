/*
 *	Sherlock Checking Plugins for Nagios -- Common Stuff
 *
 *	(c) 2001--2004 Martin Mares <mj@ucw.cz>
 */

/* Nagios status codes */

#define STATE_CRITICAL  2
#define STATE_WARNING   1
#define STATE_OK        0
#define STATE_UNKNOWN   -1
#define STATE_DEPENDENT -2

static void NONRET
result(int state, char *msg, ...)
{
  va_list args;

  va_start(args, msg);
  alarm(0);
  vfprintf(stdout, msg, args);
  putchar('\n');
  exit(state);
}

static char *phase;

static void
timeout(int sig UNUSED)
{
  result(STATE_CRITICAL, "ERROR: %s timeout", phase);
}
