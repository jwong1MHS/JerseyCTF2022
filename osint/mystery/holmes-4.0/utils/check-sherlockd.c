/*
 *	SherlockD Checking Plugin for Nagios
 *
 *	(c) 2001--2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <signal.h>
#include <netinet/in.h>
#include <netdb.h>

#include "utils/check.h"

static void NONRET
help(void)
{
  fprintf(stderr, "Usage: check-sherlockd [<options>] <server> <query>\n\
\n\
Options:\n\
\n\
-d <sec>\tSet DNS resolving timeout (default=10)\n\
-p <port>\tChoose server port (default=8192)\n\
-t <sec>\tSet query timeout (default=10)\n\
-w <sec>\tIf query processing takes more than this, emit a warning (default=5)\n\
");
  result(STATE_UNKNOWN, "ERROR: Incorrect parameters");
}

int
main(int argc, char **argv)
{
  char *server_name, *query;
  int port = 8192;
  int resolve_timeout = 10;
  int request_timeout = 10;
  int warning_threshold = 5;
  struct sockaddr_in sa;
  struct hostent *he;
  int c, sk, querylen, anslen, t;
  byte buf[1024];
  byte *x;
  timestamp_t timer;

  while ((c = getopt(argc, argv, "p:d:t:w:")) >= 0)
    switch (c)
      {
      case 'p':
	port = atol(optarg);
	break;
      case 'd':
	resolve_timeout = atol(optarg);
	break;
      case 't':
	request_timeout = atol(optarg);
	break;
      case 'w':
	warning_threshold = atol(optarg);
	break;
      default:
	help();
      }
  if (optind != argc - 2)
    help();
  server_name = argv[optind++];
  query = argv[optind];
  querylen = strlen(query);

  signal(SIGALRM, timeout);
  init_timer(&timer);

  bzero(&sa, sizeof(sa));
  sa.sin_family = AF_INET;
  sa.sin_port = htons(port);
  phase = "DNS";
  alarm(resolve_timeout);
  if (!(he = gethostbyname(server_name)))
    result(STATE_CRITICAL, "ERROR: Cannot resolve %s - %s", server_name, hstrerror(h_errno));
  memcpy(&sa.sin_addr, he->h_addr, he->h_length);

  phase = "Connect";
  alarm(request_timeout);
  sk = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sk < 0)
    result(STATE_UNKNOWN, "ERROR: socket() failed - %m");
  if (connect(sk, (struct sockaddr *) &sa, sizeof(sa)) < 0)
    result(STATE_CRITICAL, "ERROR: Cannot connect - %m");

  phase = "Request send";
  if (careful_write(sk, query, querylen) <= 0 || careful_write(sk, "\n", 1) <= 0)
    result(STATE_CRITICAL, "ERROR: Send failed - %m");

  phase = "Receive";
  anslen = read(sk, buf, sizeof(buf)-1);
  if (anslen < 0)
    result(STATE_CRITICAL, "ERROR: Receive failed - %m");
  if (!anslen)
    result(STATE_CRITICAL, "ERROR: Empty answer");
  x = buf;
  while (x < buf + anslen && *x != ' ' && *x != '\n')
    x++;
  *x = 0;
  t = get_timer(&timer);
  if (buf[0] == '-')
    result(STATE_CRITICAL, "ERROR: %s", buf+1);
  else if (buf[0] == '+')
    {
      if (t > warning_threshold*1000)
	result(STATE_WARNING, "WARNING: Query took %dms", t);
      else
	result(STATE_OK, "OK: Query took %dms", t);
    }
  else
    result(STATE_CRITICAL, "ERROR: Wrong answer");
}
