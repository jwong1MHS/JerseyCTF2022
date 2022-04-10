/*
 *	Shepherd Checking Plugin for Nagios
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/shepherd/protocol.h"

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
  fprintf(stderr, "Usage: check-shepherd [<options>] <server>\n\
\n\
Options:\n\
\n\
-d <sec>\tSet DNS resolving timeout (default=10)\n\
-p <port>\tChoose server port (default=%d)\n\
-t <sec>\tSet query timeout (default=10)\n\
", SHEPHERD_DEFAULT_PORT);
  result(STATE_UNKNOWN, "ERROR: Incorrect parameters");
}

int
main(int argc, char **argv)
{
  char *server_name;
  int port = SHEPHERD_DEFAULT_PORT;
  int resolve_timeout = 10;
  int request_timeout = 10;
  struct sockaddr_in sa;
  struct hostent *he;
  int c, sk, anslen, t;
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
      default:
	help();
      }
  if (optind != argc - 1)
    help();
  server_name = argv[optind];

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

  phase = "Welcome";
  struct shepp_packet_hdr rp;
  anslen = careful_read(sk, &rp, sizeof(rp));
  if (anslen < 0)
    result(STATE_CRITICAL, "ERROR: Receive failed - %m");
  if (!anslen)
    result(STATE_CRITICAL, "ERROR: Incomplete welcome");
  if (rp.leader != SHEPP_LEADER)
    result(STATE_CRITICAL, "ERROR: Malformed welcome");
  if (rp.type != SHEPP_REPLY_WELCOME)
    result(STATE_CRITICAL, "ERROR: Got %08x instead of welcome", rp.type);
  if (rp.data_len > 0x10000)
    result(STATE_CRITICAL, "ERROR: Welcome packet absurdly long (%d)", rp.data_len);
  byte welcome_buf[rp.data_len];
  anslen = careful_read(sk, welcome_buf, rp.data_len);
  if (anslen < 0)
    result(STATE_CRITICAL, "ERROR: Receive failed - %m");
  if (!anslen)
    result(STATE_CRITICAL, "ERROR: Incomplete welcome data");

  phase = "Request send";
  struct shepp_packet_hdr rq;
  rq.leader = SHEPP_LEADER;
  rq.type = SHEPP_REQ_PING;
  rq.id = 159357;
  rq.data_len = 0;
  if (careful_write(sk, &rq, sizeof(rq)) <= 0)
    result(STATE_CRITICAL, "ERROR: Send failed - %m");

  phase = "Receive";
  anslen = careful_read(sk, &rp, sizeof(rp));
  if (anslen < 0)
    result(STATE_CRITICAL, "ERROR: Receive failed - %m");
  if (!anslen)
    result(STATE_CRITICAL, "ERROR: Incomplete reply");
  if (rp.leader != SHEPP_LEADER)
    result(STATE_CRITICAL, "ERROR: Malformed reply");
  if (rp.type != SHEPP_REPLY_PONG || rp.id != rq.id || rp.data_len)
    result(STATE_CRITICAL, "ERROR: Incorrect reply");

  t = get_timer(&timer);
  result(STATE_OK, "OK: Query took %dms", t);
}
