/*
 *	Sherlock Utilities -- A Speed Benchmark
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 */

/*
 *  Error codes used internally:
 *
 *	900	connect refused
 *	901	request send failed
 *	902	reply parsing failed
 *	903	no reply
 */

#include "sherlock/sherlock.h"
#include "sherlock/math.h"
#include "ucw/getopt.h"
#include "ucw/conf.h"
#include "ucw/stkstring.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/log.h"
#include "sherlock/object.h"
#include "sherlock/objread.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/poll.h>

static enum {
  MODE_FLOOD,
  MODE_UNIFORM,
  MODE_EXPONENTIAL
} mode = MODE_FLOOD;

static double rate;
static uns max_requests = ~0U;
static uns max_time;
static uns max_in_flight = 5;
static byte *host = "localhost";
static uns port = 8192;
static byte *logfile;
static uns verbose;

static int log_fd = -1;
static struct sockaddr_in sockaddr;
static struct mempool *rx_pool;
static struct odes *obj;
static byte *temp_file;

static void FORMAT_CHECK(printf, 1, 2)
trace(char *msg, ...)
{
  va_list args;
  va_start(args, msg);
  if (verbose > 1)
    vmsg(L_DEBUG, msg, args);
  va_end(args);
}

static void
setup(void)
{
  rx_pool = mp_new(4096);
  obj = obj_new(rx_pool);

  if (!logfile)
    {
      logfile = temp_file = mp_printf(rx_pool, "tmp/bench-%d", getpid());
      trace("Will use temp file %s", temp_file);
    }

  if ((log_fd = open(logfile, O_RDWR | O_CREAT | O_TRUNC | O_APPEND, 0666)) < 0)
    die("Cannot create %s: %m", logfile);
  static const byte hdr[] = "# ID\tstat\ttotal\tconn\tanswr\tmatches\n";
  write(log_fd, hdr, sizeof(hdr)-1);

  trace("Resolving host %s", host);
  struct hostent *he = gethostbyname(host);
  if (!he)
    die("Cannot resolve %s: %s", host, hstrerror(h_errno));
  sockaddr.sin_family = AF_INET;
  memcpy(&sockaddr.sin_addr.s_addr, he->h_addr, 4);
  sockaddr.sin_port = htons(port);
}

static s64
whats_the_time(void)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (s64)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static s64 t_start, t_end, t_connect, t_reply;

#define LINE_LEN 4096

static int
get_line(struct fastbuf *fb, byte *buf)
{
  int rc = bgets_nodie(fb, buf, LINE_LEN);
  if (rc < 0)
    {
      log(L_ERROR, "Reply line too long");
      return 0;
    }
  return rc;
}

static byte *
fire(byte *req)
{
  int sk = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sk < 0)
    die("socket: %m");

  int one = 1;
  if (setsockopt(sk, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    die("Cannot set TCP_NODELAY");

  trace("Connecting");
  if (connect(sk, (struct sockaddr *) &sockaddr, sizeof(struct sockaddr)) < 0)
    return "-900 Connection refused";
  t_connect = whats_the_time();

  trace("Sending request");
  byte *r = stk_printf("%s\n", req);
  int len = strlen(r);
  if (careful_write(sk, r, len) < 0)
    return "-901 Cannot send request";

  trace("Receiving status");
  struct fastbuf *fb = bfdopen(sk, 65536);
  byte buf[LINE_LEN];
  if (!get_line(fb, buf) || !buf[0])
    return "-903 Missing status line";
  byte *status = mp_strdup(rx_pool, buf);
  t_reply = whats_the_time();

  trace("Receiving header");
  struct obj_read_state rs;
  obj_read_start(&rs, obj);
  rs.errors = 1;		// To shut up error messages
  while (get_line(fb, buf) && buf[0])
    obj_read_attr(&rs, buf[0], buf+1);
  if (buf[0])
    return (status[0] == '-') ? status : (byte*)"-902 Incomplete reply";
  if (obj_read_end(&rs) != 1)
    return "-902 Malformed reply";

  trace("Receiving body");
  int seen_plus = 0;
  while (get_line(fb, buf))
    {
      if (!buf[0])
	seen_plus = 0;
      else if (buf[0] == '+')
	seen_plus = 1;
    }
  if (!seen_plus)
    return "-902 Incomplete reply";

  return status;
}

static void
handle_request(uns index, byte *req, s64 t_planned)
{
  log_fork();
  log_set_format(log_default_stream(), ~0U, LSFMT_USEC);
  setproctitle("bench: req %d", index);

  t_start = whats_the_time();
  byte *stat = fire(req);
  t_end = whats_the_time();
  trace("Done with status %s", stat);

  int rc = (stat[0] == '-') ? atol(stat+1) : 0;
  if (rc && verbose)
    log(L_WARN, "Request #%d: %s", index, stat+1);

  int matches = obj_find_anum(obj, 'N', 0);

  byte *s = stk_printf("%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
		       index, rc,
		       (int)(t_end - t_start),
		       (int)(t_start - t_planned),
		       t_connect ? (int)(t_connect - t_start) : 0,
		       t_reply ? (int)(t_reply - t_connect) : 0,
		       matches);
  write(log_fd, s, strlen(s));
  if (rc >= 900)
    exit(11);
}

static struct fastbuf *query_fb;
static s64 tt_start, tt_now, tt_next, tt_next_progress;
static uns n_req;

static s64
next_query(byte *buf)
{
  if (!bgets(query_fb, buf, 4096) ||
      max_time && tt_now > tt_start + 1000*(s64)max_time ||
      n_req >= max_requests)
    return 0;

  double delta;
  switch (mode)
    {
    case MODE_FLOOD:
      delta = 0;
      break;
    case MODE_UNIFORM:
      delta = 1/rate;
      break;
    case MODE_EXPONENTIAL:
      do
	{
	  double d1 = (double) rand() / RAND_MAX;
	  double d2 = (double) rand() / RAND_MAX;
	  delta = d1 + d2 / ((double)RAND_MAX+1);
	}
      while (!delta);
      delta = -log10(delta) * M_LN10 / rate;
      break;
    default:
      ASSERT(0);
    }

  if (verbose > 1)
    log(L_DEBUG, "Delta = %.3g", delta);
  return tt_now + (int)(delta*1000+0.5);
}

static void
sigchld_handler(int n UNUSED)
{
}

static volatile sig_atomic_t hey_shut_down;

static void
sigint_handler(int n UNUSED)
{
  if (!hey_shut_down)
    log(L_INFO | L_SIGHANDLER, "Shutdown requested");
  else
    log(L_INFO | L_SIGHANDLER, "Shutup requested");
  hey_shut_down++;
}

static void
progress(void)
{
  if (tt_now < tt_next_progress)
    return;
  tt_next_progress += 1000;
  double rs = (tt_now > tt_start) ? (n_req / ((tt_now-tt_start)/1000.)) : 0;
  setproctitle("bench: %d req, %.3f r/s", n_req, rs);
  if (verbose)
    {
      printf("%d req, %.3f r/s      \r", n_req, rs);
      fflush(stdout);
    }
}

static void
loop(void)
{
  uns children = 0;
  byte query[4096];
  pid_t pid;
  int status;
  uns err_cnt = 0;

  struct sigaction sa = { .sa_handler = sigchld_handler, .sa_flags = SA_RESTART };
  sigaction(SIGCHLD, &sa, NULL);
  sa.sa_handler = sigint_handler;
  sigaction(SIGINT, &sa, NULL);
  signal(SIGPIPE, SIG_IGN);

  query_fb = bfdopen(0, 65536);
  tt_start = tt_now = whats_the_time();
  tt_next = next_query(query);
  if (tt_next)
    tt_next = tt_now;
  while (tt_next && !hey_shut_down ||
	 children && hey_shut_down < 2)
    {
      tt_now = whats_the_time();
      progress();
      if (children && (pid = waitpid(-1, &status, WNOHANG)) > 0)
	{
	  byte status_msg[EXIT_STATUS_MSG_SIZE];
	  if (WIFEXITED(status) && WEXITSTATUS(status) == 11)
	    {
	      if (++err_cnt >= 10)
		{
		  log(L_FATAL, "Too many failed connections, giving up.");
		  tt_next = 0;
		}
	    }
	  else if (format_exit_status(status_msg, status))
	    log(L_ERROR, "Child process %s", status_msg);
	  else
	    {
	      trace("Child process %d exited OK", (int)pid);
	      err_cnt = 0;
	    }
	  children--;
	  continue;
	}
      if (tt_next && tt_now >= tt_next && children < max_in_flight && !hey_shut_down)
	{
	  uns dly = tt_now - tt_next;
	  if (dly >= 200 && mode != MODE_FLOOD)
	    log(L_WARN, "Request delayed by %d ms", dly);
	  pid = fork();
	  if (pid < 0)
	    {
	      log(L_FATAL, "fork failed: %m. Shutting down.");
	      tt_next = 0;
	    }
	  else if (!pid)
	    {
	      signal(SIGINT, SIG_DFL);
	      handle_request(n_req, query, tt_next);
	      exit(0);
	    }
	  else
	    {
	      children++;
	      n_req++;
	      tt_next = next_query(query);
	    }
	  continue;
	}
      int timeout = tt_next ? tt_next - tt_now : 10000;
      poll(NULL, 0, timeout);
    }

  double tt = (tt_now - tt_start) / 1000.;
  log(L_INFO, "Fired %d requests in %.3f seconds (%.3f req/sec = %.3f sec/req)", n_req, tt,
      tt ? n_req/tt : 0, n_req ? tt/n_req : 0);
}

static void
stats(void)
{
  lseek(log_fd, 0, SEEK_SET);
  struct fastbuf *fb = bfdopen_shared(log_fd, 4096);
  byte buf[LINE_LEN];
  uns cnt = 0, errs = 0;
  uns stot = 0, scon = 0, srep = 0, sdly = 0, smat = 0;
  uns mtot = 0, mcon = 0, mrep = 0, mdly = 0, mmat = 0;

  while (get_line(fb, buf))
    {
      uns n, rc, tot, con, rep, mat, dly;
      if (!buf[0] || buf[0] == '#')
	continue;
      if (sscanf(buf, "%d%d%d%d%d%d%d", &n, &rc, &tot, &dly, &con, &rep, &mat) != 7)
	ASSERT(0);
      cnt++;
      if (rc)
	errs++;
#define UC(x) s##x += x; m##x = MAX(m##x, x)
      UC(tot);
      UC(con);
      UC(rep);
      UC(dly);
      UC(mat);
    }
  bclose(fb);

  log(L_INFO, "Finished reqs:  %d", cnt);
  log(L_INFO, "Errors:         %d", errs);
  log(L_INFO, "Total matches:  %d", smat);
  if (cnt)
    {
#define AVG(x) (double)s##x/cnt/1000	/* Hack warning */
#define EX(x) (double)m##x/1000
      log(L_INFO, "Total latency:  %.3f avg, %.3f max", AVG(tot), EX(tot));
      if (mode != MODE_FLOOD)
	log(L_INFO, "Request delay:  %.3f avg, %.3f max", AVG(dly), EX(dly));
      log(L_INFO, "Connect delay:  %.3f avg, %.3f max", AVG(con), EX(con));
      log(L_INFO, "Req. to reply:  %.3f avg, %.3f max", AVG(rep), EX(rep));
    }

  if (temp_file && unlink(temp_file) < 0)
    log(L_ERROR, "Cannot unlink %s: %m", temp_file);
}

static void NONRET
usage(void)
{
  fprintf(stderr, "\
Usage: bench <options> < requests\n\
\n\
Basic options:\n\
" CF_USAGE "\
-h <host>[:<port>]\tConnect to the given host and port (default: localhost, 8192)\n\
-l <log>\t\tLog request timing to the specified file\n\
-p <max>\t\tRun at most <max> requests in parallel (default: 5)\n\
-v\t\t\tBe verbose\n\
-vv\t\t\tBe very verbose and trace execution\n\
\n\
Duration options:\n\
-n <count>\t\tStop after <count> requests\n\
-t <time>\t\tStop after <time> seconds\n\
\n\
Request distribution options:\n\
\t\t\tBy default, fire as many requests in parallel as -p allows\n\
-r <rate>\t\tExponential distribution with <rate> requests per second\n\
-u <rate>\t\tUniform distribution with <rate> requests per second\n\
");
  exit(1);
}

int main(int argc, char **argv)
{
  int opt;

  log_init("bench");
  setproctitle_init(argc, argv);
  while ((opt = cf_getopt(argc, argv, "h:l:n:p:r:t:u:v" CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      case 'h':
	{
	  byte *sep = strchr(optarg, ':');
	  if (sep)
	    {
	      *sep++ = 0;
	      if (cf_parse_int(sep, &port) || port >= 65535)
		usage();
	    }
	  else
	    port = 8192;
	  host = optarg[0] ? optarg : "localhost";
	  break;
	}
      case 'l':
	logfile = optarg;
	break;
      case 'n':
	if (cf_parse_int(optarg, &max_requests))
	  usage();
	break;
      case 'p':
	if (cf_parse_int(optarg, &max_in_flight))
	  usage();
	break;
      case 'r':
	if (cf_parse_double(optarg, &rate))
	  usage();
	mode = MODE_EXPONENTIAL;
	break;
      case 't':
	if (cf_parse_int(optarg, &max_time))
	  usage();
	break;
      case 'u':
	if (cf_parse_double(optarg, &rate))
	  usage();
	mode = MODE_UNIFORM;
	break;
      case 'v':
	verbose++;
	break;
      default:
	usage();
      }
  if (optind != argc)
    usage();

  setup();
  loop();
  stats();
  return 0;
}
