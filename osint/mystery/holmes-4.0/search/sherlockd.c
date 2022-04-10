/*
 *	Sherlock Search Engine -- Main
 *
 *	(c) 1997--2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/ipaccess.h"
#include "search/sherlockd.h"
#include "search/fulltext.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <netinet/in.h>

/*** Allocation and freeing of queries ***/

static clist query_list;

#define IOBUF_SIZE 4096

static struct query *
new_query(void)
{
  struct mempool *pool = mp_new(8192);
  struct query *q = mp_alloc_zero(pool, sizeof(struct query));

  q->pool = pool;
  init_reply_buf(&q->reply_header, pool);
  init_reply_buf(&q->reply_footer, pool);
  q->current_reply_buf = &q->reply_header;
  q->iobuf = mp_alloc(pool, IOBUF_SIZE);
  q->ibptr = q->iobuf;
  q->ibend = q->iobuf + IOBUF_SIZE;
  q->obptr = q->iobuf;
  q->obend = q->iobuf + IOBUF_SIZE;
  clist_add_tail(&query_list, &q->n);
  memory_setup(q);
  q->q_status = -1;
  return q;
}

static void
free_query(struct query *q)
{
  clist_remove(&q->n);
  prefetch_results_cleanup(q);
  memory_flush(q);
  mp_delete(q->pool);
}

/*** Recoding and I/O ***/

static void
flush_buffer(struct query *q, int keep)
{
  int l, e;
  byte *buf = q->iobuf;
  prof_t *old_prof;

  old_prof = profiler_switch(&prof_send);
  l = q->obptr - q->iobuf;
  keep = MIN(l, keep);
  l -= keep;
  while (l && !q->fd_err)
    {
      e = write(q->fd, buf, l);
      if (e <= 0)
	q->fd_err = 1;
      else
	{
	  buf += e;
	  l -= e;
	}
    }
  memmove(q->iobuf, buf, keep);
  q->obptr = q->iobuf + keep;
  profiler_switch(old_prof);
}

void
write_reply(struct query *q, byte *data, uns len)
{
  uns r;

  while (len)
    {
      r = q->obend - q->obptr;
      if (r > len)
	r = len;
      if (!r)
	{
	  flush_buffer(q, 2); // keep 2 bytes to allow detection of trailing newlines in close_conn()
	  continue;
	}
      memcpy(q->obptr, data, r);
      q->obptr += r;
      data += r;
      len -= r;
    }
}

/*** Handling of connections ***/

static void
close_conn(struct query *q, byte *err)
{
  if (err)
    {
      if (log_replies)
	log(L_INFO, "%s : %s", q->ipaddr, err);
      add_reply_to(&q->reply_header, err);
    }
  flush_reply_buf(q, &q->reply_header);
  if (q->obptr >= q->iobuf+2 && (q->obptr[-1] != '\n' || q->obptr[-2] != '\n'))
    write_reply(q, "\n", 1);
  flush_reply_buf(q, &q->reply_footer);
  write_reply(q, "+++\n", 4);
  flush_buffer(q, 0);
  close(q->fd);
  free_query(q);
}

static void
incoming_conn(int sock)
{
  int sock2, alen;
  struct sockaddr_in addr;
  u32 ip;
  struct query *q;

  alen = sizeof(addr);
  sock2 = accept(sock, (struct sockaddr *) &addr, &alen);
  if (sock2 < 0)
    {
      if (errno != EAGAIN)
	log(L_ERROR, "accept(): %m");
      return;
    }
  if (fcntl(sock2, F_SETFL, O_NONBLOCK))
    {
      log(L_ERROR, "fcntl(): %m");
      return;
    }

  q = new_query();
  q->fd = sock2;
  q->established = time(NULL);

  ip = ntohl(addr.sin_addr.s_addr);
  sprintf(q->ipaddr, "%d.%d.%d.%d", (ip>>24)&255, (ip>>16)&255, (ip>>8)&255, ip&255);
  if (!ipaccess_check(&access_list, ip))
    {
      if (log_rejected)
	log(L_INFO, "Rejected connection from %s", q->ipaddr);
      close_conn(q, "-100 Apage Satanas!");
      return;
    }

  if (log_incoming)
    log(L_INFO, "Accepted connection from %s, fd=%d", q->ipaddr, sock2);
}

static void
continue_conn(struct query *q)
{
  int sz, mx;
  byte *r;

  mx = q->ibend - q->ibptr - 1;
  sz = read(q->fd, q->ibptr, mx);
  DBG("Data from %d: %d bytes", q->fd, sz);
  if (!sz)
    {
      close_conn(q, "-108 Incomplete request");
      return;
    }
  if (sz < 0)
    {
      if (errno == EAGAIN)
	return;
      close_conn(q, "-108 Read error");
      return;
    }
  if (sz >= mx)
    {
      close_conn(q, "-101 Request too long");
      return;
    }
  if (fcntl(q->fd, F_SETFL, 0))		/* writes will be blocking */
    die("fcntl(): %m");
  r = q->ibptr;
  while (sz--)
    {
      if (*r == '\r' || *r == '\n')
	{
	  *r = 0;
	  if (query_watchdog)
	    alarm(query_watchdog);
	  if (log_requests)
	    log(L_INFO, "%s < %s", q->ipaddr, q->iobuf);
	  process_query(q);
	  if (query_watchdog)
	    alarm(0);
	  if (log_replies)
	    {
	      byte sprof[PROF_STR_SIZE+6];
#ifdef PROFILER
	      strcpy(sprof, " send=");
	      prof_format(sprof+6, &prof_send);
#else
	      sprof[0] = 0;
#endif
	      log(L_INFO, "> %d t=%d%s%s%s", q->q_status, q->time_total,
		  q->profile_stats ? " " : "",
		  q->profile_stats ? : "",
		  sprof);
	    }
	  close_conn(q, NULL);
	  return;
	}
      r++;
    }
  q->ibptr = r;
}

static void
watchdog_timeout(int x UNUSED)
{
  die("Watchdog timeout");
}

static void
mainloop(int watch_fd)
{
  int sock, n;
  int one = 1;
  time_t now;
  fd_set fds;
  struct query *q, *nq;
  struct timeval tv;
  struct sockaddr_in addr;
  uns timeout;

  cards_init_process();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGALRM, watchdog_timeout);
  sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock < 0)
    die("socket(): %m");
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)))
    die("setsockopt(): %m");
  if (fcntl(sock, F_SETFL, O_NONBLOCK))
    die("fcntl(): %m");
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;
  if (bind(sock, (struct sockaddr *) &addr, sizeof(addr)))
    die("bind(): %m");
  if (listen(sock, listen_queue))
    die("listen(): %m");
  log(L_INFO, "Listening at port %d", port);

  if (status_name)
    {
      int sfd = open(status_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
      if (sfd < 0)
	die("Cannot open status file: %m");
      write(sfd, "OK\n", 3);
      close(sfd);
    }

  clist_init(&query_list);

  for(;;)
    {
      now = time(NULL);
      n = sock;
      FD_ZERO(&fds);
      FD_SET(sock, &fds);
      if (watch_fd)
	FD_SET(watch_fd, &fds);
      timeout = 1000000;
      CLIST_WALK_DELSAFE(q, query_list, nq)
	{
	  if (now - q->established >= connection_timeout)
	    close_conn(q, "-107 Timed out");
	  else
	    {
	      uns rem = q->established + connection_timeout - now;
	      if (rem < timeout)
		timeout = rem;
	      FD_SET(q->fd, &fds);
	      if (q->fd > n)
		n = q->fd;
	    }
	}

      tv.tv_sec = timeout;
      tv.tv_usec = 0;
      n = select(n+1, &fds, NULL, NULL, &tv);
      if (n < 0)
	die("Select failed: %m");
      if (!n)
	continue;

      if (FD_ISSET(sock, &fds))
	incoming_conn(sock);
      if (watch_fd && FD_ISSET(watch_fd, &fds))
	die("Master process lost, terminating");

      CLIST_WALK_DELSAFE(q, query_list, nq)
	if (FD_ISSET(q->fd, &fds))
	  continue_conn(q);
    }
}

/*** The "Hydra Mode" -- fork several copies working in parallel on different ports ***/

#define MAX_HEADS 16

static pid_t hydra_pid[MAX_HEADS];
static volatile sig_atomic_t hydra_terminate;
static int hydra_killed;

static void
hydra_sigterm(int sig UNUSED)
{
  hydra_terminate = 1;
}

static void
hydra_kill(void)
{
  if (hydra_killed)
    return;
  hydra_killed = 1;
  for (uns i=0; i<hydra_processes; i++)
    if (hydra_pid[i])
      kill(hydra_pid[i], SIGTERM);
}

static void
hydra_setup(void)
{
  int error = 0;
  int running = 0;
  int fds[2];

  if (hydra_processes > MAX_HEADS)
    die("Search.HydraProcesses too large, maximum is %d", MAX_HEADS);
  if (pipe(fds) < 0)
    die("pipe: %m");

  for (uns i=0; i<hydra_processes; i++)
    {
      pid_t pid = fork();
      if (pid < 0)
	{
	  log(L_ERROR, "Cannot create subprocess: %m");
	  error = 1;
	}
      else if (!pid)
	{
	  log_fork();
	  close(fds[1]);
	  mainloop(fds[0]);
	}
      else
	{
	  hydra_pid[i] = pid;
	  running++;
	}
      port++;
    }
  close(fds[0]);
  log(L_INFO, "Started %d subprocesses", running);
  if (error)
    hydra_kill();

  struct sigaction sa;
  bzero(&sa, sizeof(sa));
  sa.sa_handler = hydra_sigterm;
  sigaction(SIGTERM, &sa, NULL);

  while (running)
    {
      if (hydra_terminate)
	hydra_kill();
      int stat;
      pid_t pid = wait(&stat);
      if (pid > 0)
	{
	  uns i = 0;
	  while (i < hydra_processes && hydra_pid[i] != pid)
	    i++;
	  if (i >= hydra_processes)
	    log(L_ERROR, "Unknown child process %d exited", (int) pid);
	  else
	    {
	      byte status_msg[EXIT_STATUS_MSG_SIZE];
	      format_exit_status(status_msg, stat);
	      if (WIFSIGNALED(stat) && WTERMSIG(stat) == SIGTERM && hydra_terminate)
		log(L_INFO, "Subprocess %d terminated", (int) pid);
	      else
		{
		  log(L_ERROR, "Subprocess %d %s", (int) pid, status_msg);
		  hydra_kill();
		  error = 1;
		}
	      hydra_pid[i] = 0;
	      running--;
	    }
	}
      else if (errno != EINTR)
	log(L_ERROR, "wait: %m");
    }

  log(L_INFO, "All subprocesses exited, good night");
  exit(error);
}

/*** Main ***/

static void NONRET
usage(void)
{
  fputs("\
Usage: sherlockd <options>\n\
\n\
Options:\n\
" CF_USAGE "\
-m, --merge-only\tMerge indices and exit (safe on indices used by another sherlockd)\n\
", stderr);
  exit(1);
}

static struct option longopts[] =
{
  CF_LONG_OPTS
  { "merge-only",	0, 0, 'm' },
  { NULL,		0, 0, 0 }
};

int
main(int argc, char **argv)
{
  int opt;
  int merge_only = 0;

  log_init(NULL);
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS "m", longopts, NULL)) >= 0)
    switch (opt)
      {
      case 'm':
	merge_only++;
	break;
      default:
	usage();
      }
  if (optind < argc)
    usage();
  if (merge_only)
    {
      db_init(1);
      return 0;
    }
  log_file(log_name);
  db_init(0);
  cache_init();
  query_init();
  refs_init();
  fulltext_init();
  memory_init();
  cards_init();
  spell_init();
#ifdef CUSTOM_INIT
  CUSTOM_INIT();
#endif
  if (hydra_processes)
    hydra_setup();
  mainloop(0);
  return 0;
}
