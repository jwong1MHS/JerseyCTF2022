/*
 *	Sherlock Library -- Sherlock Query Sender
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#include "sherlock/sherlock.h"
#include "sherlock/query.h"
#include "sherlock/object.h"
#include "sherlock/objread.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>

void
sh_query_init(struct sh_query *q, char *host, int port)
{
  bzero(q, sizeof(*q));
  q->mp = mp_new(4096);
  q->host = xstrdup(host);
  q->port = port;
}

void
sh_query_cleanup(struct sh_query *q)
{
  sh_query_reset(q);
  mp_delete(q->mp);
  xfree(q->host);
}

static void
sh_query_disconnect(struct sh_query *q)
{
  if (q->fb)
    {
      bclose(q->fb);
      q->fb = NULL;
    }
}

void
sh_query_reset(struct sh_query *q)
{
  sh_query_disconnect(q);
  mp_flush(q->mp);
  q->query = q->stat = NULL;
  q->reply = q->header = q->footer = NULL;
  q->cards = NULL;
}

static void NONRET
sh_query_error(struct sh_query *q, char *stat)
{
  ASSERT(stat[0] == '-');
  q->code = atoi(stat + 1);
  q->stat = stat;
  sh_query_disconnect(q);
  longjmp(q->jmp, q->code);
}

#define QFB(f) ((struct sh_query_fb *)(f)->is_fastbuf)
#define NET_BUF_SIZE (16 * 1024)

struct sh_query_fb {
  struct fastbuf fb;
  int fd;
  struct sh_query *q;
};

static int
sh_query_fb_refill(struct fastbuf *f)
{
  f->bptr = f->buffer;
  int l = read(QFB(f)->fd, f->buffer, f->bufend - f->buffer);
  if (l < 0)
    sh_query_error(QFB(f)->q, "-202 Read error");
  f->bstop = f->buffer + l;
  f->pos += l;
  return l;
}

static void
sh_query_fb_close(struct fastbuf *f)
{
  if (close(QFB(f)->fd))
    die("Cannot close socket: %m");
  xfree(f);
}

static struct fastbuf *
sh_query_fb_open(int fd, struct sh_query *q)
{
  struct sh_query_fb *F = xmalloc_zero(sizeof(*F) + NET_BUF_SIZE);
  struct fastbuf *f = &F->fb;
  bzero(F, sizeof(*F));
  f->buffer = (void *)(F + 1);
  f->bptr = f->bstop = f->buffer;
  f->bufend = f->buffer + NET_BUF_SIZE;
  f->name = "<socket>";
  F->fd = fd;
  F->q = q;
  f->refill = sh_query_fb_refill;
  f->close = sh_query_fb_close;
  f->can_overwrite_buffer = 1;
  return f;
}

static void
sh_query_connect(struct sh_query *q)
{
  /* Resolve hostname */
  if (!q->ip)
    {
      struct hostent *server;
      server = gethostbyname(q->host);
      if (!server)
        sh_query_error(q, "-900 Cannot resolve hostname");
      memcpy(&q->ip, server->h_addr, 4);
    }

  /* Create socket */
  int sk = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sk < 0)
    die("Cannot create socket: %m");
  q->fb = sh_query_fb_open(sk, q);

  /* Connect to search server */
  struct sockaddr_in sin;
  bzero(&sin, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = htons(q->port);
  memcpy(&sin.sin_addr, &q->ip, 4);
  if (connect(sk, &sin, sizeof(sin)) < 0)
    sh_query_error(q, "-900 Cannot connect to search server");
}

static void
sh_query_send_query(struct sh_query *q, char *s)
{
  if (strchr(s, '\n'))
    die("Invalid character in the query");
  uns l = strlen(s);
  q->query = mp_memdup(q->mp, s, l + 1);
  q->query[l] = '\n';
  int e = careful_write(QFB(q->fb)->fd, q->query, l + 1);
  q->query[l] = 0;
  if (e <= 0)
    sh_query_error(q, "-206 Write error");
}

static void NONRET
my_obj_read_error(struct obj_read_state *st, char *error)
{
  struct sh_query *q = st->user;
  sh_query_error(q, mp_printf(q->mp, "-901 Reply parse error (%s)", error));
}

static struct odes *
sh_query_parse_object(struct sh_query *q, int *footer, int old)
{
  struct odes *o = obj_new(q->mp);
  struct obj_read_state st;
  byte *s;
  *footer = 0;
  obj_read_start(&st, o);
  st.error_callback = my_obj_read_error;
  st.user = q;
  while (s = bgets_mp(q->fb, q->mp))
    if (!s[0])
      {
	if ((*footer && !old) || (*footer && bgets_mp(q->fb, q->mp)))
	  sh_query_error(q, "-901 Reply parse error (unexpected data after the footer)");
	/* End of header/card or newline-terminated footer of <V3.5 reply */
	obj_read_end(&st);
	return o;
      }
    else if (s[0] == '+' && !st.obj->parent)
      {
	if (!s[1])
	  {
	    if (o->attrs || *footer)
	      sh_query_error(q, "-901 Reply parse error (invalid usage of '+')");
	    *footer = 1;
	  }
	else if ((!o->attrs || *footer) && !old && !strcmp(s, "+++"))
	  {
	    /* End of >=V3.5 reply */
	    *footer = 1;
	    obj_read_end(&st);
	    return o;
	  }
	else
	  sh_query_error(q, "-901 Reply parse error ('+' must not have value)");
      }
    else
      obj_read_attr(&st, s[0], s + 1);
  if (!old)
    sh_query_error(q, "-903 Incomplete reply (missing '+++' trailer)");
  /* End of <V3.5 reply */
  *footer = 1;
  obj_read_end(&st);
  return o;
}

static void
sh_query_parse_reply(struct sh_query *q)
{
  byte *s;
  struct odes *o;
  int footer;
  int old = 0;
  q->reply = obj_new(q->mp);

  /* Parse status line */
  if (!(s = bgets_mp(q->fb, q->mp)))
    sh_query_error(q, "-903 Incomplete reply (empty)");
  if (s[0] != '+' && s[0] != '-')
    sh_query_error(q, "-901 Reply parse error (invalid status line)");
  q->code = atoi(s + 1);
  obj_add_attr_ref(q->reply, 'S', q->stat = s);

  /* Parse header */
  o = sh_query_parse_object(q, &footer, 0);
  if (footer)
    sh_query_error(q, "-901 Reply parse error (missing header)");
  obj_add_son_ref(q->reply, 'H' + OBJ_ATTR_SON, q->header = o);
  
  /* Detect protocol version prior to V3.5 */
  char *version = obj_find_aval(o, 'V');
  if (version)
    {
      uns major, minor;
      if (sscanf(version, "%u.%u", &major, &minor) == 2 && (major < 3 || (major == 3 && minor < 5)))
	old = 1;
    }

  /* Parse cards and footer */
  while (1)
    {
      o = sh_query_parse_object(q, &footer, old);
      if (!footer)
        obj_add_son_ref(q->reply, 'C' + OBJ_ATTR_SON, o);
      else
        {
	  obj_add_son_ref(q->reply, 'F' + OBJ_ATTR_SON, q->footer = o);
          q->cards = obj_find_attr(q->reply, 'C' + OBJ_ATTR_SON);
	  return;
	}
    }
}

int
sh_query_run(struct sh_query *q, char *query)
{
  if (setjmp(q->jmp))
    {
      ASSERT(q->code);
      return q->code;
    }
  sh_query_reset(q);
  sh_query_connect(q);
  sh_query_send_query(q, query);
  sh_query_parse_reply(q);
  sh_query_disconnect(q);
  return q->code;
}

#ifdef TEST

#include "ucw/getopt.h"
#include <stdio.h>

static char *shortopts = CF_SHORT_OPTS "h:p:i";
static struct option longopts[] =
{
  CF_LONG_OPTS
  { "host",             1, 0, 'h' },
  { "port",             1, 0, 'p' },
  { "incomplete",	0, 0, 'i' },
  { NULL,               0, 0, 0 }
};

static char *host = "localhost";
static int port = 8192;
static int incomplete;

static void NONRET
usage(void)
{
  die("Invalid usage, see source code");
}

int main(int argc, char **argv)
{
  int opt;
  log_init(argv[0]);
  while ((opt = cf_getopt(argc, argv, shortopts, longopts, NULL)) >= 0)
    switch (opt)
      {
	case 'h':
	  host = optarg;
	  break;
	case 'p':
	  port = atoi(optarg);
	  break;
	case 'i':
	  incomplete++;
	  break;
	default:
	  usage();
      }
  if (optind == argc)
    usage();

  struct sh_query q;
  log(L_INFO, "Connecting to %s:%d", host, port);
  sh_query_init(&q, host, port);
  for (int i = optind; i < argc; i++)
    {
      printf("<<< %s\n", argv[i]);
      if (sh_query_run(&q, argv[i]))
        {
	  printf("Query failed: %s\n", q.stat);
	  if (incomplete)
	    {
	      printf("Partial reply:\n");
	      obj_dump_indented(q.reply, 0);
	    }
	}
      else
        obj_dump_indented(q.reply, 0);
      printf("\n");
    }
  sh_query_cleanup(&q);

  return 0;
}
#endif
