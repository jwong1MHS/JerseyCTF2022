/*
 *	Sherlock Search Engine -- Buffering of Replies
 *
 *	(c) 1997--2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "search/sherlockd.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

void
init_reply_buf(struct reply_buf *r, struct mempool *p)
{
  r->first = NULL;
  r->last = &r->first;
  r->pool = p;
}

static struct reply *
make_reply(struct reply_buf *rb, byte *m, va_list args)
{
  byte buf[4096];
  struct reply *r;
  int l;

  l = vsnprintf(buf, sizeof(buf), m, args);
  if (l < 0 || l >= (int)sizeof(buf))
    l = sprintf(buf, ".!TOO LONG!");
  if (log_replies > 1)
    log(L_INFO, ">> %s", buf);
  r = mp_alloc_fast(rb->pool, sizeof(struct reply) + l + 1);
  memcpy(r->text, buf, l);
  r->text[l++] = '\n';
  r->text[l] = 0;
  r->len = l;
  return r;
}

static struct reply *
add_reply_to_v(struct reply_buf *rb, byte *msg, va_list args)
{
  struct reply *r;

  r = make_reply(rb, msg, args);
  *rb->last = r;
  rb->last = &r->next;
  r->next = NULL;
  return r;
}

void
add_reply_to(struct reply_buf *rb, char *msg, ...)
{
  va_list args;

  va_start(args, msg);
  add_reply_to_v(rb, msg, args);
  va_end(args);
}

static struct reply *
prepend_reply_to_v(struct reply_buf *rb, byte *msg, va_list args)
{
  struct reply *r;

  r = make_reply(rb, msg, args);
  r->next = rb->first;
  if (!rb->first)
      rb->last = &r->next;
  rb->first = r;
  return r;
}

void
ship_reply_buf(struct query *q, struct reply_buf *rb)
{
  struct reply *r;

  for(r = rb->first; r; r=r->next)
    write_reply(q, r->text, r->len);
}

void
flush_reply_buf(struct query *q, struct reply_buf *rb)
{
  ship_reply_buf(q, rb);
  init_reply_buf(rb, rb->pool);
}

struct reply *
first_reply_last(struct reply_buf *rb)
{
  struct reply *r = rb->first;

  if (rb->last == &r->next)
    return r;
  rb->first = r->next;
  *rb->last = r;
  rb->last = &r->next;
  r->next = NULL;
  return r;
}

void
add_reply(char *msg, ...)
{
  va_list args;

  va_start(args, msg);
  add_reply_to_v(current_query->current_reply_buf, msg, args);
  va_end(args);
}

void
add_footer(char *msg, ...)
{
  va_list args;

  va_start(args, msg);
  add_reply_to_v(&current_query->reply_footer, msg, args);
  va_end(args);
}

void
add_err(char *msg, ...)
{
  va_list args;

  va_start(args, msg);
  struct reply *r = prepend_reply_to_v(current_query->current_reply_buf, msg, args);
  current_query->q_status = (r->text[0]=='-') ? atol(r->text+1) : 0;
  va_end(args);
}

void
send_reply(char *msg, ...)
{
  byte buf[4096];
  va_list args;
  int l;

  va_start(args, msg);
  l = vsprintf(buf, msg, args);
  ASSERT(l >= 0 && l < (int)sizeof(buf)-1);
  buf[l++] = '\n';
  write_reply(current_query, buf, l);
  va_end(args);
}

void
send_reply_string(char *msg)
{
  write_reply(current_query, msg, strlen(msg));
}

void
send_reply_block(char *start, uns len)
{
  write_reply(current_query, start, len);
}
