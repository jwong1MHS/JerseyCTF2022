/*
 * 	Sherlock Shepherd Protocol -- Simple Communication Library
 *
 * 	(c) 2004 Martin Mares <mj@ucw.cz>
 * 	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "ucw/mempool.h"
#include "ucw/conf.h"
#include "ucw/unicode.h"
#include "ucw/chartype.h"
#include "ucw/string.h"
#include "gather/shepherd/protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/signal.h>

int shepp_fd;
u32 shepp_id_counter;
int shepp_timeout;
int shepp_version;

static struct mempool *shepp_pool;

void (*shepp_error_cb)(uns code, char *msg);

void NONRET
shepp_error(uns code, char *msg)
{
  if (shepp_error_cb)
    shepp_error_cb(code, msg);
  die("Communication error %d: %s", code, msg);
}

void
shepp_err(char *msg, ...)
{
  byte buf[256];
  va_list args;
  va_start(args, msg);
  int l = vsnprintf(buf, sizeof(buf), msg, args);
  ASSERT(l >= 0 && l < (int) sizeof(buf));
  shepp_error(SHEPP_REPLY_INTERNAL, buf);
}

static void
shepp_alarm_handler(int x UNUSED)
{
  shepp_error(SHEPP_REPLY_TIMEOUT, "Connection timed out");
}

static void
shepp_alarm_on(void)
{
  if (shepp_timeout)
    {
      static int shepp_have_sigh;
      if (!shepp_have_sigh)
	{
	  struct sigaction sa;
	  bzero(&sa, sizeof(sa));
	  sa.sa_handler = shepp_alarm_handler;
	  sigaction(SIGALRM, &sa, NULL);
	  shepp_have_sigh = 1;
	}
      alarm(shepp_timeout);
    }
}

static void
shepp_alarm_off(void)
{
  if (shepp_timeout)
    alarm(0);
}

static int
shepp_maybe_read(void *buf, uns size)
{
  shepp_alarm_on();
  int l = careful_read(shepp_fd, buf, size);
  shepp_alarm_off();
  if (l < 0)
    shepp_err("Read error: %m");
  return l;
}

void
shepp_read(void *buf, uns size)
{
  if (!shepp_maybe_read(buf, size))
    shepp_err("Unexpected connection close");
}

void
shepp_skip(uns size)
{
  while (size)
    {
      byte buf[1024];
      uns l = MIN(size, sizeof(buf));
      shepp_read(buf, l);
      size -= l;
    }
}

void
shepp_write(void *buf, uns size)
{
  shepp_alarm_on();
  int e = careful_write(shepp_fd, buf, size);
  shepp_alarm_off();
  if (e <= 0)
    shepp_err("Write error: %m");
}

static void
shepp_reset_pool(void)
{
  if (shepp_pool)
    mp_flush(shepp_pool);
  else
    shepp_pool = mp_new(4096);
}

static void
shepp_append_pool(void)
{
  if (!shepp_pool)
    shepp_pool = mp_new(4096);
}

struct odes *
shepp_new_attrs(void)
{
  shepp_reset_pool();
  return obj_new(shepp_pool);
}

void
shepp_send_hdr(struct shepp_packet_hdr *pkt, struct shepp_packet_hdr *reply_to)
{
  pkt->leader = SHEPP_LEADER;
  if (reply_to)
    pkt->id = reply_to->id;
  else
    pkt->id = shepp_id_counter++;
  DBG(">>> %08x id=%08x len=%d", pkt->type, pkt->id, pkt->data_len);
  shepp_write(pkt, sizeof(*pkt));
}

void
shepp_send_none(struct shepp_packet_hdr *pkt, u32 type, struct shepp_packet_hdr *reply_to)
{
  struct shepp_packet_hdr p0;
  if (!pkt)
    pkt = &p0;
  pkt->type = type;
  pkt->data_len = 0;
  shepp_send_hdr(pkt, reply_to);
}

void
shepp_send_raw(struct shepp_packet_hdr *pkt, u32 type, struct shepp_packet_hdr *reply_to, void *data, uns len)
{
  struct shepp_packet_hdr p0;
  if (!pkt)
    pkt = &p0;
  ASSERT((type & 0x0f00) == SHEPP_PAYLOAD_RAW);
  pkt->type = type;
  pkt->data_len = len;
  shepp_send_hdr(pkt, reply_to);
  shepp_write(data, len);
}

void
shepp_send_attrs(struct shepp_packet_hdr *pkt, u32 type, struct shepp_packet_hdr *reply_to, struct odes *attrs)
{
  struct shepp_packet_hdr p0;
  if (!pkt)
    pkt = &p0;
  ASSERT((type & 0x0f00) == SHEPP_PAYLOAD_ATTRS);
  pkt->type = type;
  byte *data = shepp_encode_attrs(attrs, &pkt->data_len);
  shepp_send_hdr(pkt, reply_to);
  shepp_write(data, pkt->data_len);
  mp_flush(shepp_pool);
}

int
shepp_recv_hdr(struct shepp_packet_hdr *pkt, struct shepp_packet_hdr *reply_to)
{
  if (!shepp_maybe_read(pkt, sizeof(*pkt)))
    return 0;
  if (pkt->leader != SHEPP_LEADER)
    shepp_err("Mismatched packet leader %08x", pkt->leader);
  DBG("<<< %08x id=%08x len=%d", pkt->type, pkt->id, pkt->data_len);
  if (reply_to && pkt->id != reply_to->id)
    shepp_err("Expected reply for packet ID %08x, got %08x", reply_to->id, pkt->id);
  return 1;
}

void *
shepp_recv_data(struct shepp_packet_hdr *pkt)
{
  if (pkt->data_len > 0x10000000)
    shepp_err("Packet with %u bytes of data is too large for me", pkt->data_len);
  switch (pkt->type & 0x0f00)
    {
    case SHEPP_PAYLOAD_NONE:
      return NULL;
    case SHEPP_PAYLOAD_RAW:
      shepp_reset_pool();
      void *data = mp_alloc(shepp_pool, pkt->data_len);
      shepp_read(data, pkt->data_len);
      return data;
    case SHEPP_PAYLOAD_ATTRS:
      {
	struct odes *obj = shepp_new_attrs();
	void *data = mp_alloc(shepp_pool, pkt->data_len);
	shepp_read(data, pkt->data_len);
	shepp_decode_attrs(obj, data, pkt->data_len);
	return obj;
      }
    default:
      shepp_err("Packet %08x with unknown payload type received", pkt->type);
    }
}

void *
shepp_recv(struct shepp_packet_hdr *pkt, struct shepp_packet_hdr *reply_to)
{
  if (!shepp_recv_hdr(pkt, reply_to))
    shepp_err("Unexpected connection close");
  return shepp_recv_data(pkt);
}

void
shepp_unex(struct shepp_packet_hdr *pkt)
{
  shepp_err("Unexpected reply (type %08x, id %08x, len %d)", pkt->type, pkt->id, pkt->data_len);
}

byte **
shepp_connect(byte *src)
{
  char **words = cf_malloc(sizeof(byte *) * 17);
  src = cf_strdup(src);
  int n = str_sepsplit(src, ':', words, 15);
  if (n < 1)
    shepp_err("Invalid server address: %s", src);
  words[n] = NULL;
  byte *host = *words++;
  int port = SHEPHERD_DEFAULT_PORT;
  if (*words && Cdigit(**words))
    port = atol(*words++);
  else
    {
      for (uns i=n; i>0; i--)		// remember that words has been incremented exactly once
	words[i] = words[i-1];
      *words++ = cf_printf("%d", SHEPHERD_DEFAULT_PORT);
    }
  log(L_INFO, "Connecting to %s, port %d", host, port);
  shepp_alarm_on();
  struct hostent *he = gethostbyname(host);
  shepp_alarm_off();
  if (!he)
    shepp_err("Cannot resolve %s: %s", host, hstrerror(h_errno));

  int sk = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sk < 0)
    shepp_err("socket() failed: %m");
  shepp_fd = sk;

  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_port = htons(port);
  memcpy(&sin.sin_addr, he->h_addr, 4);
  shepp_alarm_on();
  int e = connect(sk, (struct sockaddr *) &sin, sizeof(sin));
  shepp_alarm_off();
  if (e < 0)
    shepp_err("Cannot connect to %s:%d: %m", host, port);
  int one = 1;
  if (setsockopt(sk, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) < 0)
    shepp_err("Cannot request TCP keepalives: %m");

  struct shepp_packet_hdr rq, rp;
  struct odes *o = shepp_recv(&rp, NULL);
  if (rp.type == SHEPP_REPLY_NOT_AUTHORIZED)
    shepp_err("Connection rejected as unauthorized");
  else if (rp.type != SHEPP_REPLY_WELCOME)
    shepp_err("Received packet type %08x instead of welcome packet", rp.type);
  byte *ver = obj_find_aval(o, 'V');
  if (!ver)
    shepp_err("Missing version number in welcome packet");
  shepp_version = atol(ver);
  shepp_id_counter = 0x1234;

  shepp_send_none(&rq, SHEPP_REQ_PING, NULL);
  shepp_recv(&rp, &rq);

  log(L_INFO, "Connection established (protocol version %d)", shepp_version);
  return (byte **)words;
}

struct odes *
shepp_send_mode(byte **options)
{
  struct odes *attrs = shepp_new_attrs();
  struct shepp_packet_hdr rq, rp;

  while (*options)
    {
      byte *a = *options++;
      if (a[0])
	obj_add_attr(attrs, a[0], a+1);
    }

  shepp_send_attrs(&rq, SHEPP_REQ_SEND_MODE, NULL, attrs);
  attrs = shepp_recv(&rp, &rq);
  if (rp.type == SHEPP_REPLY_DEFER)
    shepp_error(SHEPP_REPLY_DEFER, "Shepherd database not available (cleanup in progress)");
  else if (rp.type == SHEPP_REPLY_NO_SUCH_STATE)
    shepp_error(SHEPP_REPLY_NO_SUCH_STATE, "Requested Shepherd state doesn't exist");
  else if (rp.type != SHEPP_REPLY_SEND_MODE)
    shepp_unex(&rp);
  return attrs;
}

byte *
shepp_encode_attrs(struct odes *attrs, u32 *lenp)
{
  shepp_append_pool();
  put_attr_set_type(BUCKET_TYPE_V33);
  uns len = 0;
  for (struct oattr *a = attrs->attrs; a; a=a->next)
    for (struct oattr *b = a; b; b=b->same)
      len += size_attr(strlen(b->val));
  byte *buf = mp_alloc(shepp_pool, len);
  byte *p = buf;
  for (struct oattr *a = attrs->attrs; a; a=a->next)
    for (struct oattr *b = a; b; b=b->same)
      {
	DBG(">>ATTR: %c%s", b->attr, b->val);
	p = put_attr_str(p, b->attr, b->val);
      }
  *lenp = p - buf;
  ASSERT(*lenp == len);
  return buf;
}

void
shepp_decode_attrs(struct odes *attrs, byte *data, uns len)
{
  /* We do the decoding ourselves, since we want to avoid linking all the decompression machinery */
  byte *end = data + len;
  uns l;
  while (data < end)
    {
      if (data + utf8_encoding_len(*data) > end)
	goto err;
      data = utf8_32_get(data, &l);
      if (!l || data + l > end)
	goto err;
      uns type = data[l-1];
      data[l-1] = 0;
      DBG("<<ATTR: %c%s", type, data);
      obj_add_attr_ref(attrs, type, data);
      data += l;
    }
  return;

 err:
  shepp_err("Attribute format error");
}
