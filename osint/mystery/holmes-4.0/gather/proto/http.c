/*
 *	Sherlock HTTP Downloader
 *
 *	(c) 1997--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2002 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/chartype.h"
#include "ucw/url.h"
#include "ucw/conf.h"
#include "ucw/mempool.h"
#include "ucw/base64.h"
#include "ucw/clists.h"
#include "ucw/simple-lists.h"
#include "lang/lang.h"
#include "sherlock/index.h"
#include "gather/gather.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <time.h>
#include <sys/signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

/* Configuration parameters */

static uns max_hdr_lines = 1000;
static char *local_admin = "";
static char *referer = "";
static char *acc_charset = "";
static char *acc_encoding = "";
static char *acc_lang = "";
static char *acc_types = "";
static uns connect_timeout = 60;
static uns header_timeout = 60;
static uns body_timeout = 600;
static char *proxy = "";
static uns proxy_port = 80;
static uns trace = 0;
static uns length_checks = 1;
static char *user_agent = "holmes/" SHER_VER;
static char *user_agent_cmt = "";

static clist list_charset, list_encoding, list_lang, list_types;
static clist user_headers;

static void
merge_list(char **dest, clist *src)
{
  cf_journal_block(dest, sizeof(byte*));
  simp_node *n;
  uns len = 0;
  CLIST_WALK(n, *src)
    len += strlen(n->s) + 2;
  char *c = *dest = cf_malloc(len + 1);
  CLIST_WALK(n, *src)
    c += sprintf(c, "%s, ", n->s);
  if (c > *dest)
    c[-2] = 0;
}

static char *
http_commit(void *ptr UNUSED)
{
  merge_list(&acc_charset, &list_charset);
  merge_list(&acc_encoding, &list_encoding);
  merge_list(&acc_lang, &list_lang);
  merge_list(&acc_types, &list_types);
  return NULL;
}

static struct cf_section http_config = {
  CF_COMMIT(http_commit),
  CF_ITEMS {
    CF_UNS("MaxHeaderLines", &max_hdr_lines),
    CF_STRING("From", &local_admin),
    CF_STRING("Referer", &referer),
    CF_LIST("AcceptCharset", &list_charset, &cf_string_list_config),
    CF_LIST("AcceptEncoding", &list_encoding, &cf_string_list_config),
    CF_LIST("AcceptLanguage", &list_lang, &cf_string_list_config),
    CF_LIST("AcceptTypes", &list_types, &cf_string_list_config),
    CF_UNS("ConnectTimeout", &connect_timeout),
    CF_UNS("HeaderTimeout", &header_timeout),
    CF_UNS("BodyTimeout", &body_timeout),
    CF_STRING("UseProxy", &proxy),
    CF_UNS("ProxyPort", &proxy_port),
    CF_UNS("Trace", &trace),
    CF_UNS("LengthChecks", &length_checks),
    CF_LIST("Header", &user_headers, &cf_string_list_config),
    CF_STRING("UserAgent", &user_agent),
    CF_STRING("UserAgentComment", &user_agent_cmt),
    CF_END
  }
};

static void CONSTRUCTOR http_init_config(void)
{
  cf_declare_section("HTTP", &http_config, 0);
}

/* Other globals */

static uns head_only;
static int expected_length;
static uns is_chunked;

#define TRACE(x,y...) do { if (trace) log(L_DEBUG, x,##y); } while (0)

static void
timeout(int unused UNUSED)
{
  gerror(1102, "HTTP timeout");
}

/* Set up a network connection */

static int
http_connect(char *host, uns port)
{
  struct sockaddr_in rem;
  int sock;

  rem.sin_family = AF_INET;
  rem.sin_addr.s_addr = resolve_host_name(host);
  rem.sin_port = htons(port);
  if (proxy[0])
    {
      struct hostent *h = gethostbyname(proxy);
      if (!h)
	gerror(1138, "Unable to resolve name of HTTP proxy (%s)", hstrerror(h_errno));
      memcpy(&rem.sin_addr.s_addr, h->h_addr, 4);
      rem.sin_port = htons(proxy_port);
      host = proxy;
    }
  TRACE("Opening connection to %s (%08x), port %d", host, ntohl(rem.sin_addr.s_addr), ntohs(rem.sin_port));

  sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock < 0)
    die("No socket (%m)");
  alarm(connect_timeout);
  if (connect(sock, (struct sockaddr *) &rem, sizeof(struct sockaddr_in)) < 0)
    {
      if (proxy[0])
	gerror(1138, "Cannot connect to HTTP proxy (%m)");
      else
	gerror(1107, "Connect failed (%m)");
    }
  TRACE("Connected");

  return sock;
}

/* Sending of single header */

static void FORMAT_CHECK(printf,2,3)
sendhdr(FILE *f, char *txt, ...)
{
  byte buf[1024];
  va_list args;

  va_start(args, txt);
  vsprintf(buf, txt, args);
  TRACE("> %s", buf);
  fputs(buf, f);
  fputs("\r\n", f);
}

/* Date formatting and parsing functions */

static void
http_form_date(byte *buf, time_t time)
{
  struct tm *tm;

  tm = gmtime(&time);
  strftime(buf, 64, "%a, %d %b %Y %H:%M:%S GMT", tm);
}

static int
validate_time(byte *a, byte *p)
{
  while (*p && *a)
    {
      switch (*p)
	{
	case '.':
	  break;
	case '$':
	  if (*a == ' ')
	    break;
	  /* Fall-thru */
	case '#':
	  if (!Cdigit(*a))
	    return 0;
	  break;
	case 'a':
	  if (!Clower(*a))
	    return 0;
	  break;
	case 'A':
	  if (!Cupper(*a))
	    return 0;
	  break;
	case '@':
	  if (!Calpha(*a))
	    return 0;
	  break;
	case '*':
	  return 1;
	case '_':
	  if (*a != ' ' && *a != '-')
	    return 0;
	  break;
	default:
	  if (*p != *a)
	    return 0;
	}
      p++;
      a++;
    }
  return (*p == *a || *p == '*');
}

static int
twodig(byte *p)
{
  if (p[0] == ' ')
    return p[1] - '0';
  else
    return (p[0] - '0')*10 + p[1] - '0';
}

static int
fourdig(byte *p)
{
  return twodig(p)*100 + twodig(p+2);
}

static byte short_months[] = "JanFebMarAprMayJunJulAugSepOctNovDec";

static int
find_month(byte *p)
{
  byte *z = short_months;
  int m = 0;

  while (*z)
    {
      m++;
      if (p[0] == z[0] && p[1] == z[1] && p[2] == z[2])
	return m;
      z += 3;
    }
  return 0;
}

static uns
http_parse_date(byte *buf)
{
  byte *p = buf;
  byte *q;
  struct tm tm;
  int m, y;

  bzero(&tm, sizeof(tm));
#ifdef STRICT_RFC822_CHECK
  if (validate_time(p, "Aaa, ## Aaa #### ##:##:## GMT"))
#else
  if (validate_time(p, "Aaa, ##_Aaa_#### ##:##:##*"))
#endif
    {					/* RFC 822/1123 */
      tm.tm_mday = twodig(p+5);
      m = find_month(p+8);
      if (!m)
	goto unk;
      tm.tm_mon = m - 1;
      y = fourdig(p+12);
      if (y < 1970)
	goto unk;
      tm.tm_year = y - 1900;
      tm.tm_hour = twodig(p+17);
      tm.tm_min = twodig(p+20);
      tm.tm_sec = twodig(p+23);
      goto ook;
    }
  if (q = strchr(p, ','))
    {
      int flag = 0;
      if (validate_time(q, ", ##-Aaa-## ##:##:## GMT"))	/* RFC 850 */
        flag = 1;
      else if (validate_time(q, ", #-Aaa-## ##:##:## GMT"))	/* Incorrectly implemented RFC 850 */
	flag = 2;
      if (flag)
	{
	  tm.tm_mday = twodig(q+2);
	  if (flag == 2)
	    q--;
	  m = find_month(q+5);
	  if (!m)
	    goto unk;
	  tm.tm_mon = m - 1;
	  tm.tm_year = twodig(q+9);
	  if (tm.tm_year < 76)
	    tm.tm_year += 100;
	  tm.tm_hour = twodig(q+12);
	  tm.tm_min = twodig(q+15);
	  tm.tm_sec = twodig(q+18);
	  goto ook;
	}
    }
  if (validate_time(p, "Aaa Aaa $# ##:##:## ####"))
    {					/* ANSI C asctime() */
      m = find_month(p+4);
      if (!m)
	goto unk;
      tm.tm_mon = m - 1;
      tm.tm_mday = twodig(p+8);
      tm.tm_hour = twodig(p+11);
      tm.tm_min = twodig(p+14);
      tm.tm_sec = twodig(p+17);
      y = fourdig(p+20);
      if (y < 1980)
	goto unk;
      tm.tm_year = y - 1900;
      goto ook;
    }

unk:
  log(L_WARN_R, "Unable to parse date `%s'", buf);
  return 0;

ook:
  m = timegm(&tm);
  if (m == (time_t) -1)
    goto unk;
  if (!m)	/* 1. 1. 1970 (the UNIX Epoch) is a common date, but timestamp 0 is reserved, so shift it 1 sec forward */
    m = 1;
  return m;
}

/* Sending of all required headers */

static void
send_header(FILE *f, byte *host, uns port, byte *rest)
{
  byte buf[1536];
  byte *k;

  if (max_obj_size)
    {
      k = buf + sprintf(buf, "GET ");
      head_only = 0;
    }
  else
    {
      k = buf + sprintf(buf, "HEAD ");
      gobj_truncate();
      head_only = 1;
    }
  if (proxy[0])
    {
      k += sprintf(k, "http://%s", host);
      if (port != 80)
	k += sprintf(k, ":%d", port);
    }
  k += sprintf(k, "%s HTTP/1.1", rest);
  TRACE("> %s", buf);
  *k++ = '\r';
  *k++ = '\n';
  *k = 0;
  fputs(buf, f);
  if (port == 80)
    sendhdr(f, "Host: %s", host);
  else
    sendhdr(f, "Host: %s:%d", host, port);
  if (acc_types[0])
    sendhdr(f, "Accept: %s", acc_types);
  if (acc_charset[0])
    sendhdr(f, "Accept-Charset: %s", acc_charset);
  if (acc_encoding[0])
    sendhdr(f, "Accept-Encoding: %s", acc_encoding);
  if (acc_lang[0])
    sendhdr(f, "Accept-Language: %s", acc_lang);
  sendhdr(f, "Connection: close");
  if (local_admin[0])
    sendhdr(f, "From: %s", local_admin);
  if (referer[0])
    sendhdr(f, "Referer: %s", referer);
  if (gthis->if_modified_since_time)
    {
      byte buf[32];
      http_form_date(buf, gthis->if_modified_since_time);
      sendhdr(f, "If-Modified-Since: %s", buf);
    }
  if (gthis->if_different_etag)
    sendhdr(f, "If-None-Match: %s", gthis->if_different_etag);

  if (user_agent_cmt[0])
    sendhdr(f, "User-Agent: %s (%s)", user_agent, user_agent_cmt);
  else
    sendhdr(f, "User-Agent: %s", user_agent);

  if (gthis->auth_user && gthis->auth_pass)
  {
    uns srclen = strlen(gthis->auth_user) + strlen(gthis->auth_pass) + 1;
    uns destlen = BASE64_ENC_LENGTH(srclen);
    byte srcbuf[srclen + 1], destbuf[destlen + 1];
    sprintf(srcbuf, "%s:%s", gthis->auth_user, gthis->auth_pass);
    base64_encode(destbuf, srcbuf, srclen);
    destbuf[destlen] = 0;
    sendhdr(f, "Authorization: Basic %s", destbuf);
  }

  CLIST_FOR_EACH(simp_node *, uh, user_headers)
    sendhdr(f, "%s", uh->s);
  fputs("\r\n" ,f);
  fflush(f);
}

/* Receiving of all headers */

struct hdr {
  struct hdr *next;
  byte *contents;
  byte name[1];
};

static struct hdr *firsthdr;
static int response_code;
static byte *response_text;
static uns additional_hdr;

static void
parse_resphdr(byte *t)
{
  TRACE("< %s", t);
  if (!t[0])
    gerror(2114, "Missing response header");
  if (strncmp(t, "HTTP/", 5))
    gerror(2108, "Invalid response header");
  if (t[5] != '1' || t[6] != '.')
    gerror(2115, "Invalid HTTP version: %s", t);
  t = strchr(t, ' ');
  if (!t || !Cdigit(t[1]) || !Cdigit(t[2]) || !Cdigit(t[3]) || (t[4] != ' ' && t[4]))
    gerror(2116, "Invalid response header");
  response_code = (t[1] - '0')*100 + (t[2] - '0')*10 + t[3] - '0';
  t += 4;
  if (t[0] == ' ')
    t++;
  response_text = mp_strdup(gthis->pool, t);
}

static void
hdrflush(byte *x)
{
  byte *f;
  struct hdr *h;

  if (!*x)
    return;
  uns len = strlen(x);
  while (len > 0 && (x[len-1] == ' ' || x[len-1] == '\t'))
    x[--len] = 0;
  TRACE("%s %s", additional_hdr ? "<<" : "<", x);
  h = mp_alloc(gthis->pool, sizeof(struct hdr) + len);
  memcpy(h->name, x, len+1);
  h->next = firsthdr;
  firsthdr = h;
  f = h->name;
  while (*f && *f != ' ' && *f != '\t')
    f++;
  while (*f == ' ' || *f == '\t')
    *f++ = 0;
  h->contents = f;
}

static void
recv_header(FILE *f)
{
  byte buf[1024], buf2[1024];
  byte *k, *l;
  uns maxlin = 0;
  uns warned = 0;

  buf2[0] = 0;
  l = NULL;
  firsthdr = NULL;
  for(;;)
    {
next:
      if (maxlin++ > max_hdr_lines)
	gerror(2113, "Maximal number of header lines (%d) exceeded", max_hdr_lines);
      if (!fgets(buf, sizeof(buf), f))
	gerror(1109, "Unexpected close while scanning %sheader", additional_hdr ? "additional " : "");
      k = strchr(buf, '\r');
      if (!k)
	k = strchr(buf, '\n');
      if (!k)
	gerror(2110, "Header line too long");
      *k = 0;
      for (k = buf; *k; k++)
	if ((*k < 0x20 || *k >= 0x80) && *k != '\t')
	  {
	    if (!warned++)
	      log(L_WARN_R, "Found non-ASCII character(s) in HTTP header");
	    goto next;
	  }
      if (response_code < 0)
	{
	  parse_resphdr(buf);
	  continue;
	}
      if (!buf[0])
	break;
      if (buf[0] == ' ' || buf[0] == '\t')
	{
	  if (!l)
	    gerror(2112, "Invalid continuation line");
	  k = buf;
	  while (*k == ' ' || *k == '\t')
	    k++;
	  int len = strlen(k);
	  if (l + len + 2 >= buf2 + sizeof(buf2))
	    gerror(2110, "Header line too long after folding");
	  *l++ = ' ';
	  memcpy(l, k, len+1);
	  l += len;
	}
      else
	{
	  hdrflush(buf2);
	  strcpy(buf2, buf);
	  l = buf2 + strlen(buf2);
	}
    }
  hdrflush(buf2);
}

static byte *
findhdr(byte *hdr)
{
  struct hdr *h = firsthdr;

  while (h)
    {
      if (!strcasecmp(h->name, hdr))
	return h->contents;
      h = h->next;
    }
  return NULL;
}

/* Parsing and analysis of all incoming headers */

static void
parse_hdr(void)
{
  byte *l;

  uns code = response_code;
  switch (code)
    {
    case 304:				/* Not modified */
      if (gthis->if_modified_since_time || gthis->if_different_etag)
	gerror(3, "Not modified");
      else
	gerror(2137, "Unexpected HTTP status %d: %s", code, response_text);
    case 503:				/* Temporarily Unavailable */
      if (l = findhdr("Retry-After:"))
	obj_set_attr_num(gthis->aa, 'W', (int) atol(l));
      /* Fall-thru */
    case 504:				/* Gateway Timeout */
      gerror(1130, "HTTP error %d: %s", code, response_text);
    case 505:				/* HTTP version not supported */
      gerror(2133, "Downgrade HTTP");
    }

  if (code >= 300 && code < 400 && code != 305)	/* Redirect (305 == Use Proxy) */
    {
      if (l = findhdr("Location:"))
	{
	  /*
	   * HTTP RFC explicitly says redirects must not be relative, but they
	   * unfortunately are common practice, so we accept them.
	   */
	  gobj_add_ref('Y', l);
	}
      gobj_set_redirect("HTTP redirect %d: %s", code, response_text);
      return;
    }
  else if (code < 200 || code >= 300)	/* Not a `Success' code */
    gerror(2130, "HTTP error %d: %s", code, response_text);

  if (l = findhdr("Content-Base:"))
    gthis->base_url = gobj_parse_url(&gthis->base_url_s, l, "base", 0);
  else if (l = findhdr("Content-Location:"))
    gthis->base_url = gobj_parse_url(&gthis->base_url_s, l, "base", 1);
  set_content_encoding(findhdr("Content-Encoding:"));
  if (l = findhdr("Content-Language:"))
    {
      byte buf[MAX_LANG_LIST_SIZE];
      if (lang_normalize_list(buf, l) > 0)
	gthis->language = gstrdup(buf);
    }
  if (l = findhdr("Content-Type:"))
    {
      parse_content_type(l, &gthis->charset);
      set_content_type(l);
    }
  /* Let the core check whether we want the content type or not */
  gather_filter(0);
  if ((l = findhdr("Content-Length:")) && !head_only)
    {
      expected_length = atol(l);
      if ((uns)expected_length > max_obj_size && !allow_truncate)
	gerror(2135, "Object would be too large");
    }
  if (l = findhdr("Expires:"))
    gthis->expires_time = http_parse_date(l);
  if (l = findhdr("Last-Modified:"))
    {
      gthis->lastmod_time = http_parse_date(l);
      if (gthis->lastmod_time)
	{
	  if (gthis->if_modified_since_time && gthis->if_modified_since_time > gthis->lastmod_time)
	    gerror(3, "Not modified [hdr]");
	}
    }
  gthis->etag = findhdr("ETag:");
  gthis->http_server = findhdr("Server:");
  if (l = findhdr("Warning:"))
    log(L_WARN_R, "%s: HTTP Warning: %s", gthis->url, l);
  if (l = findhdr("Transfer-Encoding:"))
    {
      if (!strcasecmp(l, "chunked"))
	is_chunked = 1;
      else
	gerror(2121, "Unknown Transfer-Encoding: %s", l);
    }
}

/* Copying of message body */

static void
recv_body_normal(FILE *f)
{
  byte buf[16384];
  int sum = 0;

  for(;;)
    {
      int len = sizeof(buf);
      if (expected_length >= 0)
	{
	  len = MIN(len, expected_length - sum);
	  if (len <= 0)
	    {
	      /* We shouldn't wait for EOF as some buggy versions of IIS don't close the connection even though they MUST. */
	      break;
	    }
	}
      len = fread(buf, 1, len, f);
      if (len <= 0)
	break;
      sum += len;
      gthis->orig_size = sum;
      bwrite(gthis->temp, buf, len);
      if ((uns)sum > max_obj_size)
	{
	  gobj_truncate();
	  if (expected_length >= sum)
	    gthis->expected_size = expected_length;
	  return;
	}
    }
  if (expected_length >= 0 && expected_length != sum && !gthis->content_encoding)
    if (length_checks)
      gerror(1118, "Length mismatch (got %d, expected %d)", sum, expected_length);
    else
      log(L_WARN_R, "Length mismatch (got %d, expected %d)", sum, expected_length);
}

static void
recv_body_chunked(FILE *f)
{
  byte buf[16384];
  byte *x;
  uns len, blen, tlen;
  int c;

  TRACE("Using chunked encoding");
  tlen = 0;
  while (fgets(buf, 1024, f))
    {
      x = strchr(buf, '\r');
      if (!x || x[1] != '\n')
	break;
      *x = 0;
      len = 0;
      for(x=buf; Cxdigit(*x); x++)
	len = (len << 4) | Cxvalue(*x);
      if (*x && !Cblank(*x) && *x != ';')
	goto err;
      if (!len)
	{
	  additional_hdr = 1;
	  recv_header(f);
	  gthis->orig_size = tlen;
	  return;
	}
      while (len)
	{
	  if (len > sizeof(buf))
	    blen = sizeof(buf);
	  else
	    blen = len;
	  if (fread(buf, 1, blen, f) != blen)
	    goto unex;
	  bwrite(gthis->temp, buf, blen);
	  tlen += blen;
	  gthis->orig_size = tlen;
	  if (tlen > max_obj_size)
	    {
	      gobj_truncate();
	      return;
	    }
	  len -= blen;
	}
      c = fgetc(f);
      if (c == EOF)
	goto unex;
      if (c != '\r')
	goto err;
      c = fgetc(f);
      if (c == EOF)
	goto unex;
      if (c != '\n')
	goto err;
    }
unex:
  gerror(1123, "Unexpected close in chunked encoding");
  return;

err:
  gerror(2122, "Malformed chunked encoding");
}

static void
recv_body(FILE *f)
{
  if (is_chunked)
    recv_body_chunked(f);
  else
    recv_body_normal(f);
  alarm(0);
}

/* Download an object */

static void
download_url(byte *host, uns port, byte *rest)
{
  int sock;
  FILE *f;

  is_chunked = 0;
  expected_length = -1;
  additional_hdr = 0;
  response_code = -1;

  signal(SIGALRM, timeout);

  sock = http_connect(host, port);
  f = fdopen(sock, "w+");
  if (!f)
    die("fdopen failed (%m)");

  alarm(header_timeout);
  send_header(f, host, port, rest);
  recv_header(f);
  parse_hdr();
  if (gthis->error_code == 1)
    return;
  alarm(body_timeout);
  recv_body(f);
  fclose(f);
}

void
http_download(void)
{
  byte buf[MAX_URL_SIZE];
  int e;

  if (!local_admin[0])
    gerror(2136, "You forgot to set HTTP.From in the configuration");
  if (e = url_enescape(gthis->url_s.rest, buf))
    gerror(2000+e, "HTTP: Error parsing URL rest: %s", url_error(e));
  download_url(gthis->url_s.host, gthis->url_s.port, buf);
}
