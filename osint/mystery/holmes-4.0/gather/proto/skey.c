/*
 *	Sherlock Gatherer: Server Key Resolver
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/ipaccess.h"
#include "ucw/hashfunc.h"
#include "gather/gather.h"

#include <stdlib.h>
#include <netdb.h>
#include <netinet/in.h>

#define TRACE(x,y...) do { if (trace_resolve) log(L_DEBUG, "Resolve: " x,##y); } while (0)

static void
add_skey_attr(u32 addr)
{
  struct oattr *a, *olda;

  a = obj_add_attr_format(gthis->aa, 'k', "%08x", addr);
  if (gthis->refreshing && (olda = obj_find_attr(gthis->refreshing, 'k')) && strcmp(a->val, olda->val))
    {
      /*
       * If the skey differs, force full download, because we want to store a new version
       * anyway, and also the timestamps and etags are not likely to be consistent in such cases.
       */
      gthis->if_modified_since_time = 0;
      gthis->if_different_etag = 0;
    }
}

u32
resolve_host_name(byte *name)
{
  struct hostent *h = gethostbyname(name);
  if (!h)
    {
      switch (h_errno)
	{
	case HOST_NOT_FOUND:
	case NO_ADDRESS:
	  gerror(2103, "Host doesn't exist");
	case TRY_AGAIN:
	  gerror(1104, "DNS timeout");
	default:
	  gerror(2105, "Unrecoverable DNS error");
	}
    }

  uns cnt = 0;
  while (h->h_addr_list[cnt] && cnt < 256)
    cnt++;
  u32 addrs[cnt];

  uns n = 0;
  for (uns i=0; i < cnt; i++)
    {
      u32 addr;
      memcpy(&addr, h->h_addr_list[i], sizeof(addr));
      u32 ha = ntohl(addr);
      if (ipaccess_check(&gaccess_list, ha))
	{
	  addrs[n++] = ha;
	  TRACE("Allowed address %08x", ha);
	}
      else
	TRACE("Forbidden address %08x", ha);
    }
  if (!n)
    {
      byte *a = h->h_addr;
      gerror(2134, "No valid IP address (%d.%d.%d.%d forbidden)", a[0], a[1], a[2], a[3]);
    }

  u32 addr = addrs[0];
  if (n > 1)				/* If there are multiple addresses, choose deterministically */
    {
      for (uns i=n; i>0; i--)
	for (uns j=0; j<i-1; j++)
	  if (addrs[j] > addrs[j+1])
	    {
	      addr = addrs[j];
	      addrs[j] = addrs[j+1];
	      addrs[j+1] = addr;
	    }
      addr = addrs[hash_string(name) % n];
    }
  TRACE("Chosen address %08x", addr);

  add_skey_attr(addr);
  return htonl(addr);
}

void
gather_create_key(void)
{
  switch (gthis->url_s.protoid)
    {
    case URL_PROTO_HTTP:
      resolve_host_name(gthis->url_s.host);
      break;
    default:
      add_skey_attr(0x7f010000 + hash_string(gthis->url) % 1024);
    }
  gerror(2, "Resolved");
}
