/*
 *	Sherlock Gatherer: Protocol Multiplexer
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/gather.h"

#include <stdlib.h>

typedef void (*downloader_t)(void);

static downloader_t downloaders[URL_PROTO_MAX] = {
  [URL_PROTO_HTTP] =	http_download,
  [URL_PROTO_FILE] =	file_download,
};

void
gather_download(void)
{
  if (!downloaders[gthis->url_s.protoid])
    gerror(2100, "Unknown protocol");
  gather_filter(0);
  if (gthis->refreshing)
    {
      byte *last_dwn = obj_find_aval(gthis->refreshing, 'D');
      byte *last_mod = obj_find_aval(gthis->refreshing, 'L');
      if (last_dwn &&
	  gthis->start_time - (ucw_time_t)atol(last_dwn) <= max_refresh_age &&
	  !gather_analysis_needed())
	{
	  if (last_mod && min_ims_delay >= 0)
	    gthis->if_modified_since_time = atol(last_mod) + min_ims_delay;
	  gthis->if_different_etag = obj_find_aval(gthis->refreshing, 'g');
	}
    }
  gthis->temp = fbmem_create(16384);
  downloaders[gthis->url_s.protoid]();
  bflush(gthis->temp);
  gthis->contents = gthis->temp;
  gthis->temp = NULL;
}
