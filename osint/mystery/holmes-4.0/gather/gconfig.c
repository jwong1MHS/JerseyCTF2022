/*
 *	Sherlock Gatherer -- Gatherer Configuration
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/ipaccess.h"
#include "gather/gather.h"

uns max_obj_size = ~0;
uns allow_truncate = 1;
uns max_decode_size = ~0;
uns trace_decode;
uns log_ref_errors;
uns log_base_errors;
uns min_summed_size;
clist gaccess_list;
int min_ims_delay;
uns max_refresh_age = ~0U;
char *gather_filter_name;
uns gather_min_compression = 100;
uns trace_resolve;
uns max_parser_alloc = ~0U;

static struct cf_section gatherer_config = {
  CF_ITEMS {
    CF_UNS("MaxObjSize", &max_obj_size),
    CF_UNS("AllowTruncate", &allow_truncate),
    CF_UNS("MaxDecodeSize", &max_decode_size),
    CF_UNS("TraceDecode", &trace_decode),
    CF_STRING( "Filter", &gather_filter_name),
    CF_UNS("LogRefErrors", &log_ref_errors),
    CF_UNS("LogBaseErrors", &log_base_errors),
    CF_UNS("MinSummedSize", &min_summed_size),
    CF_LIST("AccessIP", &gaccess_list, &ipaccess_cf),
    CF_INT("MinIMSDelay", &min_ims_delay),
    CF_UNS("MaxRefreshAge", &max_refresh_age),
    CF_UNS("MinCompression", &gather_min_compression),
    CF_UNS("TraceResolve", &trace_resolve),
    CF_UNS("MaxParserAlloc", &max_parser_alloc),
    CF_END
  }
};

static void CONSTRUCTOR gconf_init(void)
{
  cf_declare_section("Gather", &gatherer_config, 0);
}
