/*
 *	Sherlock Filter Engine -- Configuration
 *
 *	(c) 2002--2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "filter/filter.h"

uns filter_trace = 0;
uns filter_hash_limit = 4;
uns filter_kmp_limit = 4;
uns filter_trie_limit = 4;
uns filter_tree_limit = 4;
uns filter_optimize = 1;
char *filter_dump_to = NULL;

static struct cf_section fconfig = {
	CF_ITEMS {
		CF_UNS("Trace", &filter_trace),
		CF_UNS("HashLimit", &filter_hash_limit),
		CF_UNS("KMPLimit", &filter_kmp_limit),
		CF_UNS("TrieLimit", &filter_trie_limit),
		CF_UNS("TreeLimit", &filter_tree_limit),
		CF_UNS("Optimize", &filter_optimize),
		CF_STRING("DumpFilterTo", &filter_dump_to),
		CF_END
	}
};

static void CONSTRUCTOR fconfig_init(void)
{
	cf_declare_section("Filter", &fconfig, 0);
}
