# Watson statistics makefile
# (c) 2004-2005 Tomas Valla <tom@ucw.cz>
# (c) 2008-2009 Pavel Charvat <pchar@ucw.cz>

DIRS+=utils/watson/graph
PROGS+=$(addprefix $(o)/utils/watson/graph/, \
	gatherd-cummulative \
	gatherd-performance \
	gatherd-performance_errors \
	gatherd-refresh_spectrum \
	gatherd-spectrum \
	indexer-documents \
	indexer-filetypes_in \
	indexer-filetypes_out \
	indexer-index_size \
	indexer-languages \
	indexer-lexicon \
	indexer-runtime \
	status-indexer \
	search-avg_inter_query \
	search-avg_load \
	search-avg_qr_proctime \
	search-cache_efficiency \
	search-max_qr_proctime \
	search-qr_errors \
	search-queries_sec \
)
ifdef CONFIG_SHEPHERD_PROTOCOL
PROGS+=$(addprefix $(o)/utils/watson/graph/, \
	shepherd-avg_sizes \
	shepherd-bucket_size \
	shepherd-contribs \
	shepherd-perf_util \
	shepherd-planning \
	shepherd-reap_performance \
	shepherd-reap_qkeys \
	shepherd-reap_queues \
	shepherd-sections \
	shepherd-select_actions \
	shepherd-select_urls \
	shepherd-sites \
	status-shepherd \
)
endif

WA_GR=lib/cgi-bin/graph
WA_GS=$(o)/utils/watson/graph
EXTRA_RUNDIRS+=$(WA_GR)

$(WA_GS)/gatherd-cummulative: BINDIR=$(WA_GR)
$(WA_GS)/gatherd-performance: BINDIR=$(WA_GR)
$(WA_GS)/gatherd-performance_errors: BINDIR=$(WA_GR)
$(WA_GS)/gatherd-refresh_spectrum: BINDIR=$(WA_GR)
$(WA_GS)/gatherd-spectrum: BINDIR=$(WA_GR)
$(WA_GS)/indexer-documents: BINDIR=$(WA_GR)
$(WA_GS)/indexer-filetypes_in: BINDIR=$(WA_GR)
$(WA_GS)/indexer-filetypes_out: BINDIR=$(WA_GR)
$(WA_GS)/indexer-index_size: BINDIR=$(WA_GR)
$(WA_GS)/indexer-languages: BINDIR=$(WA_GR)
$(WA_GS)/indexer-lexicon: BINDIR=$(WA_GR)
$(WA_GS)/indexer-runtime: BINDIR=$(WA_GR)
$(WA_GS)/status-indexer: BINDIR=$(WA_GR)
$(WA_GS)/search-avg_inter_query: BINDIR=$(WA_GR)
$(WA_GS)/search-avg_load: BINDIR=$(WA_GR)
$(WA_GS)/search-avg_qr_proctime: BINDIR=$(WA_GR)
$(WA_GS)/search-cache_efficiency: BINDIR=$(WA_GR)
$(WA_GS)/search-max_qr_proctime: BINDIR=$(WA_GR)
$(WA_GS)/search-qr_errors: BINDIR=$(WA_GR)
$(WA_GS)/search-queries_sec: BINDIR=$(WA_GR)

ifdef CONFIG_SHEPHERD_PROTOCOL
$(WA_GS)/shepherd-avg_sizes: BINDIR=$(WA_GR)
$(WA_GS)/shepherd-bucket_size: BINDIR=$(WA_GR)
$(WA_GS)/shepherd-contribs: BINDIR=$(WA_GR)
$(WA_GS)/shepherd-perf_util: BINDIR=$(WA_GR)
$(WA_GS)/shepherd-planning: BINDIR=$(WA_GR)
$(WA_GS)/shepherd-reap_performance: BINDIR=$(WA_GR)
$(WA_GS)/shepherd-sections: BINDIR=$(WA_GR)
$(WA_GS)/shepherd-select_actions: BINDIR=$(WA_GR)
$(WA_GS)/shepherd-select_urls: BINDIR=$(WA_GR)
$(WA_GS)/shepherd-sites: BINDIR=$(WA_GR)
$(WA_GS)/status-shepherd: BINDIR=$(WA_GR)
endif
