# Debugging utilities which are intended solely for authors' internal use

DIRS+=debug
PROGS+=$(addprefix $(o)/debug/,\
	whenis sample cols histogram random-access hex \
	log-times log-qsplit log-ssstats \
	find-cycles mkgraphidx find-unreachable visualize-site compare-lang count-domains)

$(o)/debug/random-access: $(o)/debug/random-access.o $(LIBSH)
$(o)/debug/sample: $(o)/debug/sample.o $(LIBSH)
$(o)/debug/histogram: $(o)/debug/histogram.o $(LIBSH)
$(o)/debug/flashdis: $(o)/debug/flashdis.o $(LIBSH)
$(o)/debug/ffstest: $(o)/debug/ffstest.o $(LIBSH)
$(o)/debug/refs-analyse: $(o)/debug/refs-analyse.o $(o)/indexer/iconfig.o $(LIBSH) -lm
$(o)/debug/bmap: $(o)/debug/bmap.o $(LIBSH)
$(o)/debug/mkgraphidx: $(o)/debug/mkgraphidx.o $(LIBSH)
$(o)/debug/noop: $(o)/debug/noop.o $(LIBUCW)
$(o)/debug/noop: LIBS+=-lpthread
$(o)/debug/find-unreachable: $(o)/debug/find-unreachable.o $(LIBINDEXER) $(LIBSH)
$(o)/debug/cachesim: $(o)/debug/cachesim.o $(LIBUCW)
$(o)/debug/shdd: $(o)/debug/shdd.o $(LIBUCW)
$(o)/debug/shot-answer-urls: $(s)/debug/shot-answer-urls.pl
$(o)/debug/shot-stats: $(o)/debug/shot-stats.o $(LIBSH)
$(o)/debug/visualize-site: $(s)/debug/visualize-site.pl
$(o)/debug/compare-lang: $(o)/debug/compare-lang.o $(LIBINDEXER) $(LIBLANG) $(LIBSH)
$(o)/debug/count-domains: $(s)/debug/count-domains.pl
$(o)/debug/hex: $(o)/debug/hex.o $(LIBUCW)

ifdef CONFIG_WEIGHTS
PROGS+=$(o)/debug/pagerank
$(o)/debug/pagerank: $(o)/debug/pagerank.o $(LIBINDEXER) $(LIBSH)
endif

include $(s)/debug/sorter/Makefile
