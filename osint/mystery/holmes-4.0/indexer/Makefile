# Makefile for the Sherlock Indexer (c) 2001--2004 Martin Mares <mj@ucw.cz>

DIRS+=indexer

ifdef CONFIG_INDEXER

PROGS+=$(addprefix $(o)/indexer/,indexer scanner merger labelsort mklex \
chewer wsort ssort psort lexorder lexsort lexfreq seal black-gen attrsort export-feedback)
CONFIGS+=indexer

ifndef CONFIG_BARE
PROGS+=$(addprefix $(o)/indexer/,mergesums mergesigns keywords mergefp ireport reftexts \
backlinker mkgraph fpsort)
endif

ifdef CONFIG_SITES
PROGS+=$(o)/indexer/sitefinder
$(o)/indexer/sitefinder: $(o)/indexer/sitefinder.o $(LIBINDEXER) $(LIBSH)
endif

ifdef CONFIG_WEIGHTS
PROGS+=$(o)/indexer/weights
$(o)/indexer/weights: $(o)/indexer/weights.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/weights: LIBS+=-lm
endif

ifdef CONFIG_SHEPHERD_PROTOCOL
PROGS+=$(o)/indexer/iconnect $(o)/indexer/feedback-gath
$(o)/indexer/iconnect: $(o)/indexer/iconnect.o $(o)/gather/shepherd/protocol.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/feedback-gath: $(o)/indexer/feedback-gath.o $(o)/gather/shepherd/protocol.o $(LIBINDEXER) $(LIBSH)
endif

ifdef CONFIG_IMAGES_DUP
PROGS+=$(o)/indexer/mergeimages
$(o)/indexer/mergeimages: $(o)/indexer/mergeimages.o $(LIBINDEXER) $(LIBIMAGES) $(LIBSH)
endif

ifdef CONFIG_IMAGES_SIM
PROGS+=$(o)/indexer/imagesigs
$(o)/indexer/imagesigs: $(o)/indexer/imagesigs.o $(LIBINDEXER) $(LIBIMAGES) $(LIBSH)
endif

endif

LIBINDEXER_MODS=iconfig access fetch getbuck resolve
ifdef CONFIG_SITES
LIBINDEXER_MODS+=sites
endif

$(o)/indexer/libindexer.a: $(addsuffix .o,$(addprefix $(o)/indexer/,$(LIBINDEXER_MODS)))
$(o)/indexer/libindexer.so: $(addsuffix .oo,$(addprefix $(o)/indexer/,$(LIBINDEXER_MODS)))
$(o)/indexer/libindexer.pc: $(LIBSH)

$(o)/indexer/scanner: $(o)/indexer/scanner.o $(o)/indexer/matcher.o $(o)/indexer/alphabet.o $(o)/indexer/lexicon.o \
	$(o)/indexer/getbuck.o $(LIBINDEXER) $(LIBFILTER) $(LIBANAL) $(LIBLANG) $(LIBCHARSET) $(LIBCUSTOM) $(LIBSH)
$(o)/indexer/scanner: LIBS+=-lm
$(o)/indexer/mergefp: $(o)/indexer/mergefp.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/mergesums: $(o)/indexer/mergesums.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/mergesigns: $(o)/indexer/mergesigns.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/fpsort: $(o)/indexer/fpsort.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/merger: $(o)/indexer/merger.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/labelsort: $(o)/indexer/labelsort.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/mklex: $(o)/indexer/mklex.o $(o)/indexer/lexicon.o $(o)/indexer/alphabet.o $(LIBINDEXER) $(LIBLANG) $(LIBCHARSET) $(LIBCUSTOM) $(LIBSH)
$(o)/indexer/chewer: $(o)/indexer/chewer.o $(o)/indexer/lexicon.o $(o)/indexer/alphabet.o $(LIBINDEXER) $(LIBANAL) $(LIBCHARSET) $(LIBCUSTOM) $(LIBSH)
$(o)/indexer/chewer: LIBS+=-lm
$(o)/indexer/wsort: $(o)/indexer/wsort.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/ssort: $(o)/indexer/ssort.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/psort: $(o)/indexer/psort.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/mkgraph: $(o)/indexer/mkgraph.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/backlinker: $(o)/indexer/backlinker.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/backlinker: LIBS+=-lm
$(o)/indexer/reftexts: $(o)/indexer/reftexts.o $(LIBINDEXER) $(LIBCHARSET) $(LIBSH)
$(o)/indexer/reftexts: LIBS+=-lm
$(o)/indexer/lexmap-test: $(o)/indexer/lexmap-test.o $(o)/indexer/lexicon.o $(o)/indexer/alphabet.o $(LIBCHARSET) $(LIBSH)
$(o)/indexer/lexorder: $(o)/indexer/lexorder.o $(LIBINDEXER) $(o)/indexer/lexicon.o $(LIBLANG) $(LIBCHARSET) $(LIBSH)
$(o)/indexer/lexorder: LIBS+=-lm
$(o)/indexer/lexsort: $(o)/indexer/lexsort.o $(LIBINDEXER) $(o)/indexer/lexicon.o $(LIBCHARSET) $(LIBSH)
$(o)/indexer/lexfreq: $(o)/indexer/lexfreq.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/ireport: $(o)/indexer/ireport.o $(LIBINDEXER) $(LIBLANG) $(LIBCHARSET) $(LIBCUSTOM) $(LIBSH)
$(o)/indexer/keywords: $(o)/indexer/keywords.o $(o)/indexer/lexicon.o $(o)/indexer/alphabet.o $(LIBINDEXER) $(LIBCHARSET) $(LIBSH)
$(o)/indexer/seal: $(o)/indexer/seal.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/black-gen: $(o)/indexer/black-gen.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/attrsort: $(o)/indexer/attrsort.o $(LIBINDEXER) $(LIBSH)

$(o)/indexer/getbuck-t: $(o)/indexer/getbuck-tt.o $(LIBINDEXER) $(LIBSH)
$(o)/indexer/fetch-t: $(o)/indexer/fetch-tt.o $(LIBINDEXER) $(LIBSH)
