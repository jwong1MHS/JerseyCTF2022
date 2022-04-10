# Makefile for the Sherlock Search Server (c) 2001--2007 Martin Mares <mj@ucw.cz>

DIRS+=search
PROGS+=$(o)/search/sherlockd
CONFIGS+=sherlockd

SS_OBJS=sherlockd.o config.o dbase.o reply.o query.o lex.o parse.tab.o cards.o words.o strings.o memory.o \
	refs.o cmds.o lexicon.o spell.o vocabolario.o fulltext.o
SI_OBJS=alphabet.o

ifdef CONFIG_SITES
SI_OBJS+=sites.o
endif

ifdef CONFIG_IMAGES_SIM
SS_OBJS+=images.o
SS_IMAGES=$(LIBIMAGES)
endif

$(o)/search/sherlockd: $(addprefix $(o)/search/,$(SS_OBJS)) $(addprefix $(o)/indexer/,$(SI_OBJS)) $(SS_IMAGES) $(LIBLANG) $(LIBCHARSET) $(LIBCUSTOM) $(LIBSH)

$(o)/search/lex.o: $(o)/search/parse.tab.h
$(o)/search/parse.tab.o $(o)/search/parse.tab.oo: CWARNS+=-Wno-sign-compare -Wno-redundant-decls -Wno-undef

$(addprefix $(o)/search/,words.o refs.o cards.o): COPT+=$(COPT2)

$(o)/search/parse.tab.h: $(o)/search/parse.tab.c
	$(Q)touch $@

$(o)/search/parse.tab.c: $(s)/search/parse.y
	$(M)BISON $^
	$(Q)bison -dv $^ -o $@
