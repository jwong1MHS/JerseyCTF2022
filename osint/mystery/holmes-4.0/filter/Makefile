# Makefile for the Sherlock Filter Language (c) 2001--2007 Martin Mares <mj@ucw.cz>

DIRS+=filter
PROGS+=$(o)/filter/filter-test

LIBFILTER_MODS=lex parse.tab filter prune builtin hashes kmp tries trees dumper fconfig
LIBFILTER_INCLUDES=filter.h

$(o)/filter/libfilter.a: $(addsuffix .o,$(addprefix $(o)/filter/,$(LIBFILTER_MODS)) $(CUSTOM_FILTER_MODULES))
$(o)/filter/libfilter.so: $(addsuffix .oo,$(addprefix $(o)/filter/,$(LIBFILTER_MODS)) $(CUSTOM_FILTER_MODULES))
$(o)/filter/libfilter.pc: $(LIBSH)

$(o)/filter/ftest: $(o)/filter/ftest.o $(LIBFILTER) $(LIBSH)
$(o)/filter/filter-test: $(o)/filter/filter-test.o $(LIBFILTER) $(LIBSH)
$(o)/filter/filter-example: $(o)/filter/filter-example.o $(LIBFILTER) $(LIBSH)

$(o)/filter/parse.tab.o $(o)/filter/parse.tab.oo: CWARNS+=-Wno-sign-compare -Wno-redundant-decls -Wno-undef
$(o)/filter/lex.o $(o)/filter/lex.oo: CWARNS+=-Wno-sign-compare -Wno-redundant-decls -Wno-missing-prototypes -Wno-unused-function -Wno-undef

$(addsuffix .o,$(addprefix $(o)/filter/,$(LIBFILTER_MODS))) \
$(addsuffix .oo,$(addprefix $(o)/filter/,$(LIBFILTER_MODS))): $(o)/filter/parse.tab.h

$(o)/filter/parse.tab.h: $(o)/filter/parse.tab.c
	$(Q)touch $@

$(o)/filter/parse.tab.c: $(s)/filter/parse.y
	$(M)BISON $^
	$(Q)bison -dv $^ -o $@

$(o)/filter/lex.c: $(s)/filter/lex.l
	$(M)FLEX $^
	$(Q)flex -s -B -8 -o$@ $^

API_LIBS+=libfilter
API_INCLUDES+=$(o)/filter/.include-stamp
$(o)/filter/.include-stamp: $(addprefix $(s)/filter/,$(LIBFILTER_INCLUDES))
$(o)/filter/.include-stamp: IDST=filter
run/lib/pkgconfig/libfilter.pc: $(o)/filter/libfilter.pc
