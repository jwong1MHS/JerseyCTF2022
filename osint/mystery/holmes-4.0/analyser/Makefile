# Makefile for the Sherlock Analysers (c) 2007 Martin Mares <mj@ucw.cz>

DIRS+=analyser

ifdef CONFIG_ANALYSER

CONFIGS+=analyser

LIBANAL_MODS=analyser a-test a-iprange a-substr
LIBANAL_DEPS=

ifdef CONFIG_LANG
LIBANAL_MODS+=a-lang
LIBANAL_DEPS+=$(LIBLANG)
endif

ifneq ($(CONFIG_IMAGES_DUP)$(CONFIG_IMAGES_SIM),)
LIBANAL_MODS+=a-imagesig
LIBANAL_DEPS+=$(LIBIMAGES)
endif

$(o)/analyser/libanalyser.a: $(addsuffix .o,$(addprefix $(o)/analyser/,$(LIBANAL_MODS)))
$(o)/analyser/libanalyser.so: $(addsuffix .oo,$(addprefix $(o)/analyser/,$(LIBANAL_MODS)))
$(o)/analyser/libanalyser.pc: $(LIBSH) $(LIBANAL_DEPS) $(LIBCHARSET)

PROGS+=$(o)/analyser/atest

$(o)/analyser/atest: $(o)/analyser/atest.o $(LIBANAL) $(LIBSH)

TESTS+=$(addprefix $(o)/analyser/,a-substr.test a-iprange.test)

$(o)/analyser/a-substr.test: $(o)/analyser/atest
$(o)/analyser/a-iprange.test: $(o)/analyser/atest

API_LIBS+=libanalyser
run/lib/pkgconfig/libanalyser.pc: $(o)/analyser/libanalyser.pc

endif
