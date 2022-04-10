# Makefile for the Sherlock Language Processing Modules (c) 2003--2007 Martin Mares <mj@ucw.cz>

DIRS+=lang
LIBLANG_CONFIGS=lang

# The basic modules are compiled even when CONFIG_LANG is not enabled
LIBLANG_MODS=lang
LIBLANG_INCLUDES=lang.h

ifdef CONFIG_LANG

LIBLANG_PROGS=$(o)/lang/stem-table-gen $(o)/lang/stemtest $(o)/lang/stem-dict-gen
LIBLANG_MODS+=stemmer stem-porter stem-table stem-dict syndict

ifdef CONFIG_LANG_DETECT
LIBLANG_PROGS+=$(o)/lang/lang-tables
LIBLANG_CONFIGS+=lang-detect
LIBLANG_MODS+=detect
LIBLANG_INCLUDES+=detect.h
endif

API_LIBS+=liblang
API_INCLUDES+=$(o)/lang/.include-stamp
$(o)/lang/.include-stamp: $(addprefix $(s)/lang/,$(LIBLANG_INCLUDES))
$(o)/lang/.include-stamp: IDST=lang
run/lib/pkgconfig/liblang.pc: $(o)/lang/liblang.pc

endif

PROGS+=$(LIBLANG_PROGS)
CONFIGS+=$(LIBLANG_CONFIGS)

$(o)/lang/liblang.a: $(addsuffix .o,$(addprefix $(o)/lang/,$(LIBLANG_MODS)) $(LIBLANG_EXTRAS))
$(o)/lang/liblang.so: $(addsuffix .oo,$(addprefix $(o)/lang/,$(LIBLANG_MODS)) $(LIBLANG_EXTRAS))
$(o)/lang/liblang.pc: $(LIBUCW) $(LIBCHARSET)

$(o)/lang/stemtest: $(o)/lang/stemtest.o $(LIBLANG) $(LIBCHARSET) $(LIBUCW)
$(o)/lang/lang-tables: $(o)/lang/lang-tables.o $(o)/indexer/getbuck.o $(o)/indexer/iconfig.o $(LIBFILTER) $(LIBLANG) $(LIBCHARSET) $(LIBSH)
$(o)/lang/stem-table-gen: $(o)/lang/stem-table-gen.o $(LIBUCW)
$(o)/lang/stem-dict-gen: $(o)/lang/stem-dict-gen.o $(LIBCHARSET) $(LIBUCW)

INSTALL_TARGETS+=install-liblang
install-liblang:
	install -d -m 755 $(DESTDIR)$(INSTALL_INCLUDE_DIR)/lang $(DESTDIR)$(INSTALL_LIB_DIR) $(DESTDIR)$(INSTALL_PKGCONFIG_DIR) $(DESTDIR)$(INSTALL_BIN_DIR) $(DESTDIR)$(INSTALL_CONFIG_DIR)
	install -m 755 $(LIBLANG_PROGS) $(DESTDIR)$(INSTALL_BIN_DIR)
	install -m 644 $(addprefix run/$(CONFIG_DIR)/,$(LIBLANG_CONFIGS)) $(DESTDIR)$(INSTALL_CONFIG_DIR)
	install -m 644 $(addprefix run/include/lang/,$(LIBLANG_INCLUDES)) $(DESTDIR)$(INSTALL_INCLUDE_DIR)/lang
	install -m 644 run/lib/liblang.$(LS) $(DESTDIR)$(INSTALL_LIB_DIR)
	install -m 644 run/lib/pkgconfig/liblang.pc $(DESTDIR)$(INSTALL_PKGCONFIG_DIR)

.PHONY: install-liblang
