# Makefile for the Sherlock Library (c) 2004--2007 Martin Mares <mj@ucw.cz>

DIRS+=sherlock
SHPROGS=$(o)/sherlock/buckettool

ifdef CONFIG_DBTOOL
SHPROGS+=$(o)/sherlock/db-tool
endif
PROGS+=$(SHPROGS)

LIBSH_MODS= \
	object bucket buck2obj obj2buck obj-linear obj-format \
	lizard-fb \
	attrset conf-parse \
	db pagecache \
	query

ifdef CONFIG_SHERLOCK
LIBSH_MODS+=urlkey finger
endif

LIBSH_INCLUDES=sherlock.h attrset.h bucket.h conf.h lizard-fb.h object.h \
        objread.h tagged-text.h query.h math.h db.h pagecache.h

LIBSH_MOD_PATHS=$(addprefix $(o)/sherlock/,$(LIBSH_MODS))

$(o)/sherlock/libsh.a: $(addsuffix .o,$(LIBSH_MOD_PATHS))
$(o)/sherlock/libsh.so: $(addsuffix .oo,$(LIBSH_MOD_PATHS))
$(o)/sherlock/libsh.pc: $(LIBUCW)

export CUSTOM_LIB_EXTRAS
$(o)/sherlock/libcustom.a: $(addsuffix .o,$(CUSTOM_LIB_MODULES))
$(o)/sherlock/libcustom.so: $(addsuffix .oo,$(CUSTOM_LIB_MODULES))
$(o)/sherlock/libcustom.pc: $(LIBSH) $(CUSTOM_LIB_DEPS)

$(o)/sherlock/buckettool: $(o)/sherlock/buckettool.o $(LIBSH)

$(o)/sherlock/db-test: $(o)/sherlock/db-test.o $(LIBSH)
$(o)/sherlock/db-tool: $(o)/sherlock/db-tool.o $(LIBSH)

API_LIBS+=libsh
API_INCLUDES+=$(o)/sherlock/.include-stamp
$(o)/sherlock/.include-stamp: $(addprefix $(s)/sherlock/,$(LIBSH_INCLUDES))
$(o)/sherlock/.include-stamp: IDST=sherlock
run/lib/pkgconfig/libsh.pc: $(o)/sherlock/libsh.pc

include $(s)/sherlock/perl/Makefile

ifdef CONFIG_XML
include $(s)/sherlock/xml/Makefile
endif

CONFIGS+=libsherlock

INSTALL_TARGETS+=install-libsh
install-libsh:
	install -d -m 755 $(DESTDIR)$(INSTALL_BIN_DIR) $(DESTDIR)$(INSTALL_LIB_DIR) $(DESTDIR)$(INSTALL_PKGCONFIG_DIR) $(DESTDIR)$(INSTALL_INCLUDE_DIR)/sherlock $(DESTDIR)$(INSTALL_CONFIG_DIR)
	install -m 755 $(SHPROGS) $(DESTDIR)$(INSTALL_BIN_DIR)
	install -m 644 run/lib/libsh.$(LS) $(DESTDIR)$(INSTALL_LIB_DIR)
	install -m 644 $(addprefix run/include/sherlock/,$(LIBSH_INCLUDES)) $(DESTDIR)$(INSTALL_INCLUDE_DIR)/sherlock
	install -m 644 run/lib/pkgconfig/libsh.pc $(DESTDIR)$(INSTALL_PKGCONFIG_DIR)
	install -m 644 run/$(CONFIG_DIR)/libsherlock $(DESTDIR)$(INSTALL_CONFIG_DIR)

.PHONY: install-libsh
