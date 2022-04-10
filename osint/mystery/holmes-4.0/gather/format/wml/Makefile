# Makefile for the Sherlock Parsers
# (c) 2007 Pavel Charvat <pchar@ucw.cz>

DIRS+=gather/format/wml
LIBPARSE_MODS+=format/wml/wml format/wml/tags
LIBPARSE_DEPS+=$(LIBSHXML)

$(o)/gather/format/wml/tags.c: $(s)/gather/format/wml/tags.k $(o)/build/genhash
	$(M)HASH $@
	$(Q)$(o)/build/genhash $< $@ is_wml_tag

# Shut up incomplete initializer warnings from newer versions of gcc
$(o)/gather/format/wml/tags.o $(o)/gather/format/wml/tags.oo: CWARNS=$(CWARNS_OFF)
