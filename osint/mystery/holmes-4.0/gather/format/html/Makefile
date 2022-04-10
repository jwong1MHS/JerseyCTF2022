# Makefile for the Sherlock HTML Parser (c) 2001 Martin Mares <mj@ucw.cz>

DIRS+=gather/format/html

LIBPARSE_MODS+=format/html/html format/html/entities format/html/tags

$(o)/gather/format/html/entities.c: $(s)/gather/format/html/entities.k $(o)/build/genhash
	$(M)HASH $@
	$(Q)$(o)/build/genhash $< $@ is_entity

$(o)/gather/format/html/tags.c: $(s)/gather/format/html/tags.k $(o)/build/genhash
	$(M)HASH $@
	$(Q)$(o)/build/genhash $< $@ is_tag

# Shut up incomplete initializer warnings from newer versions of gcc
$(o)/gather/format/html/entities.o $(o)/gather/format/html/entities.oo: CWARNS=$(CWARNS_OFF)
$(o)/gather/format/html/tags.o $(o)/gather/format/html/tags.oo: CWARNS=$(CWARNS_OFF)

TESTS+=$(o)/gather/format/html/html.test
$(o)/gather/format/html/html.test: $(o)/gather/format/parser-test
