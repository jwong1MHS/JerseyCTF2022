# Makefile for the Sherlock PDF Parser (c) 2002 Milan Vancura <milan@ucw.cz>

DIRS+=gather/format/pdf

LIBPARSE_MODS+=$(addprefix format/pdf/,pdf glyphs adobeencs crypt lex stream fonts)

ifdef CONFIG_DEBUG_TOOLS
PROGS+=$(o)/gather/format/pdf/pdfdump
endif

$(o)/gather/format/pdf/glyphs.c: $(s)/gather/format/pdf/glyphs.k $(o)/build/genhash
	$(M)HASH $@
	$(Q)$(o)/build/genhash $< $@ is_glyph

# Shut up incomplete initializer warnings from newer versions of gcc
$(o)/gather/format/pdf/glyphs.o $(o)/gather/format/pdf/glyphs.oo: CWARNS=$(CWARNS_OFF)

$(o)/gather/format/pdf/pdfdump: $(o)/gather/format/pdf/pdf.o $(LIBPARSE) $(LIBGATH) $(LIBCUSTOM) $(LIBSH)
$(o)/gather/format/pdf/pdfdump: LIBS+=$(LIBPARSE_LIBS) $(LIBGATH_LIBS)
$(o)/gather/format/pdf/pdfdump.o: $(s)/gather/format/pdf/pdfdump.c $(s)/gather/format/pdf/pdf.c
