# Makefile for the Sherlock Parsers (c) 2001 Martin Mares <mj@ucw.cz>

DIRS+=gather/format

LIBGATH_MODS+=$(addprefix format/,format)

LIBPARSE_MODS=$(addprefix format/,parse text zlib zip unlzw robots sink validate external fb-textpacker alloc)
LIBPARSE_INCLUDES=parsers.h
LIBPARSE_DEPS=$(LIBGATH)
PROGS+=$(o)/gather/format/ps-parse

ifdef CONFIG_DEBUG_TOOLS
PROGS+=$(o)/gather/format/parser-test
endif

ifdef CONFIG_IMAGES
LIBPARSE_MODS+=format/image
LIBPARSE_DEPS+=$(LIBIMAGES)
endif

ifdef CONFIG_PDF
include $(s)/gather/format/pdf/Makefile
endif

include $(s)/gather/format/html/Makefile

ifdef CONFIG_MSWORD
include $(s)/gather/format/msword/Makefile
endif

ifdef CONFIG_EXCEL
include $(s)/gather/format/excel/Makefile
endif

ifneq ($(CONFIG_MP3)$(CONFIG_OGG),)
LIBPARSE_MODS+=format/audio
endif

ifdef CONFIG_MP3
include $(s)/gather/format/mp3/Makefile
endif

ifdef CONFIG_OGG
include $(s)/gather/format/ogg/Makefile
endif

ifdef CONFIG_WML
include $(s)/gather/format/wml/Makefile
endif

$(o)/gather/format/libparse.a: $(addsuffix .o,$(addprefix $(o)/gather/,$(LIBPARSE_MODS)))
$(o)/gather/format/libparse.so: $(addsuffix .oo,$(addprefix $(o)/gather/,$(LIBPARSE_MODS)))
$(o)/gather/format/libparse.pc: $(LIBPARSE_DEPS)

API_LIBS+=libparse
API_INCLUDES+=$(o)/gather/format/.include-stamp
$(o)/gather/format/.include-stamp: $(addprefix $(s)/gather/format/,$(LIBPARSE_INCLUDES))
$(o)/gather/format/.include-stamp: IDST=gather/format
run/lib/pkgconfig/libparse.pc: $(o)/gather/format/libparse.pc

$(o)/gather/format/fb-textpacker-t: $(LIBSH) $(LIBCHARSET)
$(o)/gather/format/parser-test: $(LIBPARSE) $(LIBGATH) $(LIBSH)
