# Makefile for the Sherlock Parsers (c) 2007 Pavel Charvat <pchar@ucw.cz>

DIRS+=gather/format/ogg
OGG_MODS=ogg ogg-vorbis ogg-codecs
LIBPARSE_MODS+=$(addprefix format/ogg/,$(OGG_MODS) parse)

ifdef CONFIG_DEBUG_TOOLS
PROGS+=$(o)/gather/format/ogg/ogg-scan
endif

$(o)/gather/format/ogg/ogg-scan: $(addprefix $(o)/gather/format/ogg/,$(addsuffix .o,$(OGG_MODS) ogg-scan)) $(LIBUCW)
