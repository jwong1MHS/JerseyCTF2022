# Makefile for the Sherlock MP3 Parser (c) 2007 Martin Mares <mj@ucw.cz>

DIRS+=gather/format/mp3
MP3_MODS=bitstream id3-common id3v1 id3v2 mp3
MP3_OBJS=$(addprefix $(o)/gather/format/mp3/,$(addsuffix .o,$(MP3_MODS)))
LIBPARSE_MODS+=$(addprefix format/mp3/,$(MP3_MODS) parse)

ifdef CONFIG_DEBUG_TOOLS
PROGS+=$(o)/gather/format/mp3/mp3scan $(o)/gather/format/mp3/mp3info
endif

$(o)/gather/format/mp3/mp3scan: $(o)/gather/format/mp3/mp3scan.o $(MP3_OBJS) $(LIBCHARSET) $(LIBSH)
$(o)/gather/format/mp3/mp3info: $(o)/gather/format/mp3/mp3info.o $(MP3_OBJS) $(LIBCHARSET) $(LIBSH)
