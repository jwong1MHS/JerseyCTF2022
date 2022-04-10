# Makefile for the Sherlock MS-Word (.doc) Parser 
# (c) 2004 Tomas Holusa <tomas.holusa@netcentrum.cz>

DIRS+=gather/format/msword

ANTIWORD_MODS=blocklist chartrans datalist depot finddata findtext \
	fontlist fonts fonts_u listlist main_u misc notes options out2window \
	output pictlist prop0 prop2 prop6 prop8 properties propmod rowlist \
	sectlist stylelist stylesheet summary tabstop atext unix utf8 \
	word2text worddos wordlib wordmac wordole wordwin

LIBPARSE_MODS+=format/msword/msword

$(o)/gather/format/msword/msword.$(OS): $(addprefix $(o)/gather/format/msword/,$(addsuffix .$(OS),$(ANTIWORD_MODS)))
	$(M)LD -r $@
	$(Q)$(LD) -r -o $@ $^
