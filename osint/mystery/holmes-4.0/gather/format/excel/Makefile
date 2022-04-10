# Makefile for the Sherlock MS-Excel (.xls) Parser
# (c) 2004 Tomas Holusa <tomas.holusa@netcentrum.cz>

DIRS+=gather/format/excel

XLHTML_MODS=ascii cole cp1252 internal oledecod support supportxl xlhtml

LIBPARSE_MODS+=format/excel/excel

$(o)/gather/format/excel/excel.$(OS): $(addprefix $(o)/gather/format/excel/,$(addsuffix .$(OS),$(XLHTML_MODS)))
	$(M)LD -r $@
	$(Q)$(LD) -r -o $@ $^
