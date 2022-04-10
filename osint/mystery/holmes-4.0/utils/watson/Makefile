# Watson: Monitoring system for Sherlock

DIRS+=utils/watson
PROGS+=$(addprefix $(o)/utils/watson/,watson watson-install Watsonlib.pm analyze-gatherd analyze-indexer analyze-search \
	analyze-gatherd analyze-indexer-status drwatson.cgi graph.cgi watson-err log-gatherer-bugs)
ifdef CONFIG_SHEPHERD_PROTOCOL
PROGS+=$(o)/utils/watson/analyze-shepherd
PROGS+=$(o)/utils/watson/analyze-shepherd-status
endif
EXTRA_RUNDIRS+=log/inter cache/graph lib/cgi-bin
DATAFILES+=$(o)/utils/watson/error.png
CONFIGS+=watson

$(o)/utils/watson/drwatson.cgi: BINDIR=lib/cgi-bin
$(o)/utils/watson/graph.cgi: BINDIR=lib/cgi-bin
$(o)/utils/watson/error.png: DATADIR=lib/cgi-bin
$(o)/utils/watson/Watsonlib.pm: PERL_MODULE_DIR=Sherlock

include $(s)/utils/watson/graph/Makefile
