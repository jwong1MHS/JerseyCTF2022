# Miscellaneous utilities

DIRS+=utils
UTILS=cs2cs objdump idxdump scheduler sizer query shcp \
	dump-card index-version \
	configtest bgrep sched-control send-indices black-control send-mail locker check-indexer-input

ifdef CONFIG_SEARCH
UTILS+=skeeper scontrol check-sherlockd
endif

ifdef CONFIG_GATHERER
UTILS+=checker gcontrol
endif

ifdef CONFIG_SHEPHERD
UTILS+=check-shepherd check-limit
$(o)/utils/check-shepherd: $(o)/utils/check-shepherd.o $(LIBSH)
endif

PROGS+=$(addprefix $(o)/utils/,$(UTILS))

$(o)/utils/cs2cs: $(o)/utils/cs2cs.o $(LIBCHARSET) $(LIBSH)
$(o)/utils/objdump: $(o)/utils/objdump.o $(o)/utils/dumpconfig.o $(LIBLANG) $(LIBCHARSET) $(LIBSH)
$(o)/utils/idxdump: $(o)/utils/idxdump.o $(o)/utils/dumpconfig.o $(o)/indexer/iconfig.o $(LIBLANG) $(LIBCUSTOM) $(LIBCHARSET) $(LIBSH)
$(o)/utils/check-sherlockd: $(o)/utils/check-sherlockd.o $(LIBSH)
$(o)/utils/sizer: $(o)/utils/sizer.o $(LIBSH)
$(o)/utils/shcp: $(o)/utils/shcp.o $(LIBSH)
$(o)/utils/index-version: $(o)/utils/index-version.o $(LIBSH)
$(o)/utils/bgrep: $(o)/utils/bgrep.o $(LIBSH)
$(o)/utils/check-indexer-input: $(o)/utils/check-indexer-input.o $(LIBSH)

ifdef CONFIG_WATSON
include $(s)/utils/watson/Makefile
endif

include $(s)/utils/bench/relevance/Makefile
include $(s)/utils/bench/speed/Makefile
