# Makefile for the Sherlock Gatherer (c) 2001--2006 Martin Mares <mj@ucw.cz>

DIRS+=gather

ifdef CONFIG_GATHERER

PROGS+=$(o)/gather/gtest $(o)/gather/gbatch
CONFIGS+=gatherer checker checker-filter

LIBGATH_MODS=ginit gobject gconfig charset content gfilter bucket diff ganalyse \
        ctmatch
LIBGATH_INCLUDES=gather.h

include $(s)/gather/proto/Makefile
include $(s)/gather/format/Makefile

$(o)/gather/libgather.a: $(addsuffix .o,$(addprefix $(o)/gather/,$(LIBGATH_MODS)))
$(o)/gather/libgather.so: $(addsuffix .oo,$(addprefix $(o)/gather/,$(LIBGATH_MODS)))
$(o)/gather/libgather.pc: $(LIBSH) $(LIBLANG) $(LIBANAL) $(LIBCHARSET) $(LIBFILTER)

$(o)/gather/gtest: $(o)/gather/gtest.o $(LIBPARSE) $(LIBPROTO) $(LIBGATH) $(LIBSH)
$(o)/gather/gbatch: $(o)/gather/gbatch.o $(LIBPARSE) $(LIBPROTO) $(LIBGATH) $(LIBSH)

API_LIBS+=libgather
API_INCLUDES+=$(o)/gather/.include-stamp
$(o)/gather/.include-stamp: $(addprefix $(s)/gather/,$(LIBGATH_INCLUDES))
$(o)/gather/.include-stamp: IDST=gather
run/lib/pkgconfig/libgather.pc: $(o)/gather/libgather.pc

ifdef CONFIG_GATHERD
include $(s)/gather/daemon/Makefile
endif

endif

ifdef CONFIG_SHEPHERD_PROTOCOL
include $(s)/gather/shepherd/Makefile
endif
