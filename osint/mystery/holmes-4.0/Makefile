# Makefile for the Sherlock Search System
# (c) 1997--2007 Martin Mares <mj@ucw.cz>

# The default target
all: runtree programs api datafiles extras configs

# Include configuration
s=.
-include obj/config.mk
obj/config.mk:
	@echo 'You have to run configure first.'
	@exit 1

BUILDSYS=$(s)/build

# We will use the libucw makefile system
include $(BUILDSYS)/Maketop

# Custom clean rules
distclean::
	rm -f custom

# Configuration files
ifdef CONFIG_SHERLOCK
CONFIGS+=accept filter sherlock local url-equiv trace
endif

# Extra directories to be created in the run/* hierarchy
EXTRA_RUNDIRS+=lock lib/perl5/Sherlock
ifdef CONFIG_SHERLOCK
EXTRA_RUNDIRS+=db cache index
endif

# We need to define names of libraries here, because even though variable references
# in bodies of rules can be forward, those in dependencies cannot. Also the order of
# includes is somewhat fragile because of that.
LIBSH=$(o)/sherlock/libsh.pc
LIBSHXML=$(o)/sherlock/xml/libshxml.pc
LIBCUSTOM=$(o)/sherlock/libcustom.pc
LIBCHARSET=$(o)/charset/libcharset.pc
LIBFILTER=$(o)/filter/libfilter.pc
LIBINDEXER=$(o)/indexer/libindexer.pc
LIBLANG=$(o)/lang/liblang.pc
LIBIMAGES=$(o)/images/libimages.pc
LIBANAL=$(o)/analyser/libanalyser.pc
LIBGATH=$(o)/gather/libgather.pc
LIBPROTO=$(o)/gather/proto/libproto.pc
LIBPARSE=$(o)/gather/format/libparse.pc
LIBSHEP=$(o)/gather/shepherd/libshepherd.pc

# All tests (%-t) get automatically linked with libsh or libucw
ifdef CONFIG_SHERLOCK_LIB
TESTING_DEPS=$(LIBSH)
else
TESTING_DEPS=$(LIBUCW)
endif

# Include makefiles of various subsystems
-include custom/Makefile
include $(BUILDSYS)/Makefile
include $(s)/ucw/Makefile
ifdef CONFIG_SHERLOCK_LIB
include $(s)/sherlock/Makefile
endif
ifdef CONFIG_CHARSET
include $(s)/charset/Makefile
endif
ifdef CONFIG_LIBLANG
include $(s)/lang/Makefile
endif
ifdef CONFIG_IMAGES
include $(s)/images/Makefile
endif

ifdef CONFIG_SHERLOCK
include $(s)/filter/Makefile
include $(s)/utils/Makefile
include $(s)/analyser/Makefile
include $(s)/gather/Makefile
ifdef CONFIG_ADMIN
include $(s)/centrum/admin/Makefile
endif
include $(s)/indexer/Makefile
ifdef CONFIG_SEARCH
include $(s)/search/Makefile
endif
ifdef CONFIG_DEBUG_TOOLS
include $(s)/debug/Makefile
endif
endif

# Build documentation by default?
ifdef CONFIG_DOC
all: docs
endif

# Installation

ifdef CONFIG_SHERLOCK
# If we build full Sherlock, we use our own install script which just copies
# selected parts of the runtree hierarchy.
install: all
	SH_EXTRA_RUNDIRS="$(sort $(EXTRA_RUNDIRS))" SH_INSTALL_RUNDIRS="$(sort $(INSTALL_RUNDIRS))" SH_CONFIGS="$(sort $(CONFIGS))" SH_AUTO_CONFIRM="$(CONFIRM)" $(BUILDSYS)/installer $(INSTALL_DIR)

else
ifndef CONFIG_LOCAL
# If we build only the libraries, we hand the work over to their local installation
# targets, which work in the traditional UNIXy way.
install: all $(INSTALL_TARGETS)
else
install:
	@echo "Nothing to install, this is a local build." && false
endif
endif

# We prefer configuration files defined by the customizaton over the generic versions
run/$(CONFIG_DIR)/%: custom/cf/% $(o)/config.mk $(BUILDSYS)/genconf
	$(M)CF $<
	$(Q)$(BUILDSYS)/genconf $< $@ $(o)/config.mk

include $(BUILDSYS)/Makebottom
