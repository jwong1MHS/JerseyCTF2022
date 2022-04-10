# Makefile for the Sherlock Shepherd Daemon (c) 2003--2005 Martin Mares <mj@ucw.cz>

DIRS+=gather/shepherd

ifdef CONFIG_SHEPHERD
PROGS+=$(addprefix $(o)/gather/shepherd/,shep-init shep-dump shep-plan shep-reap shep-cork shep-merge shep-select shep-record \
	shep-test shep-export shep-equiv shep-feedback shep-upgrade shep-cleanup shep shep-clone shep-recover shepherd \
	shep-ctrltest shep-mail shep-mirror shep-phase shep-urls shep-sort)
EXTRA_RUNDIRS+=db/state
CONFIGS+=shepherd
TESTS+=$(addprefix $(o)/gather/shepherd/,url-db.test man-binary.test man-text.test)
endif

LIBSHEP_MODS=config footprint state-dir state-file state-log state-params site-hash site-filter protocol protocol-fb \
	url-db man-main man-resolve man-sel man-cmd man-src man-binary man-text reap-contrib

ifdef CONFIG_AREAS
LIBSHEP_MODS+=areas
endif

REAP_MODULES=
REAP_LIBS=
ifdef CONFIG_SHEPHERD_CHILDREN
REAP_MODULES=reap-children.o
REAP_LIBS+=$(LIBPARSE) $(LIBPROTO)
endif

$(o)/gather/shepherd/shep-init: $(o)/gather/shepherd/shep-init.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-dump: $(o)/gather/shepherd/shep-dump.o $(LIBSHEP) $(LIBGATH) $(LIBSH)
$(o)/gather/shepherd/shep-plan: $(o)/gather/shepherd/shep-plan.o $(LIBSHEP) $(LIBGATH) $(LIBSH)
$(o)/gather/shepherd/shep-reap: $(addprefix $(o)/gather/shepherd/,shep-reap.o reap-queue.o reap-jobs.o reap-robots.o $(REAP_MODULES)) $(LIBSHEP) $(REAP_LIBS) $(LIBGATH) $(LIBSH)
$(o)/gather/shepherd/shep-cork: $(o)/gather/shepherd/shep-cork.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-merge: $(o)/gather/shepherd/shep-merge.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-select: $(o)/gather/shepherd/shep-select.o $(LIBSHEP) $(LIBGATH) $(LIBSH)
$(o)/gather/shepherd/shep-record: $(o)/gather/shepherd/shep-record.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-export: $(o)/gather/shepherd/shep-export.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-equiv: $(o)/gather/shepherd/shep-equiv.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-feedback: $(o)/gather/shepherd/shep-feedback.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-upgrade: $(o)/gather/shepherd/shep-upgrade.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-cleanup: $(o)/gather/shepherd/shep-cleanup.o $(LIBSHEP) $(LIBGATH) $(LIBSH)
$(o)/gather/shepherd/shep-recover: $(o)/gather/shepherd/shep-recover.o $(LIBSHEP) $(LIBGATH) $(LIBSH)
$(o)/gather/shepherd/shep-ctrltest: $(o)/gather/shepherd/shep-ctrltest.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-mirror: $(o)/gather/shepherd/shep-mirror.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep: $(o)/gather/shepherd/shep.o $(LIBSHEP) $(LIBGATH) $(LIBSH)
$(o)/gather/shepherd/shepherd: $(addprefix $(o)/gather/shepherd/,shepherd.o master-ctrl.o master-states.o) $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-urls: $(o)/gather/shepherd/shep-urls.o $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/shep-sort: $(o)/gather/shepherd/shep-sort.o $(LIBSHEP) $(LIBSH)

$(o)/gather/shepherd/libshepherd.a: $(addsuffix .o,$(addprefix $(o)/gather/shepherd/,$(LIBSHEP_MODS)))
$(o)/gather/shepherd/libshepherd.so: $(addsuffix .oo,$(addprefix $(o)/gather/shepherd/,$(LIBSHEP_MODS)))
$(o)/gather/shepherd/libshepherd.pc: $(LIBSH) $(LIBGATH) $(REAP_LIBS)

$(o)/gather/shepherd/url-db.test: $(o)/gather/shepherd/url-db-t
$(o)/gather/shepherd/url-db-t: $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/man-binary.test: $(o)/gather/shepherd/man-binary-t
$(o)/gather/shepherd/man-binary-t: $(LIBSHEP) $(LIBSH)
$(o)/gather/shepherd/man-text.test: $(o)/gather/shepherd/man-text-t
$(o)/gather/shepherd/man-text-t: $(LIBSHEP) $(LIBSH)
