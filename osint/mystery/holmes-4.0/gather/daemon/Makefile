# Makefile for the Sherlock Gatherer Daemon (c) 2001 Martin Mares <mj@ucw.cz>

DIRS+=gather/daemon
PROGS+=$(addprefix $(o)/gather/daemon/,gatherd gc expire grecover)
CONFIGS+=gatherd

GATHERD_OBJS=gatherd.o config.o db.o queue.o refs.o
GC_OBJS=gc.o config.o db.o queue.o refs.o
EXPIRE_OBJS=expire.o config.o db.o queue.o refs.o

$(o)/gather/daemon/gatherd: $(addprefix $(o)/gather/daemon/,$(GATHERD_OBJS)) \
	$(LIBPARSE) $(LIBPROTO) $(LIBGATH) $(LIBSH)

$(o)/gather/daemon/gc: $(addprefix $(o)/gather/daemon/,$(GC_OBJS)) \
	$(LIBPARSE) $(LIBPROTO) $(LIBGATH) $(LIBSH)

$(o)/gather/daemon/expire: $(addprefix $(o)/gather/daemon/,$(EXPIRE_OBJS)) \
	$(LIBPARSE) $(LIBPROTO) $(LIBGATH) $(LIBSH)
