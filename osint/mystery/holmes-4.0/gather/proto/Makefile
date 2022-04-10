# Makefile for the Sherlock Downloaders (c) 2001 Martin Mares <mj@ucw.cz>

DIRS+=gather/proto

LIBPROTO_MODS=proto skey http file

$(o)/gather/proto/libproto.a: $(addsuffix .o,$(addprefix $(o)/gather/proto/,$(LIBPROTO_MODS)))
$(o)/gather/proto/libproto.so: $(addsuffix .oo,$(addprefix $(o)/gather/proto/,$(LIBPROTO_MODS)))
$(o)/gather/proto/libproto.pc: $(LIBGATH)

API_LIBS+=libproto
run/lib/pkgconfig/libproto.pc: $(o)/gather/proto/libproto.pc
