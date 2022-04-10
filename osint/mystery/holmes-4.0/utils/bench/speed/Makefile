# Speed benchmark for Sherlock

DIRS+=utils/bench/speed
PROGS+=$(addprefix $(o)/utils/bench/speed/, bench genbench)

$(o)/utils/bench/speed/bench: $(o)/utils/bench/speed/bench.o $(LIBSH)
$(o)/utils/bench/speed/bench: LIBS+=-lm
