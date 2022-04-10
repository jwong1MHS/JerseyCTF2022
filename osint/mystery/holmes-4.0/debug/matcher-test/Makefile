# Makefile for custom modules in the matcher-test customization of Sherlock

include $(s)/customs/bare/Makefile
include $(s)/debug/matcher-test/lib/Makefile

run/cf/%: $(s)/customs/bare/cf/% $(o)/config.mk $(BUILDSYS)/genconf
	$(M)CF $<
	$(Q)$(BUILDSYS)/genconf $< $@ $(o)/config.mk
