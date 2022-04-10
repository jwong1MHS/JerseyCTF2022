/*
 *	Sherlock Shepherd -- Manual Control -- Sorted Sources for Mixed Selectors
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/man.h"

void
sel_src_close(struct sel_src *src)
{
  if (src->close)
    src->close(src);
}

void
sel_src_rewind(struct sel_src *src)
{
  src->find_first(src);
}
