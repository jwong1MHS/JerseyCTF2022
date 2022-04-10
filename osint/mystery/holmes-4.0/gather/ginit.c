/*
 *	Sherlock Gatherer -- Initialization
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 *	(c) 2004--2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "analyser/analyser.h"
#include "gather/gather.h"

/*
 *  Before you call any libgather functions related to downloading, filtering,
 *  parsing or analyzing, you have to initialize the library. There are several
 *  modules which don't need explicit initialization, but don't rely on it
 *  unless it's seriously needed for performance or to avoid lots of linking
 *  dependencies.
 */

void
gatherer_init(void)
{
  gather_init_filter();
  gather_init_analyser();
}
