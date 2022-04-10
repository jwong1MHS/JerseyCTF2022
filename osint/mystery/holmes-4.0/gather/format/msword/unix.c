/*
 * unix.c
 * Copyright (C) 1998-2000 A.J. van Os; Released under GPL
 *
 * Description:
 * Unix approximations of RISC-OS functions
 */

#include "sherlock/sherlock.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include "antiword.h"

/*
 * werr - write an error message and exit if needed
 */
void
werr(int iFatal, const char *szFormat, ...)
{
  va_list tArg;
  int n;
  byte buf[2048];

  va_start(tArg, szFormat);
  n = vsnprintf(buf, sizeof(buf), szFormat, tArg);
  if (n >= (int) sizeof(buf) || n < 0)
    log(L_WARN, "next message too long");

	switch (iFatal) {
	case 0:		/* The message is just a warning, so no exit */
	  log(L_WARN_R, "MSWord: %s", buf);
          return;
	case 1:		/* Fatal error with a standard exit */
	  gerror(2200, "MSWord: %s", buf);
	default:	/* Fatal error with a non-standard exit */
	  gerror(2203, "MSWord: %s", buf);
	}
	va_end(tArg);
} /* end of werr */

void
visdelay_begin(void)
{
} /* end of visdelay_begin */

void
visdelay_end(void)
{
} /* end of visdelay_end */
