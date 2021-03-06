/*
 *	Sherlock Library -- Stub for including math.h, avoiding name collisions
 *
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#undef log
#define log libm_log
#define exception math_exception
#include <math.h>
#undef log
#define log msg
#undef exception

#ifdef CONFIG_LINUX
float logf(float);
#endif
