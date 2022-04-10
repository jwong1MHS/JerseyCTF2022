/*
 *	Sherlock Filter Engine -- Example filter functions
 *
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "filter/filter.h"
#include "free/filter/builtin.h"

#include <string.h>

void
fv_example_function_1(struct filter_args *args UNUSED, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS])
{
  *ret = arg[0];
}
