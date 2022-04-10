/*
 *	Sherlock Filter Engine -- Builtin Functions for the free version
 *
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

void fv_example_function_1(struct filter_args *args, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS]);

#define CUSTOM_FILTER_BUILTINS \
	{ "example_function_1",	1, { F_ET_STRING },			F_ET_STRING,	fv_example_function_1 },

/*
 * You can also define custom filter variables by the CUSTOM_FILTER_VARS macro,
 * but since Sherlock won't bind anything to them, they are only useful if you
 * write your own programs which link with libfilter.
 */
