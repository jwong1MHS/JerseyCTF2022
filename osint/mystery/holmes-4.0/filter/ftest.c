#include <stdio.h>
#include <string.h>

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/mempool.h"
#include "sherlock/object.h"
#include "filter/filter.h"

static uns num = 13;
static byte *str = "initial value";

static struct cf_section ftest_config = {
	CF_ITEMS {
		CF_UNS("num", &num),
		CF_STRING("str", &str),
		CF_END
	}
};

struct my_variables {
	int x, y;
	char *s1, *s2;
};

static void CONSTRUCTOR ftest_init_config(void)
{
	cf_declare_section("ftest", &ftest_config, 0);
}

static struct filter_variable my_vars[] = {
	{ "x",	F_LVC_RAW,	F_ET_INT,	0 },
	{ "y",	F_LVC_RAW,	F_ET_INT,	1 },
	{ "s1",	F_LVC_RAW,	F_ET_STRING,	0 },
	{ "s2",	F_LVC_RAW,	F_ET_STRING,	0 },
	{ "z",	F_LVC_RAW,	F_ET_INT,	1 },
	{ "z2",	F_LVC_RAW,	F_ET_INT,	0 },
	{ "x",	F_LVC_ATTR,	F_ET_STRING,	0 },
	{ ".",	F_LVC_ATTR,	F_ET_INT,	0 },
	{ NULL,	0,		0,		0 }
};

static struct filter_binding my_bind[] = {
	{ "x",	OFFSETOF(struct my_variables, x) },
	{ "y",	OFFSETOF(struct my_variables, y) },
	{ "s1",	OFFSETOF(struct my_variables, s1) },
	{ "s2",	OFFSETOF(struct my_variables, s2) },
	{ NULL,	0 }
};

static int
dump_internal2(struct filter_declaration *decl, union filter_raw_value *user_var)
{
	int count = 0;
	for (; decl; count++, decl=decl->next)
		switch (decl->var.type)
		{
			case F_ET_INT:
				printf("%d. %s=", decl->nr, decl->var.name);
				if (user_var[decl->nr].i != F_UNDEF_INT)
					printf("%d\n", user_var[decl->nr].i);
				else
					printf("<undefined int>\n");
				break;
			case F_ET_STRING:
				printf("%d. %s=", decl->nr, decl->var.name);
				if (user_var[decl->nr].s != F_UNDEF_STRING)
					printf("%s\n", user_var[decl->nr].s);
				else
					printf("<undefined string>\n");
				break;
			case F_ET_REGEXP:
				printf("%d. %s=<regexp>\n", decl->nr, decl->var.name);
				break;
			default:
				ASSERT(0);
		}
	return count;
}

static void
dump_internal(struct filter_args *a)
{
	int count = 0;
	printf("Internal variables:\n");
	count += dump_internal2(a->filter->decl, a->user_var);
	printf("Deleted internal variables:\n");
	count += dump_internal2(a->filter->deleted_decl, a->user_var);
	ASSERT(count == a->filter->user_vars);
}

int
main(void)
{
	struct filter *f;
	struct filter_args *a;
	struct mempool *mp_oa;
	struct my_variables var = { 1, 2, "hello", "good day!" };
	int res;

	log_init("test");
	cf_load(cf_def_file);

	f = filter_load("filter/test-filter", my_vars, my_bind, NULL);
	mp_oa = mp_new(4096);

	a = filter_intr_new(f);
	a->pool = mp_oa;
	a->raw = &var;
	a->attr = obj_new(mp_oa);
	a->want_config_changes = 1;

	filter_intr_undo_init(a);
	res = filter_intr_run(a);
	printf("Result: %d, %s\n", res, (char*)a->msg ? : "?");
	printf("Global variables: %d, %d, %s, %s\n", var.x, var.y, var.s1, var.s2);
	printf("Config: %d, %s\n", num, str);
	printf("Attributes:\n");
	obj_dump(a->attr);
	dump_internal(a);
	filter_intr_undo(a);
	printf("Undone config: %d, %s\n", num, str);

	filter_intr_delete(a);

	mp_delete(mp_oa);
	filter_delete(f);

	return 0;
}
