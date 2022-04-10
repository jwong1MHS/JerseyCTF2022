/*
 *	Sherlock Filter Engine --- tree data structure
 *
 *	(c) 2004--2005, Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/fastbuf.h"
#include "filter/filter.h"
#include "filter/parse.tab.h"

#include <string.h>

static int
can_be_in_tree(struct filter_case *cs, uns type, uns icase_unsign, union filter_raw_value2 *String)
{
	struct filter_cond *cond = cs->cond;
	ASSERT(cond->cat == F_CC_EXPR);
	if (cs->type == type
	&& (
		cond->o.expr.op == EIN
		&& !cond->o.expr.neg
		&& cond->o.expr.icase == icase_unsign
		&& cond->o.expr.r->cat == F_EC_BINOP
		&& cond->o.expr.r->o.bin.op == INTERVAL
		&& cond->o.expr.r->o.bin.l->cat == F_EC_CONST
		&& cond->o.expr.r->o.bin.r->cat == F_EC_CONST
	||
		(cond->o.expr.op == LT || cond->o.expr.op == GT)
		&& cond->o.expr.neg
		&& cond->o.expr.icase == icase_unsign
		&& cond->o.expr.r->cat == F_EC_CONST
	||
		cond->o.expr.op == EQ
		&& !cond->o.expr.neg
		&& cond->o.expr.icase == icase_unsign
		&& cond->o.expr.r->cat == F_EC_CONST
	))
	{
		ASSERT(cond->o.expr.r->type == type);
		ASSERT(!cs->undef);
		ASSERT(!cond->o.expr.r->undef);
		if (String)
		{
			if (cond->o.expr.op == EIN)
			{
				String[0].s = cond->o.expr.r->o.bin.l->o.s;
				String[1].s = cond->o.expr.r->o.bin.r->o.s;
			}
			else if (cond->o.expr.op == EQ)
			{
				String[0].s = String[1].s = cond->o.expr.r->o.s;
			}
			else if (cond->o.expr.op == LT)
				String[0].s = cond->o.expr.r->o.s;
			else
			{
				ASSERT(cond->o.expr.op == GT);
				String[1].s = cond->o.expr.r->o.s;
			}
		}
		return 1;
	}
	return 0;
}

#define	NAME		filter_s
#define	NAME_(x)	filter_s_##x
#define	STRUCT_DECL	byte s[1]
#define	PTR_DECL	byte *
#define	STRUCT_ATTR	s
#define	FORMAT		"s"
#define	FULL_INTERVAL	NULL, NULL
#define	TREE_KEY_ENDSTRING	s
#define	FILTER_TYPE	F_ET_STRING
#define	RAW_ATTR	s
#define	ICASE_UNSIGN_FLAG	0
#define	DUMP_TITLE	"case-sensitive strings"
#include "filter/trees.h"

#define	NAME		filter_is
#define	NAME_(x)	filter_is_##x
#define	STRUCT_DECL	byte is[1]
#define	PTR_DECL	byte *
#define	STRUCT_ATTR	is
#define	FORMAT		"s"
#define	FULL_INTERVAL	NULL, NULL
#define	TREE_KEY_ENDSTRING	is
#define	TREE_NOCASE
#define	FILTER_TYPE	F_ET_STRING
#define	RAW_ATTR	s
#define	ICASE_UNSIGN_FLAG	1
#define	DUMP_TITLE	"case-insensitive strings"
#include "filter/trees.h"

#define	NAME		filter_d
#define	NAME_(x)	filter_d_##x
#define	STRUCT_DECL	int d
#define	PTR_DECL	int
#define	STRUCT_ATTR	d
#define	FORMAT		"d"
#define	FULL_INTERVAL	0x80000000, 0x7fffffff
#define	TREE_KEY_ATOMIC	d
#define	TREE_ATOMIC_TYPE	int
#define	FILTER_TYPE	F_ET_INT
#define	RAW_ATTR	i
#define	ICASE_UNSIGN_FLAG	0
#define	DUMP_TITLE	"signed integers"
#include "filter/trees.h"

#define	NAME		filter_ud
#define	NAME_(x)	filter_ud_##x
#define	STRUCT_DECL	uns ud
#define	PTR_DECL	uns
#define	STRUCT_ATTR	ud
#define	FORMAT		"u"
#define	FULL_INTERVAL	0, 0xffffffff
#define	TREE_KEY_ATOMIC	ud
#define	TREE_ATOMIC_TYPE	uns
#define	FILTER_TYPE	F_ET_INT
#define	RAW_ATTR	u
#define	ICASE_UNSIGN_FLAG	1
#define	DUMP_TITLE	"unsigned integers"
#include "filter/trees.h"
