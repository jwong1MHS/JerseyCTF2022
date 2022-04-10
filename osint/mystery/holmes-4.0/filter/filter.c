/*
 *	Sherlock Filter Engine
 *
 *	(c) 1999--2000 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2007 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/url.h"
#include "ucw/fastbuf.h"
#include "ucw/string.h"
#include "sherlock/object.h"
#include "filter/filter.h"
#include "filter/parse.tab.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

struct filter *filter_current;
jmp_buf filter_jmp_buf;
void (*filter_err_hook)(char *err);

static int
filter_do_parse(struct filter *f)
{
	if (setjmp(filter_jmp_buf))
		return 1;
	return yyparse(f);
}

static struct filter *
filter_do_load(struct filter_variable *var, struct filter_binding *bind, struct filter_function *func)
{
	struct filter *f;

	f = xmalloc(sizeof(struct filter));
	f->pool = mp_new(4096);
	f->name_head = mp_alloc(f->pool, sizeof(struct filter_lex_name) + 1);
	f->name_head->next = NULL;
	f->name_head->name[0] = 0;
	f->var = var;
	f->bind = bind;
	f->func = func;
	f->user_vars = 0;
	f->decl = f->deleted_decl = NULL;
	f->body = NULL;
	f->lookup_count = 0;
	f->lookup_limit = 32;
	f->lookup = xmalloc(f->lookup_limit * sizeof(*f->lookup));
	bzero(f->lookup, sizeof(*f->lookup));

	filter_current = f;
	int err = filter_do_parse(f);
	filter_lex_cleanup();
	filter_current = NULL;
	if (err)
	{
		filter_delete(f);
		return NULL;
	}

	filter_prune(f);
	if (filter_dump_to)
	{
		struct fastbuf *b = bopen(filter_dump_to, O_CREAT | O_TRUNC | O_WRONLY, 1024);
		filter_dump(b, f);
		bclose(b);
	}
	return f;
}

struct filter *
filter_load(char *name, struct filter_variable *var, struct filter_binding *bind, struct filter_function *func)
{
	filter_lex_init(name);
	return filter_do_load(var, bind, func);
}

struct filter *
filter_load_fb(struct fastbuf *fb, struct filter_variable *var, struct filter_binding *bind, struct filter_function *func)
{
	filter_lex_init_fb(fb);
	return filter_do_load(var, bind, func);
}

struct filter *
filter_clone(struct filter *filter)
{
	struct filter *f;

	ASSERT(filter);
	f = xmalloc(sizeof(*f));
	memcpy(f, filter, sizeof(*f));
	f->pool = mp_new(4096);
	f->lookup = xmalloc((f->lookup_count + 1) * sizeof(*f->lookup));
	memcpy(f->lookup, filter->lookup, (f->lookup_count + 1) * sizeof(*f->lookup));
	for (uns i = 1; i <= f->lookup_count; i++)
	{
		struct filter_lookup *l = f->lookup + i;
		switch (l->type)
		{
			case F_LT_REGEX:
			{
				l->regex = mp_memdup(f->pool, l->regex, sizeof(*l->regex));
				if (!(l->regex->regex = rx_compile(l->regex->source, l->regex->icase)))
					die("Cannot clone regular expression");
				break;
			}
			case F_LT_TRIE:
			{
				l->trie = mp_memdup(f->pool, l->trie, sizeof(*l->trie));
				l->trie->results = mp_memdup(f->pool, l->trie->results, l->trie->cmds * sizeof(struct filter_trie_result));
				clist_init(&l->trie->tests);
				clist_init(&l->trie->incremented_tests);
				clist_init(&l->trie->passed_tests);
				for (uns i = 0; i < l->trie->cmds; i++)
				{
					if (l->trie->results[i].init_passed == 2)
						clist_add_tail(&l->trie->passed_tests, &l->trie->results[i].n);
					else
						clist_add_tail(&l->trie->tests, &l->trie->results[i].n);
				}
				break;
			}
			default:
				ASSERT(0);
		}
	}
	return f;
}

int
filter_lookup_new(struct filter *filter, enum filter_lookup_type type, void *value)
{
	if (filter->lookup_count + 1 == filter->lookup_limit)
	{
		filter->lookup_limit *= 2;
		filter->lookup = xrealloc(filter->lookup, filter->lookup_limit * sizeof(*filter->lookup));
	}
	struct filter_lookup *l = filter->lookup + ++filter->lookup_count;
	l->type = type;
	l->value = value;
	return filter->lookup_count;
}

int
filter_lookup_new_no_null(struct filter *filter, enum filter_lookup_type type, void *value)
{
	return value ? filter_lookup_new(filter, type, value) : 0;
}

void
filter_delete(struct filter *f)
{
	// memory leak (regex)
	if (!f)
		return;
	mp_delete(f->pool);
	xfree(f->lookup);
	xfree(f);
}

struct filter_args *
filter_intr_new(struct filter *f)
{
	struct filter_args *a = xmalloc_zero(sizeof(struct filter_args));
	a->filter = f;
	if (a->filter->user_vars)
		a->user_var = xmalloc_zero(a->filter->user_vars*sizeof(union filter_raw_value));
	return a;
}

void
filter_intr_delete(struct filter_args *a)
{
	xfree(a->user_var);
	a->user_var = NULL;
	xfree(a);
}

static void
filter_get_lvalue(struct filter_value *dest, struct filter_args *args, struct filter_lvalue *lv)
{
	struct oattr *o;

	dest->type = lv->type;
	dest->undef = lv->undef;
	if (dest->undef)
		return;
	switch (lv->cat)
	{
		case F_LVC_RAW:
			if (lv->type == F_ET_INT)
				dest->v.i = * (int*) (args->raw + lv->v.bind->offset);
			else if (lv->type == F_ET_STRING)
				dest->v.s = * (byte**) (args->raw + lv->v.bind->offset);
			else if (lv->type == F_ET_REGEXP)
				dest->v.r = * (struct filter_regex_value**) (args->raw + lv->v.bind->offset);
			else
				ASSERT(0);
			break;
		case F_LVC_ATTR:
			if (args->attr)
				o = obj_find_attr(args->attr, lv->v.name);
			else
				o = NULL;
			if (!o)
				dest->undef = 1;
			else
			{
				char *c;
				if (lv->type == F_ET_REGEXP || lv->type == F_ET_UNKNOWN)
					ASSERT(0);
				if (lv->type == F_ET_INT)
				{
					dest->v.i = strtoul(o->val, &c, 0);
					if (c && *c || errno == ERANGE)
						dest->undef = 1;
				}
				else
					dest->v.s = o->val;
			}
			break;
		case F_LVC_CONF:
			if (lv->type == F_ET_INT)
				dest->v.i = * (int*) (lv->v.cfg->ptr);
			else if (lv->type == F_ET_STRING)
				dest->v.s = * (byte**) (lv->v.cfg->ptr);
			else
				ASSERT(0);
			break;
		case F_LVC_USER:
			dest->v = args->user_var[lv->v.decl->nr];
			break;
		default:
			ASSERT(0);
	}
	/* Recognise variables undefined not by the context, but by their real
	 * value.  */
	if (lv->type == F_ET_INT && dest->v.i == F_UNDEF_INT
	|| lv->type == F_ET_STRING && dest->v.s == F_UNDEF_STRING
	|| lv->type == F_ET_REGEXP && dest->v.s == F_UNDEF_REGEXP)
		dest->undef = 1;
}

static void
filter_set_lvalue(struct filter_lvalue *dest, struct filter_args *args, struct filter_value *v, int append)
{
	byte tmp[32], *tmp1;

	if (dest->undef)
		return;
	if (dest->cat == F_LVC_ATTR && (!v || v->undef))
	{
		if (args->attr)
		{
			struct oattr *o;
			o = obj_find_attr(args->attr, dest->v.name);
			if (o)
				obj_del_attr(args->attr, o);
		}
		return;
	}
	ASSERT(!TYPE_MISMATCH(dest->type, v->type));
	TYPE_COMBINE(v->type, dest->type);
	if (v->undef)
	{
		/* Store special undefined values as a flag to destination
		 * variables.  */
		if (v->type == F_ET_INT)
			v->v.i = F_UNDEF_INT;
		else if (v->type == F_ET_STRING)
			v->v.s = F_UNDEF_STRING;
		else if (v->type == F_ET_REGEXP)
			v->v.s = F_UNDEF_REGEXP;
		else
			ASSERT(0);
	}
	switch (dest->cat)
	{
		case F_LVC_RAW:
			if (v->type == F_ET_INT)
				* (int*) (args->raw + dest->v.bind->offset) = v->v.i;
			else if (v->type == F_ET_STRING)
				* (byte**) (args->raw + dest->v.bind->offset) = v->v.s;
			else if (v->type == F_ET_REGEXP)
				* (struct filter_regex_value**) (args->raw + dest->v.bind->offset) = v->v.r;
			else
				ASSERT(0);
			break;
		case F_LVC_ATTR:
			if (v->type == F_ET_REGEXP || v->type == F_ET_UNKNOWN)
				ASSERT(0);
			if (v->type == F_ET_INT)
			{
				sprintf(tmp, "%d", v->v.i);
				tmp1 = tmp;
			}
			else
				tmp1 = v->v.s;
			if (args->attr)
			{
				if (append)
					obj_add_attr(args->attr, dest->v.name, tmp1);
				else
					obj_set_attr(args->attr, dest->v.name, tmp1);
			}
			break;
		case F_LVC_CONF:
			if (args->config_changes_mode)
			{
				cf_journal_block(dest->v.cfg->ptr, sizeof(void*));
				if (v->type == F_ET_INT)
					* (int*) (dest->v.cfg->ptr) = v->v.i;
				else if (v->type == F_ET_STRING)
					* (byte**) (dest->v.cfg->ptr) = v->v.s;
				else
					ASSERT(0);
			}
			break;
		case F_LVC_USER:
			args->user_var[dest->v.decl->nr] = v->v;
			break;
		default:
			ASSERT(0);
	}
}

static inline int
printable_length(struct filter_value *v)
{
	if (v->undef)
		return 20;
	switch (v->type)
	{
		case F_ET_INT:
			return 16;
		case F_ET_STRING:
			return strlen(v->v.s);
		case F_ET_REGEXP:
			return 20;
		default:
			ASSERT(0);
	}
	return 0;
}

#define	PRINT_VALUE(prf, prpar) {\
	if (v->undef)\
	{\
		prf(prpar, "<undefined %s>", v->type == F_ET_INT ? "int" : v->type == F_ET_STRING ? "string" : v->type == F_ET_REGEXP ? "regexp" : "value");\
		return;\
	}\
	switch (v->type)\
	{\
		case F_ET_INT:\
			prf(prpar, "%d", v->v.i);\
			break;\
		case F_ET_STRING:\
			prf(prpar, "%s", v->v.s);\
			break;\
		case F_ET_REGEXP:\
			prf(prpar, "<regexp>");\
			break;\
		default:\
			ASSERT(0);\
	}\
}

static void
value_sprintf(byte *dest, struct filter_value *v)
{
	PRINT_VALUE(sprintf, dest);
}

static void
value_log(int level, struct filter_value *v)
{
	PRINT_VALUE(log, level);
}

void
filter_eval_expr(struct filter_value *dest, struct filter_args *args, struct filter_expr *expr)
{
	dest->type = expr->type;
	dest->undef = expr->undef;
	if (expr->undef)
		return;
	switch (expr->cat)
	{
		case F_EC_CONST:
			if (expr->type == F_ET_INT)
				dest->v.i = expr->o.i;
			else if (expr->type == F_ET_STRING)
				dest->v.s = expr->o.s;
			else if (expr->type == F_ET_REGEXP)
				dest->v.i = expr->o.i;
			else
				ASSERT(0);
			break;
		case F_EC_LVALUE:
			filter_get_lvalue(dest, args, expr->o.lv);
			break;
		case F_EC_UNOP:
			filter_eval_expr(dest, args, expr->o.un.r);
			if (dest->undef)
				break;
			switch (expr->o.un.op)
			{
				case '+':
					break;
				case '-':
					dest->v.i = -dest->v.i;
					break;
			}
			break;
		case F_EC_BINOP:
		{
			struct filter_value l, r;
			filter_eval_expr(&l, args, expr->o.bin.l);
			filter_eval_expr(&r, args, expr->o.bin.r);
			if (expr->o.bin.op != '.' && (l.undef || r.undef))
			{
				dest->undef = 1;
				break;
			}
			switch (expr->o.bin.op)
			{
				case '+':
					dest->v.i = l.v.i + r.v.i;
					break;
				case '-':
					dest->v.i = l.v.i - r.v.i;
					break;
				case '*':
					dest->v.i = l.v.i * r.v.i;
					break;
				case '/':
					if (r.v.i)
						dest->v.i = l.v.i / r.v.i;
					else
						dest->undef = 1;
					break;
				case '%':
					if (r.v.i)
						dest->v.i = l.v.i % r.v.i;
					else
						dest->undef = 1;
					break;
				case '^':
				{
					int i = 1, mask;
					for (mask=0x4000000; mask; mask >>= 1)
					{
						i *= i;
						if (r.v.i & mask)
							i *= l.v.i;
					}
					dest->v.i = i;
					break;
				}
				case '&':
					dest->v.i = l.v.i & r.v.i;
					break;
				case '|':
					dest->v.i = l.v.i | r.v.i;
					break;
				case '.':
				{
					int len = printable_length(&l) + printable_length(&r) + 1;
					dest->v.s = mp_alloc(args->pool, len);
					value_sprintf(dest->v.s, &l);
					value_sprintf(dest->v.s+strlen(dest->v.s), &r);
					dest->undef = 0;
					break;
				}
				case INTERVAL:
					dest->v.interval[0] = l.v.interval[0];
					dest->v.interval[1] = r.v.interval[0];
					break;
			}
			break;
		}
		case F_EC_FUNC:
		{
			struct filter_value a[MAX_FUNC_ARGS];
			int i;
			for (i=0; i < MAX_FUNC_ARGS && expr->o.func.a[i]; i++)
				filter_eval_expr(a+i, args, expr->o.func.a[i]);
			dest->undef = 0;
			(*expr->o.func.func->f)(args, dest, a);
			break;
		}
		default:
			ASSERT(0);
	}
}

static inline int
compare_int(struct filter_value *l, int unsign, int op, struct filter_value *r)
{
	if (l->undef || r->undef)
		return UNDEFINED;
	if (!unsign)
		switch (op)
		{
			case LT:
				return  l->v.i < r->v.i ? TRUE : FALSE;
			case GT:
				return  l->v.i > r->v.i ? TRUE : FALSE;
			case EQ:
				return  l->v.i == r->v.i ? TRUE : FALSE;
			default:
				ASSERT(0);
		}
	else
		switch (op)
		{
			case LT:
				return  l->v.u < r->v.u ? TRUE : FALSE;
			case GT:
				return  l->v.u > r->v.u ? TRUE : FALSE;
			case EQ:
				return  l->v.u == r->v.u ? TRUE : FALSE;
			default:
				ASSERT(0);
		}
	return UNDEFINED;
}

static inline int
compare_string(struct filter_value *l, int icase, int op, struct filter_value *r)
{
	int res;

	if (l->undef || r->undef)
		return UNDEFINED;
	/* EREG is handled elsewhere, because it needs the precompiled regexp.  */
	if (op == EPAT)
	{
		if (icase)
			return str_match_pattern_nocase(r->v.s, l->v.s) ? TRUE : FALSE;
		else
			return str_match_pattern(r->v.s, l->v.s) ? TRUE : FALSE;
	}

	if (icase)
		res = strcasecmp(l->v.s, r->v.s);
	else
		res = strcmp(l->v.s, r->v.s);

	switch (op)
	{
		case LT:
			return  res<0 ? TRUE : FALSE;
		case GT:
			return  res>0 ? TRUE : FALSE;
		case EQ:
			return  res==0 ? TRUE : FALSE;
		default:
			ASSERT(0);
	}
	return UNDEFINED;
}

static inline int
log_not(int x)
{
	switch (x)
	{
		case TRUE:
			return FALSE;
		case FALSE:
			return TRUE;
		case UNDEFINED:
			return UNDEFINED;
		default:
			ASSERT(0);
	}
	return UNDEFINED;
}

int
filter_eval_cond(struct filter_args *args, struct filter_cond *cond, struct filter_expr *partial_expr)
{
	if (cond->undef)
		return UNDEFINED;
	switch (cond->cat)
	{
		case F_CC_CONST:
			return cond->o.i;
		case F_CC_EXPR:
		{
			struct filter_value l, r;
			int res;
			filter_eval_expr(&l, args, cond->o.expr.l ? : partial_expr);
			filter_eval_expr(&r, args, cond->o.expr.r);
			if (cond->o.expr.op == EREG)
			{
				if (l.undef)
					return UNDEFINED;
				/* ignoring case already handled when compiled */
				res = rx_match(args->filter->lookup[r.v.i].regex->regex, l.v.s) ? TRUE : FALSE;
			}
			else if (cond->o.expr.op == EIN)
			{
				int res1, res2;
				struct filter_value rr = r;
				if (l.type == F_ET_INT)
				{
					res1 = compare_int(&l, cond->o.expr.icase, LT, &rr);
					rr.v.i = r.v.interval[1].i;
					res2 = compare_int(&l, cond->o.expr.icase, GT, &rr);
				}
				else
				{
					res1 = compare_string(&l, cond->o.expr.icase, LT, &rr);
					rr.v.s = r.v.interval[1].s;
					res2 = compare_string(&l, cond->o.expr.icase, GT, &rr);
				}
				if (res1 == TRUE || res2 == TRUE)
					res = FALSE;
				else if (res1 == UNDEFINED || res2 == UNDEFINED)
					res = UNDEFINED;
				else
					res = TRUE;
			}
			else
			{
				if (l.type == F_ET_INT)
					res = compare_int(&l, cond->o.expr.icase, cond->o.expr.op, &r);
				else if (l.type == F_ET_STRING)
					res = compare_string(&l, cond->o.expr.icase, cond->o.expr.op, &r);
				else
					ASSERT(0);
			}
			if (cond->o.expr.neg)
				res = log_not(res);
			return res;
		}
		case F_CC_DEFCOND:
		{
			int r;
			r = filter_eval_cond(args, cond->o.neg, NULL);
			return r == UNDEFINED ? FALSE : TRUE;
		}
		case F_CC_DEFEXPR:
		{
			struct filter_value arg;
			filter_eval_expr(&arg, args, cond->o.defexpr);
			return arg.undef ? FALSE : TRUE;
		}
		case F_CC_UNOP:
		{
			int r;
			r = filter_eval_cond(args, cond->o.neg, NULL);	/* ! */
			return log_not(r);
		}
		case F_CC_BINOP:
		{
			int l, r;
			l = filter_eval_cond(args, cond->o.bin.l, NULL);
			r = filter_eval_cond(args, cond->o.bin.r, NULL);
			switch (cond->o.bin.op)
			{
				case AND:
					return l==TRUE && r==TRUE
						? TRUE
						: l==FALSE || r==FALSE
							? FALSE
							: UNDEFINED;
				case OR:
					return l==FALSE && r==FALSE
						? FALSE
						: l==TRUE || r==TRUE
							? TRUE
							: UNDEFINED;
				case EQ:
					return l==UNDEFINED || r==UNDEFINED
						? UNDEFINED
						: l == r
							? TRUE
							: FALSE;
				case NE:
					return l==UNDEFINED || r==UNDEFINED
						? UNDEFINED
						: l != r
							? TRUE
							: FALSE;
				default:
					ASSERT(0);
			}
		}
		default:
			ASSERT(0);
	}
}

static byte *
return_msg(struct filter_args *args, struct filter_expr *expr)
{
	struct filter_value a;
	byte *tmp;
	filter_eval_expr(&a, args, expr);
	if (a.undef)
		return NULL;
	switch (a.type)
	{
		case F_ET_INT:
			tmp = mp_alloc(args->pool, 16);
			sprintf(tmp, "%d", a.v.i);
			return tmp;
		case F_ET_STRING:
			return a.v.s;
		case F_ET_REGEXP:
			tmp = mp_alloc(args->pool, 16);
			sprintf(tmp, "<regexp>");
			return tmp;
		default:
			ASSERT(0);
	}
}

#define ASORT_PREFIX(x) cases_##x
#define ASORT_KEY_TYPE struct filter_case *
#define ASORT_LT(x, y) ((x)->case_id < (y)->case_id)
#include "ucw/sorter/array-simple.h"

static int
filter_eval_cmd(struct filter_args *args, struct filter_cmd *cmd)
{
	struct filter_value a;

	if (!cmd)
		return 0;
	while (cmd)
	{
		switch (cmd->op)
		{
			case 0:	/* Empty command has to be skipped (declarations...).  */
				break;
			case LOG1:
				filter_eval_expr(&a, args, cmd->c.print.expr);
				value_log(cmd->c.print.level, &a);
				break;
			case ACCEPT:
				if (cmd->c.print.expr)
					args->msg = return_msg(args, cmd->c.print.expr);
				return ACCEPT;
			case REJECT:
				if (cmd->c.print.expr)
					args->msg = return_msg(args, cmd->c.print.expr);
				return REJECT;
			case '=':
				filter_eval_expr(&a, args, cmd->c.set.expr);
				filter_set_lvalue(cmd->c.set.lv, args, &a, 0);
				break;
			case ADD:
				filter_eval_expr(&a, args, cmd->c.set.expr);
				filter_set_lvalue(cmd->c.set.lv, args, &a, 1);
				break;
			case DELETE:
				filter_set_lvalue(cmd->c.set.lv, args, NULL, 0);
				break;
			case IF:
			{
				int cond, res;
				cond = filter_eval_cond(args, cmd->c.cond.cond, NULL);
				switch (cond)
				{
					case TRUE:
						res = filter_eval_cmd(args, cmd->c.cond.positive);
						break;
					case FALSE:
						res = filter_eval_cmd(args, cmd->c.cond.negative);
						break;
					case UNDEFINED:
						res = filter_eval_cmd(args, cmd->c.cond.undefined);
						break;
					default:
						ASSERT(0);
				}
				if (res)
					return res;
				break;
			}
			case SWITCH:
			{
				struct filter_case *cas, *buf[16]; // rerely need to reallocate
				struct filter_cases res = { .args = args, .list = buf, .size = ARRAY_SIZE(buf) };
				int ires;
				filter_eval_expr(&a, args, cmd->c.swit.expr);
				if (a.undef)
				{
					if (ires = filter_eval_cmd(args, cmd->c.swit.undefined))
						return ires;
					break;
				}
				if (cmd->c.swit.cmp || cmd->c.swit.icmp || cmd->c.swit.pat || cmd->c.swit.ipat)
					ASSERT(a.type == F_ET_STRING);
				if (cmd->c.swit.cmp)
					filter_ht_find(cmd->c.swit.cmp, a.v.s, &res);
				if (cmd->c.swit.icmp)
					filter_ht_find(cmd->c.swit.icmp, a.v.s, &res);
				if (cmd->c.swit.kmp)
					filter_kmp_find(cmd->c.swit.kmp, a.v.s, &res);
				if (cmd->c.swit.ikmp)
					filter_kmp_find(cmd->c.swit.ikmp, a.v.s, &res);
				if (cmd->c.swit.pat)
					filter_trie_search(args->filter->lookup[cmd->c.swit.pat].trie, a.v.s, &res);
				if (cmd->c.swit.ipat)
					filter_trie_search(args->filter->lookup[cmd->c.swit.ipat].trie, a.v.s, &res);
				if (cmd->c.swit.expr->type == F_ET_STRING)
				{
					if (cmd->c.swit.bins)
						filter_s_tree_search(cmd->c.swit.bins, a.v.s, &res);
					else if (cmd->c.swit.binis)
						filter_is_tree_search(cmd->c.swit.binis, a.v.s, &res);
				}
				else
				{
					if (cmd->c.swit.binud)
						filter_ud_tree_search(cmd->c.swit.binud, a.v.i, &res);
					else if (cmd->c.swit.bind)
						filter_d_tree_search(cmd->c.swit.bind, a.v.i, &res);
				}
				for (cas=cmd->c.swit.cases; cas; cas=cas->next)
				{
					ires = filter_eval_cond(args, cas->cond, cmd->c.swit.expr);
					if (ires == TRUE)
						filter_cases_add(&res, cas);
				}
				if (!res.count)
				{
					if (ires = filter_eval_cmd(args, cmd->c.swit.negative))
						return ires;
					break;
				}
				cases_sort(res.list, res.count);
				for (uns i = 0; i < res.count; i++)
					if (!i || res.list[i]->case_id != res.list[i - 1]->case_id)
					{
						ires = filter_eval_cmd(args, res.list[i]->positive);
						if (ires)
							return ires;
					}
				break;
			}
			default:
				ASSERT(0);
		}
		cmd = cmd->next;
	}
	return 0;
}

static int
filter_user_var_init2(struct filter_declaration *fd, union filter_raw_value *user_var)
{
	int count = 0;
	for (; fd; count++, fd=fd->next)
	{
		if (fd->var.type == F_ET_INT)
			user_var[fd->nr].i = F_UNDEF_INT;
		else if (fd->var.type == F_ET_STRING)
			user_var[fd->nr].s = F_UNDEF_STRING;
		else if(fd->var.type == F_ET_REGEXP)
			user_var[fd->nr].r = F_UNDEF_REGEXP;
		else
			ASSERT(0);
	}
	return count;
}

static void
filter_user_var_init(struct filter_args *a)
{
	int count = 0;
	count += filter_user_var_init2(a->filter->decl, a->user_var);
	count += filter_user_var_init2(a->filter->deleted_decl, a->user_var);
	ASSERT(count == a->filter->user_vars);
}

void
filter_intr_undo_init(struct filter_args *a)
{
	/* All the changes will be commited, because the undo pool is forgotten
	 * and reset to the initial state.  */
	a->saved_pool = cf_pool;
	cf_pool = a->pool; /* faster than allocation of a new pool */
	a->oldj = cf_journal_new_transaction(0);
}

void
filter_intr_undo(struct filter_args *a)
{
	cf_journal_rollback_transaction(0, a->oldj);
	a->oldj = NULL;
	cf_pool = a->saved_pool;
}

int
filter_intr_run(struct filter_args *a)
{
	int res;

	a->msg = NULL;
	filter_user_var_init(a);
	if (a->config_changes_mode == 2)
	                   filter_intr_undo_init(a);
	res = filter_eval_cmd(a, a->filter->body);
	if (a->config_changes_mode == 2)
	                  filter_intr_undo(a);
	if (res != REJECT && res != ACCEPT)
	{
		log(L_ERROR, "filter: Neither ACCEPT nor REJECT issued, defaulting to REJECT.");
		res = REJECT;
	}
	return res == ACCEPT ? 1 : 0;
}

void
filter_cases_grow(struct filter_cases *r)
{
	ASSERT(r->count == r->size);
	r->size *= 2;
	void *ptr = mp_alloc(r->args->pool, r->size * sizeof(void *));
	memcpy(ptr, r->list, r->count * sizeof(void *));
	r->list = ptr;
}

