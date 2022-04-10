/*
 *	Sherlock Filter Engine --- pruning a filter
 *
 *	(c) 2002--2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "filter/filter.h"
#include "filter/parse.tab.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

static struct filter_args prune_args;		/* prune_args.pool is important for string allocation */

static struct filter_expr *
prune_expr(struct filter_expr *r)
{
	struct filter_value val;
	int replace = 0;
	if (!r || r->undef)
		return r;
	switch (r->cat)
	{
		case F_EC_CONST:
			break;
		case F_EC_LVALUE:
			if (r->o.lv->undef)
				r->undef = 1;
			break;
		case F_EC_UNOP:
			r->o.un.r = prune_expr(r->o.un.r);
			if (r->o.un.op == '+')
				return r->o.un.r;
			else if (r->o.un.r->cat == F_EC_CONST)
			{
				filter_eval_expr(&val, &prune_args, r);
				replace = 1;
			}
			break;
		case F_EC_BINOP:
			r->o.bin.l = prune_expr(r->o.bin.l);
			r->o.bin.r = prune_expr(r->o.bin.r);
			if (r->o.bin.l->cat == F_EC_CONST
			&& r->o.bin.r->cat == F_EC_CONST
			&& r->o.bin.op != INTERVAL)
			{
				filter_eval_expr(&val, &prune_args, r);
				if (!val.undef)
					replace = 1;
			}
#define	HAS_E_VALUE(expr,val)	(expr->cat == F_EC_CONST && expr->o.i == val)
			else switch (r->o.bin.op)
			{
				case '+':
					if (HAS_E_VALUE(r->o.bin.l, 0))
						return r->o.bin.r;
					if (HAS_E_VALUE(r->o.bin.r, 0))
						return r->o.bin.l;
					break;
				case '-':
					if (HAS_E_VALUE(r->o.bin.r, 0))
						return r->o.bin.l;
					break;
				case '*':
					if (HAS_E_VALUE(r->o.bin.l, 0)
					|| HAS_E_VALUE(r->o.bin.r, 0))
						val.v.i = 0, replace = 1;
					else if (HAS_E_VALUE(r->o.bin.l, 1))
						return r->o.bin.r;
					else if (HAS_E_VALUE(r->o.bin.r, 1))
						return r->o.bin.l;
					break;
				case '/':
				case '%':
					if (HAS_E_VALUE(r->o.bin.l, 0))
						val.v.i = 0, replace = 1;
					else if (r->o.bin.op == '/' && HAS_E_VALUE(r->o.bin.r, 1))
						return r->o.bin.l;
					break;
				case '^':
					if (HAS_E_VALUE(r->o.bin.r, 0))
						val.v.i = 1, replace = 1;
					if (HAS_E_VALUE(r->o.bin.r, 1))
						return r->o.bin.l;
					if (HAS_E_VALUE(r->o.bin.l, 0))
						val.v.i = 0, replace = 1;
					if (HAS_E_VALUE(r->o.bin.l, 1))
						val.v.i = 1, replace = 1;
					break;
				case '&':
					if (HAS_E_VALUE(r->o.bin.l, 0)
					|| HAS_E_VALUE(r->o.bin.r, 0))
						val.v.i = 0, replace = 1;
					break;
				case '|':
					if (HAS_E_VALUE(r->o.bin.l, 0))
						return r->o.bin.r;
					if (HAS_E_VALUE(r->o.bin.r, 0))
						return r->o.bin.l;
					break;
			}
			break;
		case F_EC_FUNC:
			replace = 1;
			for (uns i=0; i<MAX_FUNC_ARGS && r->o.func.a[i]; i++)
			{
				r->o.func.a[i] = prune_expr(r->o.func.a[i]);
				if (r->o.func.a[i]->cat != F_EC_CONST)
					replace = 0;
			}
			if (replace)
				filter_eval_expr(&val, &prune_args, r);
			break;
	}
	if (replace)
	{
		r->cat = F_EC_CONST;
		r->o.s = val.v.s;
	}
	return r;
}

static struct filter_cond *
prune_cond(struct filter_cond *c)
{
	int res = 0;
	if (!c || c->undef)
		return c;
	switch (c->cat)
	{
		case F_CC_CONST:
			if (c->o.i == UNDEFINED)
				c->undef = 1;
			break;
		case F_CC_EXPR:
			c->o.expr.l = prune_expr(c->o.expr.l);
			c->o.expr.r = prune_expr(c->o.expr.r);
			if (c->o.expr.l)	/* not a partial condition */
			{
				if (c->o.expr.l->cat == F_EC_CONST
				&& c->o.expr.r->cat == F_EC_CONST)
					res = filter_eval_cond(&prune_args, c, NULL);
				else if (c->o.expr.l->undef || c->o.expr.r->undef)
					res = UNDEFINED;
			}
			break;
		case F_CC_DEFEXPR:
			c->o.defexpr = prune_expr(c->o.defexpr);
			if (c->o.defexpr->undef
			|| c->o.defexpr->cat == F_EC_CONST)
				res = filter_eval_cond(&prune_args, c, NULL);
			/* otherwise the condition can/cannot be defined
			 * depending on the values of the operands */
			break;
		case F_CC_DEFCOND:
		case F_CC_UNOP:
			c->o.neg = prune_cond(c->o.neg);
			if (c->o.neg->undef
			|| c->o.neg->cat == F_CC_CONST)
				res = filter_eval_cond(&prune_args, c, NULL);
			break;
		case F_CC_BINOP:
			c->o.bin.l = prune_cond(c->o.bin.l);
			c->o.bin.r = prune_cond(c->o.bin.r);
			if (c->o.bin.l->cat == F_CC_CONST && c->o.bin.r->cat == F_CC_CONST)
				res = filter_eval_cond(&prune_args, c, NULL);
#define	HAS_C_VALUE(cond,val)	(cond->cat == F_CC_CONST && cond->o.i == val)
			else switch (c->o.bin.op)
			{
				case AND:
					if (HAS_C_VALUE(c->o.bin.l, FALSE)
					|| HAS_C_VALUE(c->o.bin.r, FALSE))
						res = FALSE;
					else if (HAS_C_VALUE(c->o.bin.l, TRUE))
						return c->o.bin.r;
					else if (HAS_C_VALUE(c->o.bin.r, TRUE))
						return c->o.bin.l;
					else if (c->o.bin.l->undef && c->o.bin.r->undef)
						res = UNDEFINED;
					break;
				case OR:
					if (HAS_C_VALUE(c->o.bin.l, TRUE)
					|| HAS_C_VALUE(c->o.bin.r, TRUE))
						res = TRUE;
					else if (HAS_C_VALUE(c->o.bin.l, FALSE))
						return c->o.bin.r;
					else if (HAS_C_VALUE(c->o.bin.r, FALSE))
						return c->o.bin.l;
					else if (c->o.bin.l->undef && c->o.bin.r->undef)
						res = UNDEFINED;
					break;
				case EQ:
				case NE:
					if (c->o.bin.l->undef || c->o.bin.r->undef)
						res = UNDEFINED;
					break;
			}
			break;
		default:
			ASSERT(0);
	}
	if (res)
	{
		c->cat = F_CC_CONST;
		c->o.i = res;
		if (res == UNDEFINED)
			c->undef = 1;
	}
	return c;
}

static int prune_switch_command(struct filter_cmd *cmd);

/* Returns the NEW last command in this chain.  */
static struct filter_cmd *
prune_command(struct filter_cmd **Cmd)
{
	struct filter_cmd *cmd, *replace_by, *new_last;
	int delete;
start:
	cmd = *Cmd;
	if (!cmd)
		return NULL;
	if (cmd->pruned)
		return cmd;
	delete = 0;
	replace_by = NULL;
	/* Check whether the command could be deleted.  */
	switch (cmd->op)
	{
		case 0:
			delete = 1;
			break;
		case LOG1:
		case ACCEPT:
		case REJECT:
			cmd->c.print.expr = prune_expr(cmd->c.print.expr);
			if (cmd->op == LOG1 && cmd->c.print.expr->undef)
				delete = 1;
				/* This changes slightly the behaviour of the
				 * filter, since the undefined value would be
				 * otherwise logged.  */
			break;
		case '=':
		case ADD:
		case DELETE:
			cmd->c.set.expr = prune_expr(cmd->c.set.expr);
			if (cmd->c.set.lv->undef)
				delete = 1;
			/* The assignment of an undefined value to a defined
			 * variable should NOT be omitted.  */
			break;
		case IF:
			cmd->c.cond.cond = prune_cond(cmd->c.cond.cond);
			if (cmd->c.cond.cond->undef)
			{
				prune_command(&cmd->c.cond.undefined);
				replace_by = cmd->c.cond.undefined;
				if (!replace_by)
					delete = 1;
			}
			else
			{
				prune_command(&cmd->c.cond.positive);
				prune_command(&cmd->c.cond.negative);
				prune_command(&cmd->c.cond.undefined);
				if (!cmd->c.cond.positive
				&& !cmd->c.cond.negative
				&& !cmd->c.cond.undefined)
					delete = 1;
				else if (cmd->c.cond.cond->cat == F_CC_CONST)
				{
					if (cmd->c.cond.cond->o.i == TRUE)
						replace_by = cmd->c.cond.positive;
					else
						replace_by = cmd->c.cond.negative;
					if (!replace_by)
						delete = 1;
				}
			}
			break;
		case SWITCH:
			cmd->c.swit.expr = prune_expr(cmd->c.swit.expr);
			if (cmd->c.swit.expr->undef)
			{
				prune_command(&cmd->c.swit.undefined);
				replace_by = cmd->c.swit.undefined;
				if (!replace_by)
					delete = 1;
			}
			else
			{
				if (prune_switch_command(cmd))
					delete = 1;
				else if (cmd->c.swit.expr->cat == F_EC_CONST)
				{
/* FIXME: delete the code since tables are not built yet
					if (cmd->c.swit.cmp || cmd->c.swit.icmp || cmd->c.swit.pat || cmd->c.swit.ipat
						|| cmd->c.swit.bins || cmd->c.swit.binis)
					{
						struct filter_cmd *run_cmd;
						if (cmd->c.swit.cmp && filter_ht_find(cmd->c.swit.cmp, cmd->c.swit.expr->o.s, &run_cmd))
							replace_by = run_cmd;
						else if (cmd->c.swit.icmp && filter_ht_find(cmd->c.swit.icmp, cmd->c.swit.expr->o.s, &run_cmd))
							replace_by = run_cmd;
						else if (cmd->c.swit.pat && (run_cmd = filter_trie_search(cmd->c.swit.pat, cmd->c.swit.expr->o.s)) )
							replace_by = run_cmd;
						else if (cmd->c.swit.ipat && (run_cmd = filter_trie_search(cmd->c.swit.ipat, cmd->c.swit.expr->o.s)) )
							replace_by = run_cmd;
						else {
							if (cmd->c.swit.expr->type == F_ET_STRING)
							{
								if (cmd->c.swit.bins && (run_cmd = filter_s_tree_search(cmd->c.swit.bins, cmd->c.swit.expr->o.s)) )
									replace_by = run_cmd;
								else if (cmd->c.swit.binis && (run_cmd = filter_is_tree_search(cmd->c.swit.binis, cmd->c.swit.expr->o.s)) )
									replace_by = run_cmd;
							}
							else
							{
								if (cmd->c.swit.binud && (run_cmd = filter_ud_tree_search(cmd->c.swit.binud, cmd->c.swit.expr->o.i)) )
									replace_by = run_cmd;
								else if (cmd->c.swit.bind && (run_cmd = filter_d_tree_search(cmd->c.swit.bind, cmd->c.swit.expr->o.i)) )
									replace_by = run_cmd;
							}
						}
					}
*/
					if (!replace_by)
						for (struct filter_case *cas=cmd->c.swit.cases; cas; cas=cas->next)
						if (cas->cond->o.expr.r->cat == F_EC_CONST
						|| (cas->cond->o.expr.r->cat == F_EC_BINOP
							&& cas->cond->o.expr.r->o.bin.op == INTERVAL
							&& cas->cond->o.expr.r->o.bin.l->cat == F_EC_CONST
							&& cas->cond->o.expr.r->o.bin.r->cat == F_EC_CONST))
						{
							int res = filter_eval_cond(&prune_args, cas->cond, cmd->c.swit.expr);
							if (res == TRUE)
							{
								replace_by = cas->positive;
								break;
							}
						}
				}
			}
			break;
		default:
			break;
	}
	/* Now perform the desired changes.  */
	if (delete)
	{
		*Cmd = cmd->next;
		goto start;
	}
	if (replace_by)
	{
		*Cmd = replace_by;
		ASSERT(replace_by->last);
		replace_by->last->next = cmd->next;
		cmd = replace_by->last;
	}
	new_last = prune_command(&cmd->next);
	if (new_last)
	{
		ASSERT(cmd->next);
		cmd->last = new_last;
		cmd = new_last;
	}
	else
		cmd->last = cmd;
	cmd->pruned = 1;
	return cmd;
}

/* Returns the NEW last case in this chain.  */
static void
prune_switch_case(struct filter_case **Case, uns remove_empty)
{
	struct filter_case *c, **p = Case, *l = NULL;
	for (c = *Case; c; c = c->next)
	{
		c->cond = prune_cond(c->cond);
		ASSERT(c->undef == c->cond->undef);
		prune_command(&c->positive);
		if (!remove_empty || !(c->undef || !c->positive))
		{
			*p = l = c;
			p = &c->next;
		}
		else
		{
			/* This deletes the CASE if the command is empty, hence other
			 * matches can take part in.  If the cases are overlapping, the
			 * variable can match another case.  Tell me if you think it
			 * matters.  */
		}
	}
	*p = NULL;
	for (c = *Case; c; c = c->next)
		c->last = l;
}

/* Returns whether the whole SWITCH command should be deleted.  */
static int
prune_switch_command(struct filter_cmd *cmd)
{
	ASSERT(!cmd->c.swit.cmp);	/* should be contructed AFTERwards */
	ASSERT(!cmd->c.swit.icmp);
	ASSERT(!cmd->c.swit.pat);
	ASSERT(!cmd->c.swit.ipat);
	ASSERT(!cmd->c.swit.kmp);
	ASSERT(!cmd->c.swit.ikmp);
	ASSERT(!cmd->c.swit.bins);
	ASSERT(!cmd->c.swit.binis);
	prune_command(&cmd->c.swit.negative);
	prune_command(&cmd->c.swit.undefined);
	prune_switch_case(&cmd->c.swit.cases, !cmd->c.swit.negative);
	if (!cmd->c.swit.cases
	&& !cmd->c.swit.negative
	&& !cmd->c.swit.undefined)
		return 1;
	else
		return 0;
}

static int
is_hashable(struct filter_case *cs)
{
	struct filter_cond *cond = cs->cond;
	ASSERT(cond->cat == F_CC_EXPR);
	if (cs->type == F_ET_STRING
	&& cond->o.expr.op == EQ
	&& !cond->o.expr.neg
	&& cond->o.expr.r->cat == F_EC_CONST)
	{
		ASSERT(cond->o.expr.r->type == F_ET_STRING);
		if (cond->o.expr.icase)
			return 2;
		else
			return 1;
	}
	else
		return 0;
}

static struct filter_hash_table *
build_hash_table(struct filter *f, struct filter_cmd *cmd, int icase, uns nr_tests)
{
	struct filter_hash_table *ht;
	struct filter_case **cs = &cmd->c.swit.cases;
	ht = filter_ht_new(f->pool, nr_tests, icase);
	while (*cs)
	{
		if (is_hashable(*cs) != icase + 1)
		{
			cs = &(*cs)->next;
			continue;
		}
		filter_ht_add(ht, (*cs)->cond->o.expr.r->o.s, *cs);
		*cs = (*cs)->next;
	}
	ASSERT(ht->count == nr_tests);
	return ht;
}

static int
can_be_in_kmp(struct filter_case *cs)
{
	struct filter_cond *cond = cs->cond;
	ASSERT(cond->cat == F_CC_EXPR);
	if (cs->type == F_ET_STRING
	&& cond->o.expr.op == EPAT
	&& !cond->o.expr.neg
	&& cond->o.expr.r->cat == F_EC_CONST)
	{
		byte *s = cond->o.expr.r->o.s;
		if (*s++ != '*' || *s == '*')
			return 0;
		while (*s != '*')
		{
			if (*s == '\\')
				s++;
			if (!*s)
				return 0;
			s++;
		}
		if (s[1])
			return 0;
		ASSERT(cond->o.expr.r->type == F_ET_STRING);
		if (cond->o.expr.icase)
			return 2;
		else
			return 1;
	}
	else
		return 0;
}

static struct filter_kmp_table *
build_kmp_table(struct filter *f, struct filter_cmd *cmd, int icase)
{
	struct filter_kmp_table *kmp;
	struct filter_case **cs = &cmd->c.swit.cases, *x;
	kmp = filter_kmp_new(f->pool, icase);
	while (*cs)
	{
		if (can_be_in_kmp(*cs) != icase + 1)
		{
			cs = &(*cs)->next;
			continue;
		}
		filter_kmp_add(kmp, (*cs)->cond->o.expr.r->o.s, *cs);
		x = (*cs)->next;
		(*cs)->next = kmp->cases;
		kmp->cases = *cs;
		*cs = x;
	}
	filter_kmp_build(kmp);
	return kmp;
}

static void
possibly_create_lookup_tables(struct filter *f, struct filter_cmd *cmd)
{
	uns cmps[3] = {0, 0, 0};
	uns kmps[3] = {0, 0, 0};
	struct filter_case *cs;
	ASSERT(cmd->op == SWITCH);

	for (cs=cmd->c.swit.cases; cs; cs=cs->next)
	{
		cmps[ is_hashable(cs) ]++;
		kmps[ can_be_in_kmp(cs) ]++;
	}
	if (cmps[1] >= filter_hash_limit)
		cmd->c.swit.cmp = build_hash_table(f, cmd, 0, cmps[1]);
	if (cmps[2] >= filter_hash_limit)
		cmd->c.swit.icmp = build_hash_table(f, cmd, 1, cmps[2]);
	if (kmps[1] >= filter_kmp_limit)
		cmd->c.swit.kmp = build_kmp_table(f, cmd, 0);
	if (kmps[2] >= filter_kmp_limit)
		cmd->c.swit.ikmp = build_kmp_table(f, cmd, 1);
	cmd->c.swit.pat = filter_lookup_new_no_null(f, F_LT_TRIE, filter_trie_new(f->pool, cmd, 0));
	cmd->c.swit.ipat = filter_lookup_new_no_null(f, F_LT_TRIE, filter_trie_new(f->pool, cmd, 1));
	if (cmd->c.swit.expr->type == F_ET_STRING)
	{
		cmd->c.swit.bins = filter_s_tree_new(f->pool, cmd);
		cmd->c.swit.binis = filter_is_tree_new(f->pool, cmd);
	}
	else
	{
		cmd->c.swit.binud = filter_ud_tree_new(f->pool, cmd);
		cmd->c.swit.bind = filter_d_tree_new(f->pool, cmd);
	}
}

static void
recursively_hash_tables(struct filter *f, struct filter_cmd *cmd)
{
	struct filter_case *cs;

	for (; cmd; cmd=cmd->next)
	{
		if (cmd->pruned == 2)
			continue;
		cmd->pruned = 2;
		switch (cmd->op)
		{
			case IF:
				recursively_hash_tables(f, cmd->c.cond.positive);
				recursively_hash_tables(f, cmd->c.cond.negative);
				recursively_hash_tables(f, cmd->c.cond.undefined);
				break;
			case SWITCH:
				for (cs=cmd->c.swit.cases; cs; cs=cs->next)
					recursively_hash_tables(f, cs->positive);
				recursively_hash_tables(f, cmd->c.cond.negative);
				recursively_hash_tables(f, cmd->c.cond.undefined);
				possibly_create_lookup_tables(f, cmd);
				break;
			default:
				break;
		}
	}
}

void
filter_prune(struct filter *f)
{
	bzero(&prune_args, sizeof(prune_args));
	prune_args.filter = f;
	prune_args.pool = f->pool;
	if (filter_optimize)
		prune_command(&f->body);
	recursively_hash_tables(f, f->body);
}
