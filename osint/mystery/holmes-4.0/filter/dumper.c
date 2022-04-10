/*
 *	Sherlock Filter Engine --- dumping the parsed filter
 *
 *	(c) 2002--2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "filter/filter.h"
#include "filter/parse.tab.h"

#define	LOCAL_VAR_SUFFIX	"_LOCAL_%d"

void
filter_dump_spaces(struct fastbuf *b, uns level)
{
	uns i;
	for (i=0; i<level; i++)
		bputc(b, '\t');
}

static void
dump_declarations(struct fastbuf *b, struct filter_declaration *decl)
{
	if (!decl)
		return;
	dump_declarations(b, decl->next);

	byte *type;
	ASSERT(decl->var.cat == F_LVC_USER);
	ASSERT(!decl->var.ro);
	switch (decl->var.type)
	{
		case F_ET_INT: type = "int"; break;
		case F_ET_STRING: type = "string"; break;
		case F_ET_REGEXP: type = "regexp"; break;
		default: ASSERT(0);
	}
	bprintf(b, "%s %s", type, decl->var.name);
	if (decl->is_local)
		bprintf(b, LOCAL_VAR_SUFFIX, decl->nr);
	bputs(b, ";\n");
}

static void
dump_variable_name(struct fastbuf *b, struct filter_lvalue *lval)
{
	switch (lval->cat)
	{
		case F_LVC_RAW:
			if (lval->undef)
				bputs(b, lval->v.bind_name);
			else
				bputs(b, lval->v.bind->name);
			break;
		case F_LVC_ATTR:
			bprintf(b, "attr[\"%c\"]", lval->v.name);
			break;
		case F_LVC_CONF:
			bprintf(b, "conf[%s]", lval->v.cfg->name);
			break;
		case F_LVC_USER:
			bprintf(b, "%s", lval->v.decl->var.name);
			if (lval->v.decl->is_local)
				bprintf(b, LOCAL_VAR_SUFFIX, lval->v.decl->nr);
			break;
		default:
			ASSERT(0);
	}
}

static void
dump_expression(struct fastbuf *b, struct filter *f, struct filter_expr *e)
{
	int i;
	switch (e->cat)
	{
		case F_EC_CONST:
			switch (e->type)
			{
				case F_ET_INT: bprintf(b, "%d", e->o.i); break;
				case F_ET_STRING: bprintf(b, "\"%s\"", e->o.s); break;
				case F_ET_REGEXP:
				{
					struct filter_regex_value *rv = f->lookup[e->o.i].regex;
					if (rv->precompiled)
					{
						bputc(b, '~');
						if (rv->icase)
							bputc(b, '~');
					}
					bprintf(b, "\"%s\"", rv->source);
					break;
				}
				default: ASSERT(0);
			}
			break;
		case F_EC_LVALUE:
			dump_variable_name(b, e->o.lv);
			break;
		case F_EC_UNOP:
			bputc(b, e->o.un.op);
			dump_expression(b, f, e->o.un.r);
			break;
		case F_EC_BINOP:
			if (e->o.bin.op != INTERVAL)
				bputs(b, " (");
			dump_expression(b, f, e->o.bin.l);
			if (e->o.bin.op != INTERVAL)
			{
				bputc(b, ' ');
				bputc(b, e->o.bin.op);
				bputc(b, ' ');
			}
			else
				bputs(b, " .. ");
			dump_expression(b, f, e->o.bin.r);
			if (e->o.bin.op != INTERVAL)
				bputs(b, ") ");
			break;
		case F_EC_FUNC:
			bputs(b, e->o.func.func->name);
			bputc(b, '(');
			for (i=0; i<e->o.func.func->args; i++)
			{
				dump_expression(b, f, e->o.func.a[i]);
				if (i < e->o.func.func->args-1)
					bputs(b, ", ");
			}
			bputc(b, ')');
			break;
		default:
			ASSERT(0);
	}
}

void
filter_dump_condition(struct fastbuf *b, struct filter *f, struct filter_cond *c)
{
	byte *msg = NULL;
	switch (c->cat)
	{
		case F_CC_CONST:
			switch (c->o.i)
			{
				case TRUE: bputs(b, "true"); break;
				case FALSE: bputs(b, "false"); break;
				case UNDEFINED: bputs(b, "undefined"); break;
				default: ASSERT(0);
			}
			break;
		case F_CC_EXPR:
			if (c->o.expr.l)
			{
				bputs(b, " (");
				dump_expression(b, f, c->o.expr.l);
			}
			bputc(b, ' ');
			if (!c->o.expr.neg)
			{
				switch (c->o.expr.op)
				{
					case EIN: msg = "=#"; break;
					case EREG: msg = "=~"; break;
					case EPAT: msg = "=*"; break;
					case EQ: msg = "=="; break;
					case LT: msg = "<"; break;
					case GT: msg = ">"; break;
					default: ASSERT(0);
				}
			}
			else
			{
				switch (c->o.expr.op)
				{
					case EIN: msg = "!#"; break;
					case EREG: msg = "!~"; break;
					case EPAT: msg = "!*"; break;
					case EQ: msg = "!="; break;
					case LT: msg = ">="; break;
					case GT: msg = "<="; break;
					default: ASSERT(0);
				}
			}
			bputs(b, msg);
			if (c->o.expr.icase)
				bputc(b, msg[strlen(msg) - 1]);
			bputc(b, ' ');
			dump_expression(b, f, c->o.expr.r);
			if (c->o.expr.l)
				bputs(b, ") ");
			break;
		case F_CC_DEFCOND:
			bputs(b, "defined(");
			filter_dump_condition(b, f, c->o.neg);
			bputs(b, ") ");
			break;
		case F_CC_DEFEXPR:
			bputs(b, "defined(");
			dump_expression(b, f, c->o.defexpr);
			bputs(b, ") ");
			break;
		case F_CC_UNOP:
			bputc(b, '!');
			filter_dump_condition(b, f, c->o.neg);
			break;
		case F_CC_BINOP:
			bputs(b, " (");
			filter_dump_condition(b, f, c->o.bin.l);
			switch (c->o.bin.op)
			{
				case AND: bputs(b, " && "); break;
				case OR: bputs(b, " || "); break;
				case EQ: bputs(b, " == "); break;
				case NE: bputs(b, " != "); break;
				default: ASSERT(0);
			}
			filter_dump_condition(b, f, c->o.bin.r);
			bputs(b, ") ");
			break;
		default:
			ASSERT(0);
	}
}

static void
dump_hash_chain(struct fastbuf *b, struct filter *f, struct filter_hash_record *hr, byte *op, uns level)
{
	if (!hr)
		return;
	dump_hash_chain(b, f, hr->next, op, level);
	/* The list is printed in the reverse order so that a dumped and reread
	 * filter stays the same.  */
	filter_dump_spaces(b, level);
	bputs(b, "case ");
	bputs(b, op);
	bputs(b, " \"");
	bputs(b, hr->string);
	bputs(b, "\":\n");
	filter_dump_commands(b, f, hr->cas->positive, level+1);
}

static void
dump_switch_hash(struct fastbuf *b, struct filter *f, struct filter_hash_table *ht, byte *op, uns level)
{
	uns i;
	if (!ht)
		return;
	filter_dump_spaces(b, level);
	bprintf(b, "# hash-table for the operator %s\n", op);
	for (i=0; i<ht->size; i++)
		dump_hash_chain(b, f, ht->h[i], op, level);
}

static void
dump_kmp_chain(struct fastbuf *b, struct filter *f, struct filter_case *cs, byte *op, uns level)
{
	if (!cs)
		return;
	dump_kmp_chain(b, f, cs->next, op, level);
	/* The list is printed in the reverse order so that a dumped and reread
	 * filter stays the same.  */
	filter_dump_spaces(b, level);
	bputs(b, "case ");
	bputs(b, op);
	bputs(b, " \"");
	bputs(b, cs->cond->o.expr.r->o.s);
	bputs(b, "\":\n");
	filter_dump_commands(b, f, cs->positive, level+1);
}

static void
dump_switch_kmp(struct fastbuf *b, struct filter *f, struct filter_kmp_table *kmp, byte *op, uns level)
{
	if (!kmp)
		return;
	filter_dump_spaces(b, level);
	bprintf(b, "# kmp for the operator %s\n", op);
	dump_kmp_chain(b, f, kmp->cases, op, level);
}

static void
dump_switch_trie(struct fastbuf *b, struct filter *f, struct filter_trie_table *trie, byte *op, uns level)
{
	if (!trie)
		return;
	filter_dump_spaces(b, level);
	bprintf(b, "# trie for the operator %s\n", op);
	for (uns i=0; i<trie->cmds; i++)	/* The order is the same as in the original program.  */
	{
		struct filter_case *cas = trie->cmd[i];
		filter_dump_spaces(b, level);
		bputs(b, "case ");
		bputs(b, op);
		bputs(b, " \"");
		bputs(b, cas->cond->o.expr.r->o.s);
		bputs(b, "\":\n");
		filter_dump_commands(b, f, cas->positive, level+1);
	}
	if (filter_trace > 0)
	{
		bputs(b, "/*\n");
		filter_trie_dump(b, f, trie);
		bputs(b, "*/\n");
	}
}

static void
dump_switch_cases(struct fastbuf *b, struct filter *f, struct filter_case *fc, uns level)
{
	if (fc)
	{
		filter_dump_spaces(b, level);
		bputs(b, "# sequence of SWITCH cases\n");
	}
	for (; fc; fc=fc->next)
	{
		filter_dump_spaces(b, level);
		bputs(b, "case ");
		filter_dump_condition(b, f, fc->cond);
		bputs(b, ":\n");
		filter_dump_commands(b, f, fc->positive, level+1);
	}
}

void
filter_dump_commands(struct fastbuf *b, struct filter *f, struct filter_cmd *cmd, uns level)
{
	for (; cmd; cmd=cmd->next)
	{
		byte *msg = NULL;
		if (!cmd->op)
			continue;
		filter_dump_spaces(b, level);
		switch (cmd->op)
		{
			case LOG1:
				switch (cmd->c.print.level)
				{
					case L_DEBUG: msg = "debug"; break;
					case L_INFO_R: msg = "log"; break;
					case L_WARN_R: msg = "warning"; break;
					case L_ERROR_R: msg = "error"; break;
					default: ASSERT(0);
				}
				bputs(b, msg);
				bputc(b, ' ');
				dump_expression(b, f, cmd->c.print.expr);
				bputs(b, ";\n");
				break;
			case ACCEPT:
				msg = "accept";
				/* Fall-thru.  */
			case REJECT:
				if (!msg)
					msg = "reject";
				bputs(b, msg);
				if (cmd->c.print.expr)
				{
					bputc(b, ' ');
					dump_expression(b, f, cmd->c.print.expr);
				}
				bputs(b, ";\n");
				break;
			case ADD:
				msg = "add ";
				/* Fall-thru.  */
			case '=':
				if (msg)
					bputs(b, msg);
				dump_variable_name(b, cmd->c.set.lv);
				bputs(b, " = ");
				dump_expression(b, f, cmd->c.set.expr);
				bputs(b, ";\n");
				break;
			case DELETE:
				bputs(b, "delete ");
				dump_variable_name(b, cmd->c.set.lv);
				bputs(b, ";\n");
				break;
			case IF:
				bputs(b, "if ");
				filter_dump_condition(b, f, cmd->c.cond.cond);
				bputs(b, " {\n");
				filter_dump_commands(b, f, cmd->c.cond.positive, level+1);
				filter_dump_spaces(b, level);
				bputc(b, '}');
				if (cmd->c.cond.negative)
				{
					bputs(b, " else {\n");
					filter_dump_commands(b, f, cmd->c.cond.negative, level+1);
					filter_dump_spaces(b, level);
					bputc(b, '}');
				}
				if (cmd->c.cond.undefined)
				{
					bputs(b, " undef {\n");
					filter_dump_commands(b, f, cmd->c.cond.undefined, level+1);
					filter_dump_spaces(b, level);
					bputc(b, '}');
				}
				bputc(b, '\n');
				break;
			case SWITCH:
				bputs(b, "switch ");
				dump_expression(b, f, cmd->c.swit.expr);
				bputs(b, " {\n");
				dump_switch_hash(b, f, cmd->c.swit.cmp, "==", level+1);
				dump_switch_hash(b, f, cmd->c.swit.icmp, "===", level+1);
				dump_switch_kmp(b, f, cmd->c.swit.kmp, "=*", level+1);
				dump_switch_kmp(b, f, cmd->c.swit.ikmp, "=**", level+1);
				dump_switch_trie(b, f, f->lookup[cmd->c.swit.pat].trie, "=*", level+1);
				dump_switch_trie(b, f, f->lookup[cmd->c.swit.ipat].trie, "=**", level+1);
				if (cmd->c.swit.expr->type == F_ET_STRING)
				{
					filter_s_tree_dump(b, f, cmd->c.swit.bins, level+1);
					filter_is_tree_dump(b, f, cmd->c.swit.binis, level+1);
				}
				else
				{
					filter_ud_tree_dump(b, f, cmd->c.swit.binud, level+1);
					filter_d_tree_dump(b, f, cmd->c.swit.bind, level+1);
				}
				dump_switch_cases(b, f, cmd->c.swit.cases, level+1);
				filter_dump_spaces(b, level);
				bputc(b, '}');
				if (cmd->c.swit.negative)
				{
					bputs(b, " else {\n");
					filter_dump_commands(b, f, cmd->c.swit.negative, level+1);
					filter_dump_spaces(b, level);
					bputc(b, '}');
				}
				if (cmd->c.swit.undefined)
				{
					bputs(b, " undef {\n");
					filter_dump_commands(b, f, cmd->c.swit.undefined, level+1);
					filter_dump_spaces(b, level);
					bputc(b, '}');
				}
				bputc(b, '\n');
				break;
			default:
				ASSERT(0);
		}
	}
}

void
filter_dump(struct fastbuf *b, struct filter *f)
{
	bputs(b, "# Global variables\n");
	dump_declarations(b, f->decl);
	bputs(b, "\n# Context variables\n");
	dump_declarations(b, f->deleted_decl);
	bputs(b, "\n# Program\n");
	filter_dump_commands(b, f, f->body, 0);
}
