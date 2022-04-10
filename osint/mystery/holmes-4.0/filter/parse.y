/*
 *	Sherlock Filter Engine -- Parser
 *
 *	(c) 1999--2000 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2005 Robert Spalek <robert@ucw.cz>
 */

%{
#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/stkstring.h"
#include "filter/filter.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define	YYERROR_VERBOSE
#define	YYPARSE_PARAM	filter

#define yyerror(x) filter_err(x)
#define err(x) filter_err(x)

#define	NEW(x)	(struct x*) filter_alloc(sizeof(struct x))

static struct filter_declaration *new_declaration(struct filter *filt, int type, byte *name, int nr);
static struct filter_cmd *new_cmd_ifbody(struct filter_cond *cond, struct filter_cmd *p, struct filter_cmd *n, struct filter_cmd *u);
static struct filter_cmd *new_cmd_switch(struct filter_expr *expr, struct filter_case *cases, struct filter_cmd *n, struct filter_cmd *u);
static struct filter_case *new_casecond(struct filter_cond *cond);
static struct filter_cond *new_cond_unop(enum filter_cond_cat cat, int op, void *ptr);
static struct filter_cond *new_cond_binop(struct filter_cond *l, int op, struct filter_cond *r);
static struct filter_cond *new_cond_partial_match(int op, int neg, int icase, struct filter_expr *expr);
static struct filter_cond *new_cond_partial_compare(int op, int neg, int icase, struct filter_expr *expr);
static struct filter_expr *new_expr_unop(uns type, int op, struct filter_expr *r);
static struct filter_expr *new_expr_binop(uns type, struct filter_expr *l, int op, struct filter_expr *r);
static struct filter_expr *new_expr_func(struct filter *f, byte *fname, int args, struct filter_expr *arg[MAX_FUNC_ARGS]);
static struct filter_expr *new_pattern_string(byte *string, int icase);
static struct filter_lvalue *new_lvalue_raw(struct filter *f, byte *name);
static struct filter_lvalue *new_lvalue_attr(struct filter *f, byte *name);
static struct filter_lvalue *new_lvalue_conf(struct filter *f, byte *sect, byte *name);
static void add_declarations(struct filter *filt, struct filter_declaration *fd);
static void remove_declarations(struct filter *filt, struct filter_declaration *until);
%}

%union {
	int i;
	byte *s;
	struct filter_declaration *decl;
	struct filter_lvalue *lv;
	struct filter_expr *e;
	struct filter_cond *c;
	struct filter_case *cases;
	struct filter_cmd *n;
}

/* Tokens */

%token ERROR1 WARNING1 LOG1 DEBUG1 ACCEPT REJECT END
%token IF ELSE ELIF UNDEF SWITCH CASE
%token TRUE FALSE UNDEFINED DEFINED ATTR ADD DELETE CONF INT REGEXP
%token PATTERN IPATTERN

%token <i> NUM
%token <s> STRING IDENT

%left OR
%left AND
%left '|'
%left '&'
%nonassoc LT GT LE GE EQ NE EREG NREG EPAT NPAT EIN NIN
%nonassoc LTC GTC LEC GEC EQC NEC EREGC NREGC EPATC NPATC EINC NINC
%left '.' INTERVAL
%left '-' '+'
%left '*' '/' '%'
%right NEG '!'
%right '^'

/* Types */

%type <decl> decl idents
%type <n> command command_list commands block
%type <n> ifbody else1 undef1
%type <cases> cases case1 casecond
%type <c> cond partcond
%type <e> expr interval
%type <lv> lvalue
%type <i> constcond typeid

%%

main:
commands END	{
	((struct filter *)filter)->body = $1;
	return 0;
}
;

decl:
typeid idents ';'	{
	struct filter_declaration *decl;
	$$ = $2;
	for (decl = $2; decl; decl = decl->next)
		decl->var.type = $1;
}
;

typeid:
INT		{ $$=F_ET_INT; }
| STRING	{ $$=F_ET_STRING; }
| REGEXP	{ $$=F_ET_REGEXP; }
;

idents:
IDENT			{ $$=new_declaration(filter, 0, $1, ((struct filter*)filter)->user_vars++); }
| idents ',' IDENT	{
	struct filter_declaration *decl = new_declaration(filter, 0, $3, ((struct filter*)filter)->user_vars++);
	decl->last->next = $1;
	decl->last = $1->last;
	$$ = decl;
}
;

commands:
command_list		{ $$=$1; }
| commands command_list	{ $$=$1; $$->last->next=$2; $$->last=$2->last; }
;

command_list:		/* It ensures that every command has properly set the list pointers.  */
command			{ $$=$1; if (!$$->last) $$->last=$1; }
;

block:
'{' '}'			{ $$=NEW(filter_cmd); $$->op=0; $$->last=$$; }
| '{'			{ $<decl>$ = ((struct filter*) filter)->decl; }
	commands '}'	{ $$=$3; remove_declarations(filter, $<decl>2); }
			/* Restores the stack of internal variables to the original value.  */
;

command:
decl			{ add_declarations(filter, $1); $$=NEW(filter_cmd); $$->op=0; }
| block			{ $$=$1; }	/* It has $1->last already set.  */
| ERROR1 expr ';'	{ $$=NEW(filter_cmd); $$->op=LOG1; $$->c.print.level=L_ERROR_R; $$->c.print.expr=$2; }
| WARNING1 expr ';'	{ $$=NEW(filter_cmd); $$->op=LOG1; $$->c.print.level=L_WARN_R; $$->c.print.expr=$2; }
| LOG1 expr ';'		{ $$=NEW(filter_cmd); $$->op=LOG1; $$->c.print.level=L_INFO_R; $$->c.print.expr=$2; }
| DEBUG1 expr ';'	{ $$=NEW(filter_cmd); $$->op=LOG1; $$->c.print.level=L_DEBUG; $$->c.print.expr=$2; }
| ACCEPT ';'		{ $$=NEW(filter_cmd); $$->op=ACCEPT; $$->c.print.level=L_INFO_R; $$->c.print.expr=NULL; }
| ACCEPT expr ';'	{ $$=NEW(filter_cmd); $$->op=ACCEPT; $$->c.print.level=L_INFO_R; $$->c.print.expr=$2; }
| REJECT ';'		{ $$=NEW(filter_cmd); $$->op=REJECT; $$->c.print.level=L_INFO_R; $$->c.print.expr=NULL; }
| REJECT expr ';'	{ $$=NEW(filter_cmd); $$->op=REJECT; $$->c.print.level=L_INFO_R; $$->c.print.expr=$2; }
| lvalue '=' expr ';'	{
	$$ = NEW(filter_cmd);
	$$->op = '=';
	$$->c.set.lv = $1;
	$$->c.set.expr = $3;
	if ($1->ro)
		err("Read only variable");
	if ($3->undef && $1->cat != F_LVC_USER && !$1->undef)
		err("Assigning undefined value to defined variable");
	if (TYPE_MISMATCH($1->type, $3->type))
		err("Type mismatch in assignment");
}
| ADD lvalue '=' expr ';'	{
	$$ = NEW(filter_cmd);
	$$->op = ADD;
	$$->c.set.lv = $2;
	$$->c.set.expr = $4;
	if ($4->type == F_ET_REGEXP)
		err("Regexp not allowed here");
	if ($2->cat != F_LVC_ATTR)
		err("Not an attribute");
	if ($2->ro)
		err("Read only attribute");
	if ($4->undef && $2->cat != F_LVC_USER && !$2->undef)
		err("Assigning undefined value to defined variable");
	if (TYPE_MISMATCH($2->type, $4->type))
		err("Type mismatch in assignment");
}
| DELETE lvalue ';'	{
	$$ = NEW(filter_cmd);
	$$->op = DELETE;
	$$->c.set.lv = $2;
	if ($2->cat != F_LVC_ATTR)
		err("Not an attribute");
	if ($2->ro)
		err("Read only attribute");
}
| IF ifbody					{ $$=$2; }
| SWITCH expr '{' cases '}' else1 undef1	{ $$ = new_cmd_switch($2, $4, $6, $7); }
;

ifbody:
cond block else1 undef1				{ $$ = new_cmd_ifbody($1, $2, $3, $4); }
| cond block ELIF ifbody			{ $$ = new_cmd_ifbody($1, $2, $4, $4); }
;

cases:
case1		{ $$=$1; }
| cases case1	{
	$$ = $1;
	for (struct filter_case *cas = $2; cas; cas = cas->next)
		cas->case_id = $$->last->case_id + 1;
	$$->last->next = $2;
	$$->last = $2->last;
	if (TYPE_MISMATCH($1->type, $2->type))
		err("Type mismatch in switch");
	TYPE_COMBINE($1->type, $2->type);
}
;

case1:
CASE casecond ':' commands {
	struct filter_case *cas;
	$$ = $2;
	for (cas = $$; cas; cas = cas->next) {
		cas->positive = $4;
		cas->case_id = 0;
	}
}
;

casecond:
partcond		{ $$ = new_casecond($1); }
| casecond ',' partcond	{
	struct filter_case *cas = new_casecond($3);
	$$ = $1;
	$$->last->next = cas;
	$$->last = cas->last;
	if (TYPE_MISMATCH($1->type, cas->type))
		err("Type mismatch in multiple cases in switch");
	TYPE_COMBINE($1->type, cas->type);
}
;

else1:
/* empty */	{ $$ = NULL; }
| ELSE block	{ $$ = $2; }
;

undef1:
/* empty */	{ $$ = NULL; }
| UNDEF block	{ $$ = $2; }
;

cond:
constcond		{ $$=NEW(filter_cond); $$->cat=F_CC_CONST; $$->undef=0; $$->o.i=$1; }
| '(' cond ')'		{ $$ = $2; }
| expr partcond		{
	int t = $2->o.expr.r->type;
	if (t == F_ET_REGEXP)
		t = F_ET_STRING;
	if (TYPE_MISMATCH($1->type, t))
		err("Type mismatch in condition");
	TYPE_COMBINE($1->type, t);
	TYPE_COMBINE($2->o.expr.r->type, $1->type);
	$$ = $2;
	if ($1->undef)
		$$->undef = 1;
	$$->o.expr.l = $1;
}
| '!' cond		{ $$ = new_cond_unop(F_CC_UNOP, '!', $2); }
| DEFINED '(' cond ')'	{ $$ = new_cond_unop(F_CC_DEFCOND, 0, $3); }
| DEFINED '(' expr ')'	{ $$ = new_cond_unop(F_CC_DEFEXPR, 0, $3); }
| cond AND cond		{ $$ = new_cond_binop($1, AND, $3); }
| cond OR cond		{ $$ = new_cond_binop($1, OR, $3); }
| cond EQ cond		{ $$ = new_cond_binop($1, EQ, $3); }
| cond NE cond		{ $$ = new_cond_binop($1, NE, $3); }
;

constcond:
TRUE		{ $$=TRUE; }
| FALSE		{ $$=FALSE; }
| UNDEFINED	{ $$=UNDEFINED; }
;

partcond:
LT expr		{ $$ = new_cond_partial_compare(LT, 0, 0, $2); }
| GT expr	{ $$ = new_cond_partial_compare(GT, 0, 0, $2); }
| LE expr	{ $$ = new_cond_partial_compare(GT, 1, 0, $2); }
| GE expr	{ $$ = new_cond_partial_compare(LT, 1, 0, $2); }
| EQ expr	{ $$ = new_cond_partial_compare(EQ, 0, 0, $2); }
| NE expr	{ $$ = new_cond_partial_compare(EQ, 1, 0, $2); }
| EIN interval	{ $$ = new_cond_partial_compare(EIN, 0, 0, $2); }
| NIN interval	{ $$ = new_cond_partial_compare(EIN, 1, 0, $2); }
| EREG expr	{ $$ = new_cond_partial_match(EREG, 0, 0, $2); }
| NREG expr	{ $$ = new_cond_partial_match(EREG, 1, 0, $2); }
| EPAT expr	{ $$ = new_cond_partial_match(EPAT, 0, 0, $2); }
| NPAT expr	{ $$ = new_cond_partial_match(EPAT, 1, 0, $2); }
| LTC expr	{ $$ = new_cond_partial_compare(LT, 0, 1, $2); }
| GTC expr	{ $$ = new_cond_partial_compare(GT, 0, 1, $2); }
| LEC expr	{ $$ = new_cond_partial_compare(GT, 1, 1, $2); }
| GEC expr	{ $$ = new_cond_partial_compare(LT, 1, 1, $2); }
| EQC expr	{ $$ = new_cond_partial_compare(EQ, 0, 1, $2); }
| NEC expr	{ $$ = new_cond_partial_compare(EQ, 1, 1, $2); }
| EINC interval	{ $$ = new_cond_partial_compare(EIN, 0, 1, $2); }
| NINC interval	{ $$ = new_cond_partial_compare(EIN, 1, 1, $2); }
| EREGC expr	{ $$ = new_cond_partial_match(EREG, 0, 1, $2); }
| NREGC expr	{ $$ = new_cond_partial_match(EREG, 1, 1, $2); }
| EPATC expr	{ $$ = new_cond_partial_match(EPAT, 0, 1, $2); }
| NPATC expr	{ $$ = new_cond_partial_match(EPAT, 1, 1, $2); }
;

interval:
expr INTERVAL expr	{ $$ = new_expr_binop(~0U, $1, INTERVAL, $3); }
;

expr:
NUM			{
	$$ = NEW(filter_expr);
	$$->type = F_ET_INT;
	$$->undef = 0;
	$$->cat = F_EC_CONST;
	$$->o.i = $1;
}
| STRING		{
	$$ = NEW(filter_expr);
	$$->type = F_ET_STRING;
	$$->undef = 0;
	$$->cat = F_EC_CONST;
	$$->o.s = $1;		/* already filter_strdup()'ed */
}
| PATTERN STRING	{ $$ = new_pattern_string($2, 0); }
| IPATTERN STRING	{ $$ = new_pattern_string($2, 1); }
| lvalue		{
	$$ = NEW(filter_expr);
	$$->type = $1->type;
	$$->undef = $1->undef;
	$$->cat = F_EC_LVALUE;
	$$->o.lv = $1;
}
| '(' expr ')'		{ $$=$2; }
| '+' expr %prec NEG	{ $$ = new_expr_unop(F_ET_INT, '+', $2); }
| '-' expr %prec NEG	{ $$ = new_expr_unop(F_ET_INT, '-', $2); }
| expr '+' expr		{ $$ = new_expr_binop(F_ET_INT, $1, '+', $3); }
| expr '-' expr		{ $$ = new_expr_binop(F_ET_INT, $1, '-', $3); }
| expr '*' expr		{ $$ = new_expr_binop(F_ET_INT, $1, '*', $3); }
| expr '/' expr		{ $$ = new_expr_binop(F_ET_INT, $1, '/', $3); }
| expr '%' expr		{ $$ = new_expr_binop(F_ET_INT, $1, '%', $3); }
| expr '^' expr		{ $$ = new_expr_binop(F_ET_INT, $1, '^', $3); }
| expr '&' expr		{ $$ = new_expr_binop(F_ET_INT, $1, '&', $3); }
| expr '|' expr		{ $$ = new_expr_binop(F_ET_INT, $1, '|', $3); }
| expr '.' expr		{ $$ = new_expr_binop(F_ET_STRING, $1, '.', $3); }
| IDENT '(' expr ')'	{ struct filter_expr *arg[MAX_FUNC_ARGS] = { $3 }; $$ = new_expr_func(filter, $1, 1, arg); }
| IDENT '(' expr ',' expr ')'	{ struct filter_expr *arg[MAX_FUNC_ARGS] = { $3, $5 }; $$ = new_expr_func(filter, $1, 2, arg); }
| IDENT '(' expr ',' expr ',' expr ')'	{ struct filter_expr *arg[MAX_FUNC_ARGS] = { $3, $5, $7 }; $$ = new_expr_func(filter, $1, 3, arg); }
| IDENT '(' expr ',' expr ',' expr ',' expr ')'	{ struct filter_expr *arg[MAX_FUNC_ARGS] = { $3, $5, $7, $9 }; $$ = new_expr_func(filter, $1, 4, arg); }
;

lvalue:
IDENT				{ $$ = new_lvalue_raw(filter, $1); }
| ATTR '[' IDENT ']'		{ $$ = new_lvalue_attr(filter, $3); }
| ATTR '[' STRING ']'		{ $$ = new_lvalue_attr(filter, $3); }
| CONF '[' IDENT '.' IDENT ']'	{ $$ = new_lvalue_conf(filter, $3, $5); }
;

%%

void *
filter_alloc(int size)
{
	return mp_alloc_zero(filter_current->pool, size);
}

byte *
filter_strdup(byte *str)
{
	return mp_strdup(filter_current->pool, str);
}

static inline struct filter_declaration *
search_declaration(struct filter_declaration *decl, byte *name)
{
	for (; decl; decl = decl->next)
		if (!strcasecmp(decl->var.name, name))
			return decl;
	return NULL;
}

static struct filter_declaration *
new_declaration(struct filter *filt, int type, byte *name, int nr)
{
	struct filter_declaration *decl = NEW(filter_declaration);
	if (search_declaration(filt->decl, name))
		err("Variable has already been declared");
	decl->next = NULL;
	decl->last = decl;
	decl->var.name = name;
	decl->var.cat = F_LVC_USER;
	decl->var.type = type;
	decl->var.ro = 0;
	decl->nr = nr;
	decl->is_local = 0;
	return decl;
}

static struct filter_cmd *
new_cmd_ifbody(struct filter_cond *cond, struct filter_cmd *p, struct filter_cmd *n, struct filter_cmd *u)
{
	struct filter_cmd *cmd = NEW(filter_cmd);
	cmd->op = IF;
	cmd->c.cond.cond = cond;
	cmd->c.cond.positive = p;
	cmd->c.cond.negative = n;
	cmd->c.cond.undefined = u;
	return cmd;
}

static struct filter_cmd *
new_cmd_switch(struct filter_expr *expr, struct filter_case *cases, struct filter_cmd *n, struct filter_cmd *u)
{
	struct filter_cmd *cmd = NEW(filter_cmd);
	cmd->op = SWITCH;
	cmd->c.swit.expr = expr;
	cmd->c.swit.cases = cases;
	cmd->c.swit.negative = n;
	cmd->c.swit.undefined = u;
	if (TYPE_MISMATCH(expr->type, cases->type))
		err("Type mismatch of switch variable");
	return cmd;
}

static struct filter_case *
new_casecond(struct filter_cond *cond)
{
	struct filter_case *cas;
	cas = NEW(filter_case);
	cas->next = NULL;
	cas->last = cas;
	if (cond->cat != F_CC_EXPR || cond->o.expr.l)
		err("Internal parser error");
	cas->type = cond->o.expr.r->type;
	if (cas->type == F_ET_REGEXP)
		cas->type = F_ET_STRING;
	cas->undef = cond->undef;
	cas->cond = cond;
	cas->positive = NULL;
	return cas;
}

static struct filter_cond *
new_cond_unop(enum filter_cond_cat cat, int op UNUSED, void *ptr)
{
	struct filter_cond *cond = NEW(filter_cond);
	cond->cat = cat;
	cond->undef = 0;		/* sometimes it is NOT defined, but this only for optimization of interpreter */
	cond->o.neg = ptr;		/* if works for struct filter_expr * too */
	return cond;
}

static struct filter_cond *
new_cond_binop(struct filter_cond *l, int op, struct filter_cond *r)
{
	struct filter_cond *cond = NEW(filter_cond);
	cond->cat = F_CC_BINOP;
	cond->undef = 0;		/* sometimes it is NOT defined, but this only for optimization of interpreter */
	cond->o.bin.op = op;
	cond->o.bin.l = l;
	cond->o.bin.r = r;
	return cond;
}

static struct filter_cond *
new_cond_partial_match(int op, int neg, int icase, struct filter_expr *expr)
{
	struct filter_cond *cond = NEW(filter_cond);
	cond->cat = F_CC_EXPR;
	cond->undef = expr->undef;
	cond->o.expr.l = NULL;
	cond->o.expr.r = expr;
	cond->o.expr.neg = neg;
	cond->o.expr.icase = icase;
	cond->o.expr.op = op;
	if (op == EREG || op == NREG)
	{
		if (expr->type == F_ET_REGEXP)
			return cond;
		else if (expr->undef || expr->type != F_ET_STRING || expr->cat != F_EC_CONST)
			err("Regular expression must be a precompilable string");
		else
		{
			regex *r = rx_compile(expr->o.s, icase);
			if (!r)
				err("Malformed regular expression");
			expr->type = F_ET_REGEXP;
			struct filter_regex_value *rv = NEW(filter_regex_value);
			rv->source = expr->o.s;
			rv->regex = r;
			rv->icase = icase;
			rv->precompiled = 0;
			expr->o.i = filter_lookup_new(filter_current, F_LT_REGEX, rv);
		}
	}
	else
	{
		if (TYPE_MISMATCH(F_ET_STRING, expr->type))
			err("Using match operator on non-string expression");
	}
	return cond;
}

static struct filter_cond *
new_cond_partial_compare(int op, int neg, int icase, struct filter_expr *expr)
{
	struct filter_cond *cond = NEW(filter_cond);
	cond->cat = F_CC_EXPR;
	cond->undef = expr->undef;
	cond->o.expr.l = NULL;
	cond->o.expr.r = expr;
	cond->o.expr.neg = neg;
	cond->o.expr.icase = icase;
	cond->o.expr.op = op;
	if (expr->type == F_ET_REGEXP)
		err("Regexps can not be compared");
	return cond;
}

static struct filter_expr *
new_expr_unop(uns type, int op, struct filter_expr *r)
{
	struct filter_expr *expr = NEW(filter_expr);
	if (TYPE_MISMATCH(type, r->type))
		err("Type mismatch in unary operator");
	TYPE_COMBINE(r->type, type);
	expr->type = type;
	expr->cat = F_EC_UNOP;
	expr->undef = r->undef;
	expr->o.un.op = op;
	expr->o.un.r = r;
	return expr;
}

static struct filter_expr *
new_expr_binop(uns type, struct filter_expr *l, int op, struct filter_expr *r)
{
	struct filter_expr *expr = NEW(filter_expr);
	if (op != '.' && op != INTERVAL)
	{
		if (TYPE_MISMATCH(type, l->type) || TYPE_MISMATCH(type, r->type))
			err("Type mismatch in binary operator");
		TYPE_COMBINE(l->type, type);
		TYPE_COMBINE(r->type, type);
		expr->type = type;
	}
	else if (op == INTERVAL)
	{
		if (TYPE_MISMATCH(l->type, r->type))
			err("Type mismatch in interval");
		TYPE_COMBINE(l->type, r->type);
		TYPE_COMBINE(r->type, l->type);
		expr->type = l->type;
	}
	else		/* String operator '.' can be used on non-strings too.  */
	{
		expr->type = type;
	}
	expr->cat = F_EC_BINOP;
	expr->undef = l->undef || r->undef;
	if (op == '.')		/* Always defined */
		expr->undef = 0;
	expr->o.bin.op = op;
	expr->o.bin.l = l;
	expr->o.bin.r = r;
	return expr;
}

static struct filter_expr *
new_expr_func(struct filter *filt, byte *fname, int args, struct filter_expr *arg[MAX_FUNC_ARGS])
{
	struct filter_expr *expr = NEW(filter_expr);
	struct filter_function *f;
	int i;
	for (f = filter_builtin_func; f->name; f++)
		if (!strcasecmp(f->name, fname))
			break;
	if (!f->name && filt->func)
		for (f = filt->func; f->name; f++)
			if (!strcasecmp(f->name, fname))
				break;
	if (!f->name)
		err("Unknown function");
	expr->type = f->ret;
	expr->cat = F_EC_FUNC;
	expr->undef = 0;		/* the function must set the value of undef attribute */
	expr->o.func.func = f;
	if (args != f->args)
		err("Invalid number of parameters");
	for (i=0; i<args; i++)
	{
		if (!arg[i])
			ASSERT(0);
		if (TYPE_MISMATCH(f->arg[i], arg[i]->type))
			err("Type mismatch in function call");
		TYPE_COMBINE(arg[i]->type, f->arg[i]);
		expr->o.func.a[i] = arg[i];
	}
	return expr;
}

static struct filter_expr *
new_pattern_string(byte *string, int icase)
{
	struct filter_expr *e;
	regex *r;
	r = rx_compile(string, icase);
	if (!r)
		err("Malformed regular expression");
	e = NEW(filter_expr);
	e->type = F_ET_REGEXP;
	e->undef = 0;
	e->cat = F_EC_CONST;
	struct filter_regex_value *rv = NEW(filter_regex_value);
	rv->source = string;
	rv->regex = r;
	rv->icase = icase;
	rv->precompiled = 1;
	e->o.i = filter_lookup_new(filter_current, F_LT_REGEX, rv);
	return e;
}

static inline struct filter_variable *
search_variable(struct filter_variable *v, uns cat, byte *name)
{
	for (; v->name; v++)
		if (v->cat == cat && !strcasecmp(v->name, name))
			return v;
	return NULL;
}

static inline struct filter_variable *
search_variable_withcase(struct filter_variable *v, uns cat, byte *name)
{
	for (; v->name; v++)
		if (v->cat == cat && !strcmp(v->name, name))
			return v;
	return NULL;
}

static inline struct filter_binding *
search_binding(struct filter_binding *b, byte *name)
{
	for (; b->name; b++)
		if (!strcasecmp(b->name, name))
			return b;
	return NULL;
}

static struct filter_lvalue *
new_lvalue_raw(struct filter *filt, byte *name)
{
	struct filter_lvalue *lv = NEW(filter_lvalue);
	struct filter_variable *v;
	struct filter_binding *b;
	v = search_variable(filt->var, F_LVC_RAW, name);
	if (!v)
	{
		struct filter_declaration *decl;
		decl = search_declaration(filt->decl, name);
		if (!decl)
			err("Unknown raw variable");
		lv->cat = F_LVC_USER;
		lv->type = decl->var.type;
		lv->ro = decl->var.ro;
		lv->undef = 0;
		lv->v.decl = decl;
	}
	else
	{
		lv->cat = F_LVC_RAW;
		lv->type = v->type;
		lv->ro = v->ro;
		b = search_binding(filt->bind, name);
		if (b)
		{
			lv->undef = 0;
			lv->v.bind = b;
		}
		else
		{
			lv->undef = 1;
			lv->v.bind_name = name;
		}
	}
	return lv;
}

static struct filter_lvalue *
new_lvalue_attr(struct filter *filt, byte *name)
{
	struct filter_lvalue *lv = NEW(filter_lvalue);
	struct filter_variable *v;
	if (strlen(name) != 1)
		err("Additional attribute names must be exactly 1 character long");
	v = search_variable_withcase(filt->var, F_LVC_ATTR, name);
	if (!v)
	{
		/* All unknown additional attributes are assumed to be of
		 * string type.  */
		lv->type = F_ET_STRING;
		lv->ro = 0;
	}
	else
	{
		lv->type = v->type;
		lv->ro = v->ro;
	}
	lv->cat = F_LVC_ATTR;
	lv->undef = 0;		/* if NOT defined, will be set during the interpretation */
	lv->v.name = name[0];
	return lv;
}

static struct filter_lvalue *
new_lvalue_conf(struct filter *filt UNUSED, byte *sect, byte *name)
{
	struct filter_lvalue *lv = NEW(filter_lvalue);
	struct cf_item cfg;
	byte *full_name = filter_alloc(strlen(sect) + strlen(name) + 2);
	sprintf(full_name, "%s.%s", sect, name);
	byte *msg = cf_find_item(full_name, &cfg);
	sprintf(full_name, "%s.%s", sect, name);		// it's destroyed
	if (msg)
		err(stk_printf("Cannot find configuration item %s: %s", full_name, msg));
	/* TODO: If needed, extend the access to configuration items to support
	 * all features of conf2.  However, for the time being, this should be
	 * sufficient.  */
	if (!cfg.ptr)
	{
		lv->cat = F_LVC_CONF;
		lv->type = F_ET_UNKNOWN;
		lv->ro = 0;
		lv->undef = 1;
		lv->v.cfg = NULL;
		return lv;
	}
	if (cfg.cls != CC_STATIC || cfg.type != CT_INT && cfg.type != CT_STRING)
		err("Invalid configutation item type");
	lv->cat = F_LVC_CONF;
	lv->type = cfg.type == CT_INT ? F_ET_INT : F_ET_STRING;
	lv->ro = 0;
	lv->undef = 0;
	lv->v.cfg = filter_alloc(sizeof(struct filter_cf_item));
	lv->v.cfg->name = full_name;
	lv->v.cfg->ptr = cfg.ptr;
	return lv;
}

/* Add a list of new variables to the beginning of the global list.  */
static void
add_declarations(struct filter *filt, struct filter_declaration *fd)
{
	ASSERT(fd);
	fd->last->next = filt->decl;
	if (filt->decl)
		fd->last = filt->decl->last;
	filt->decl = fd;
}

/* Cut the beginning of the global list until (exclusively) the item UNTIL.
 * The items are moved into the global list of deleted items (to be able to
 * initialize them).  */
static void
remove_declarations(struct filter *filt, struct filter_declaration *until)
{
	struct filter_declaration *fd;
	if (filt->decl == until)
		return;
	ASSERT(filt->decl);
	for (fd=filt->decl; fd->next != until; fd=fd->next)
		fd->is_local = 1;
	fd->next = filt->deleted_decl;
	fd->is_local = 1;
	if (filt->deleted_decl)
		filt->decl->last = filt->deleted_decl->last;
	else
		filt->decl->last = fd;
	filt->deleted_decl = filt->decl;
	filt->decl = until;
}
