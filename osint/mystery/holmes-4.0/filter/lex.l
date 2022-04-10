/*
 *	Sherlock Filter Engine -- Lexer
 *
 *	(c) 1999--2000 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2007 Robert Spalek <robert@ucw.cz>
 */

%{
#undef	REJECT			/* internal unused flex macro */

#include "ucw/lib.h"
#include "ucw/fastbuf.h"
#include "ucw/string.h"
#include "filter/filter.h"
#include "filter/parse.tab.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <fcntl.h>

#define err(x) filter_err(x)

#define	YY_NO_UNPUT
#define YY_DECL	int yylex(void)
#define YY_INPUT(buf,result,max_size) do { result = (int)bread(filter_fb, buf, max_size) ? : YY_NULL; } while (0)

#define KW(x) { #x, x }
static struct keyword {
	byte *name;
	int value;
} keyword_list[] =
{
	KW(ACCEPT),	/* commands */
	KW(REJECT),
	{ "ERROR", ERROR1 },
	{ "WARNING", WARNING1 },
	{ "LOG", LOG1 },
	{ "DEBUG", DEBUG1 },

	KW(IF),		/* statements */
	KW(ELSE),
	KW(UNDEF),
	KW(ELIF),
	KW(SWITCH),
	KW(CASE),

	KW(TRUE),	/* expressions */
	KW(FALSE),
	KW(UNDEFINED),
	KW(DEFINED),
	KW(ATTR),
	KW(ADD),
	KW(DELETE),
	KW(CONF),
	KW(INT),
	KW(STRING),
	KW(REGEXP),

	{ NULL, -1 }
};
#undef KW

#define	MAX_INCLUDE_DEPTH	16
struct file_context {
	YY_BUFFER_STATE state;
	struct fastbuf *fb;
	int lino;
};
static struct file_context filter_file[MAX_INCLUDE_DEPTH];
static int filter_file_nr;
static struct fastbuf *filter_fb;

static void read_file(char *name, YY_BUFFER_STATE state);
static void close_context(void);

%}

%option noyywrap

%x COMMENT CCOMM INCLUDE

ALPHA [a-zA-Z_]
DIGIT [0-9]
XIGIT [0-9a-fA-F]
ALNUM [a-zA-Z_0-9]
WHITE [ \t]

%%

include	BEGIN(INCLUDE);

<INCLUDE>{WHITE}*
<INCLUDE>[\n] {
	err("Filename expected");
}
<INCLUDE>[^ \t\n]* {
	read_file(yytext, YY_CURRENT_BUFFER);
	BEGIN(INITIAL);
}

0x{XIGIT}+ {
	char *e;
	unsigned long int l;
	errno = 0;
	l = strtoul(yytext+2, &e, 16);
	if (e && *e || errno == ERANGE || (unsigned long int)(uns) l != l)
		err("Number out of range");
	yylval.i = l;
	return NUM;
}

0{DIGIT}+ {
	char *e;
	long int l;
	errno = 0;
	l = strtoul(yytext+1, &e, 8);
	if (e && *e || errno == ERANGE || (long int)(int) l != l)
		err("Number out of range");
	yylval.i = l;
	return NUM;
}

{DIGIT}+ {
	char *e;
	long int l;
	errno = 0;
	l = strtoul(yytext, &e, 10);
	if (e && *e || errno == ERANGE || (long int)(int) l != l)
		err("Number out of range");
	yylval.i = l;
	return NUM;
}

{ALPHA}{ALNUM}* {
	struct keyword *k;
	struct filter_lex_name *name;
	for (k = keyword_list; k->name; k++)
		if (!strcasecmp(k->name, yytext))
			return k->value;
	for (name = filter_current->name_head; name->next; name = name->next)
		if (!strcasecmp(name->next->name, yytext))
		{
			yylval.s = name->next->name;
			return IDENT;
		}
	name->next = filter_alloc(sizeof(struct filter_lex_name) + yyleng+1);
	name = name->next;
	name->next = NULL;
	strncpy(name->name, yytext, yyleng);
	name->name[yyleng] = 0;
	yylval.s = name->name;
	return IDENT;
}

"<"	return LT;
">"	return GT;
"<="	return LE;
">="	return GE;
"=="	return EQ;
"!="	return NE;
"=~"	return EREG;
"!~"	return NREG;
"=*"	return EPAT;
"!*"	return NPAT;
"=#"	return EIN;
"!#"	return NIN;

"<<"	return LTC;
">>"	return GTC;
"<=="	return LEC;
">=="	return GEC;
"==="	return EQC;
"!=="	return NEC;
"=~~"	return EREGC;
"!~~"	return NREGC;
"=**"	return EPATC;
"!**"	return NPATC;
"=##"	return EINC;
"!##"	return NINC;

"&&"	return AND;
"||"	return OR;

"~"	return PATTERN;
"~~"	return IPATTERN;

".."	return INTERVAL;

[.,:;(){}\[\]+\-*/%^&|!=<>] {
	return yytext[0];
}

['][^'\n]*['] {
	yytext[yyleng-1] = 0;
	yylval.s = filter_strdup(yytext+1);
	return STRING;
}

["]([^"\n\\]|\\[^\n])*["] {
	yytext[yyleng-1] = 0;
	str_unesc(yytext+1, yytext+1);
	yylval.s = filter_strdup(yytext+1);
	return STRING;
}

["][^"\n]*\n	err("Unterminated string");

<INITIAL,COMMENT><<EOF>> {
	close_context();
	if (!filter_file_nr)
		return END;
}

{WHITE}+

\n	filter_file[filter_file_nr-1].lino++;

#	BEGIN(COMMENT);

\/\*	BEGIN(CCOMM);

.	err("Unknown character");

<COMMENT>\n {
	filter_file[filter_file_nr-1].lino++;
	BEGIN(INITIAL);
}

<COMMENT>.

<CCOMM>\*\/	BEGIN(INITIAL);
<CCOMM>\n	filter_file[filter_file_nr-1].lino++;
<CCOMM>\/\*	err("Comment nesting not supported");
<CCOMM><<EOF>>	err("Unterminated comment");
<CCOMM>.

%%

static void
read_fb(struct fastbuf *fb, YY_BUFFER_STATE state)
{
	if (filter_file_nr >= MAX_INCLUDE_DEPTH)
	{
		bclose(fb);
		err("Too many nested files");
	}
	filter_file[filter_file_nr].state = state;
	filter_file[filter_file_nr].fb = fb;
	filter_file[filter_file_nr].lino = 1;
	filter_file_nr++;
	yy_switch_to_buffer( yy_create_buffer(NULL, YY_BUF_SIZE) );
	filter_fb = fb;
}

static void
read_file(char *name, YY_BUFFER_STATE state)
{
	struct fastbuf *fb = bopen_try(name, O_RDONLY, 4096);
	if (!fb)
		err("Unable to open file");
	read_fb(fb, state);
}

static void
close_context(void)
{
	yy_delete_buffer( YY_CURRENT_BUFFER );
	filter_file_nr--;
	bclose(filter_file[filter_file_nr].fb);
	if (filter_file_nr > 0)
	{
		filter_fb = filter_file[filter_file_nr - 1].fb;
		yy_switch_to_buffer( filter_file[filter_file_nr].state );
	}
}

void
filter_lex_init(char *name)
{
	filter_file_nr = 0;
	read_file(name, NULL);
}

void
filter_lex_init_fb(struct fastbuf *fb)
{
	filter_file_nr = 0;
	read_fb(fb, NULL);
}

void
filter_lex_cleanup(void)
{
}

void
filter_err(char *err)
{
	if (filter_err_hook)
		filter_err_hook(err);
	else
	{
		for (int i=0; i<filter_file_nr; i++)
			msg(L_INFO, "#%d %s, line %d", i+1, filter_file[i].fb->name, filter_file[i].lino);
		die("%s", err);
	}
	while (filter_file_nr)
		close_context();
	longjmp(filter_jmp_buf, 1);
}
