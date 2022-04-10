/*
 *	Sherlock Search Engine -- Query Parser
 *
 *	(c) 1997--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

%{
#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "search/sherlockd.h"
#include "indexer/sites.h"
#include "indexer/params.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <setjmp.h>

#define yyerror(x) err(x)

static byte *error;
static int yyparse(void);
static void parse_ext(struct query *q, uns id, enum custom_op op, byte *str, uns num, slist *set);

#define CREATE(type) (type *)mp_alloc_zero(current_query->pool, sizeof(type))

struct expr *new_node(enum expr_type t)
{
  struct expr *n = CREATE(struct expr);
  n->type = t;
  return n;
}

struct expr *new_op(enum expr_type t, struct expr *l, struct expr *r)
{
  struct expr *n = CREATE(struct expr);
  n->type = t;
  n->u.op.l = l;
  n->u.op.r = r;
  return n;
}

static void
init_options(struct options *o)
{
  o->weight = WEIGHT_DEFAULT;
  o->accent_mode = OPT_DEFAULT;
  o->morphing = OPT_DEFAULT;
  o->spelling = OPT_DEFAULT;
  o->synonyming = OPT_DEFAULT;
  o->syn_expand = ~0ULL;
  o->default_word_types = ~0U;
}

static struct options *
new_options(void)
{
  struct options *o = CREATE(struct options);
  init_options(o);
  return o;
}

static snode *
new_set_node(uns min, uns max, byte *text)
{
  struct set_node *n = CREATE(struct set_node);
  n->min = min;
  n->max = max;
  n->text = text;
  return &n->n;
}

%}

%union {
  uns i;
  u64 L;
  struct query *q;
  struct expr *x;
  slist *l;
  snode *n;
  struct options *o;
  byte *s;
  struct database *d;
}

/* Tokens */

%token EOLN
%token SHOW LIST STATS DUMP CONTROL
%token K_DEBUG ACCENTS CONTEXT METALEN INTERVALS DB SITE URLS SITEMAX EXPLAIN FULL TYPES
%token PARTIAL APPROX ANY DOTDOT AUTONEAR LE GE NE SORTBY ONLY CARDID SPELL MORPH SYN SYNEXP
%token IMAGESIM SIG
%token <i> TYPE NUM CUSTOM
%token <s> STRING
%token <L> ID

%left OR
%left AND
%left NOT MAYBE

%type <x> expr atom radical simple molecule neg_molecule
%type <i> type_mask type_mask_list custom_op ID32 maybe_ID32
%type <i> maybe_minus maybe_only maybe_db_no custom_attr
%type <l> maybe_set set show_list db_list dump_list
%type <n> set_node db_node dump_node
%type <q> global querystart selector
%type <o> maybe_options options option
%type <d> imagesim_db

%%

main:
   global expr EOLN {
      $1->expr = $2;
      return 0;
   }
 | global CONTROL STRING {
      $1->cmd = $3;
      return 0;
   }
 ;

querystart:
   /* empty */ { $$ = current_query; }
 ;

selector:
   querystart {
      $1->out_mode = OUT_SHOW;
      $1->out_range = CREATE(slist);
      slist_add_tail($1->out_range, new_set_node(1, MIN(max_matches, max_output_matches), NULL));
      $$ = $1;
   }
 | querystart LIST show_list {
      $1->out_mode = OUT_LIST;
      $1->out_range = $3;
      $$ = $1;
   }
 | querystart SHOW show_list {
      $1->out_mode = OUT_SHOW;
      $1->out_range = $3;
      $$ = $1;
   }
 | querystart STATS {
      $1->out_mode = OUT_STATS;
      $$ = $1;
   }
 | querystart DUMP dump_list {
      $1->out_mode = OUT_DUMP;
      $1->out_range = $3;
      $$ = $1;
   }
 ;

global:
   selector
 | global K_DEBUG NUM {
      $1->debug = $3;
      $$ = $1;
   }
 | global CONTEXT NUM {
      $1->context_chars = $3;
      $$ = $1;
   }
 | global CONTEXT FULL {
      $1->context_chars = CONTEXT_FULL;
      $$ = $1;
   }
 | global METALEN '{' type_mask '}' NUM {
      for (uns i=16; i<31; i++)
	if ($4 & (1 << i))
	  $1->meta_chars[i-16] = $6;
      $$ = $1;
   }
 | global INTERVALS NUM {
      if ($3 > NUM_BESTS)
        err("INTERVALS %d out of range", $3);
      $1->intervals = $3;
      $$ = $1;
   }
 | global option {
      merge_options(&$1->default_options, &$1->default_options, $2);
      $$ = $1;
   }
 | global DB db_list {
      $1->db_list = $3;
      $1->db_mask = 0;
      SLIST_FOR_EACH(struct db_node *, n, *$3)
	if ($1->db_mask & (1 << n->id))
	  err("Duplicate database <%s>", n->db->name);
        else
	  $1->db_mask |= (1 << n->id);
      $$ = $1;
   }
 | global SITE ID {
      if (SITE_HASH_SIZE != 8 && $3 >= (1ULL << 8*SITE_HASH_SIZE))
	err("Site hash too long");
      $1->site_hash = $3;
      $$ = $1;
   }
 | global SITE STRING {
      $1->site_hash = site_name2hash_u64($3);
      $$ = $1;
   }
 | global SITEMAX NUM {
      if ($3 > 2)
	err("SITEMAX %d out of range", $3);
      $1->site_max = $3;
      $$ = $1;
   }
 | global URLS NUM {
      $1->url_max = $3;
      $$ = $1;
   }
 | global PARTIAL NUM {
      $1->partial_answers = $3;
      $$ = $1;
   }
 | global APPROX NUM {
      $1->allow_approx = $3;
      $$ = $1;
   }
 | global CUSTOM custom_op NUM {
      parse_ext($1, $2, $3, NULL, $4, NULL);
      $$ = $1;
   }
 | global CUSTOM custom_op STRING {
      parse_ext($1, $2, $3, $4, 0, NULL);
      $$ = $1;
   }
 | global CUSTOM custom_op '{' set '}' {
      parse_ext($1, $2, $3, NULL, 0, $5);
      $$ = $1;
   }
 | global SORTBY maybe_minus custom_attr maybe_only {
      $1->custom_sorting = $4;
      $1->custom_sort_reverse = ($3 ? ~0U : 0);
      $1->custom_sort_only = $5;
      $$ = $1;
   }
 | global EXPLAIN ID32 {
      $1->explain_id = $3;
      $$ = $1;
   }
 ;

ID32:
   ID {
      if ($1 >= (1LL << 32))
	err("#ID too large");
      $$ = $1;
   }
 ;

maybe_ID32:
   ID32 { $$ = $1; }
 | /* empty */ { $$ = 0; }
 ;

custom_op:
   '<' { $$ = CUSTOM_OP_LT; }
 | '>' { $$ = CUSTOM_OP_GT; }
 | LE  { $$ = CUSTOM_OP_LE; }
 | GE  { $$ = CUSTOM_OP_GE; }
 | '=' { $$ = CUSTOM_OP_EQ; }
 | NE  { $$ = CUSTOM_OP_NE; }
 ;

custom_attr:
   CUSTOM
 | SITE { $$ = PARAM_SITE; }
 | CARDID { $$ = PARAM_CARDID; }
 ;

maybe_minus:
   '-' { $$ = 1; }
 | /* empty */ { $$ = 0; }
 ;

maybe_only:
   ONLY { $$ = 1; }
 | /* empty */ { $$ = 0; }
 ;

expr:
   molecule
 | expr AND expr {
     $$ = new_op(EX_AND, $1, $3);
   }
 | expr OR expr {
     $$ = new_op(EX_OR, $1, $3);
   }
 ;

molecule:
   neg_molecule
 | simple
 ;

neg_molecule:
   '(' expr ')' maybe_options {
     if ($4)
       {
	 $$ = new_node(EX_OPTIONS);
	 memcpy(&$$->u.options.o, $4, sizeof(struct options));
	 $$->u.options.inside = $2;
       }
     else
       $$ = $2;
   }
 | ANY { $$ = new_node(EX_ANY); }
 | NOT neg_molecule {
     $$ = new_op(EX_NOT, $2, NULL);
   }
 | IMAGESIM STRING {
     /* IMAGESIM token currently does not understand any options */
     $$ = image_sim_new_url($2);
   }
 | IMAGESIM CARDID imagesim_db ID32 {
     $$ = image_sim_new_card_id($3 ? : (struct database *)clist_head(&databases), $4);
   }
 | IMAGESIM SIG STRING {
     $$ = image_sim_new_sig($3);
   }
 ;

imagesim_db:
   /* empty */ { $$ = NULL; }
 | STRING ':' {
     struct database *db;
     for (db=clist_head(&databases); db; db=clist_next(&databases, &db->node))
       if (!strcmp(db->name, $1))
         break;
     if (!db)
       err("-120 Unknown database <%s>", $1);
     $$ = db;
   }
 ;

atom:
   radical       { $$=$1; $$->u.match.sense=1; }
 | NOT radical   { $$=$2; $$->u.match.sense=-1; }
 | MAYBE radical { $$=$2; $$->u.match.sense=0; }
 ;

radical:
   type_mask STRING maybe_options {
     $$ = new_node(EX_MATCH);
     $$->u.match.type_mask = $1;
     $$->u.match.word = $2;
     if ($3)
       memcpy(&$$->u.match.o, $3, sizeof(struct options));
     else
       init_options(&$$->u.match.o);
     $$->u.match.next_simple = NULL;
   }
 ;

simple:
   atom
 | atom '.' simple { $$=$1; $1->u.match.next_simple = $3; }
 ;

type_mask:
   /* empty */ { $$ = ~0U; }
 | type_mask_list
 ;

type_mask_list:
   TYPE { $$ = $1; }
 | type_mask_list ',' TYPE {
     if (($1 ^ $3) & 0x8000)
       err("Incompatible word types");
     $$ = $1 | $3;
   }
 ;

maybe_options:
   /* empty */ { $$ = NULL; }
 | options { $$ = $1; }
 ;

options:
   option { $$ = $1; }
 | options option { merge_options($1, $2, $1); $$ = $1; }
 ;

option:
   '/' maybe_minus NUM {
     if ($3 > 30000)
       err("Word weight %d out of range", $3);
     $$ = new_options();
     $$->weight = $2 ? -$3 : $3;
   }
 | ACCENTS NUM {
     if ($2 > 3)
       err("ACCENTS %d out of range", $2);
     $$ = new_options();
     $$->accent_mode = $2;
   }
 | MORPH NUM {
     $$ = new_options();
     $$->morphing = $2;
   }
 | SPELL NUM {
     $$ = new_options();
     $$->spelling = $2;
   }
 | SYN NUM {
     $$ = new_options();
     $$->synonyming = $2;
   }
 | SYNEXP '{' maybe_set '}' {
     $$ = new_options();
     $$->syn_expand = 0;
     if ($3)
       SLIST_FOR_EACH(struct set_node *, t, *$3)
         {
	   if (t->text)
	     err("SYNEXP: integer set expected");
	   if (t->max > 62)
	     err("SYNEXP: variant number %d out of range", t->max);
	   for (uns i=t->min; i<=t->max; i++)
	     $$->syn_expand |= 1ULL << i;
	 }
   }
 | TYPES '{' type_mask '}' {
     $$ = new_options();
     $$->default_word_types = $3;
   }
 ;

show_list:
   set {
     SLIST_FOR_EACH(struct set_node *, t, *$1)
       if (t->text)
	 err("Integer set expected");
     $$ = $1;
   }
 ;

maybe_set:
   /* empty */ { $$ = NULL; }
 | set
 ;

set:
   set_node { $$ = CREATE(slist); slist_add_tail($$, $1); }
 | set ',' set_node { $$ = $1; slist_add_tail($$, $3); }
 ;

set_node:
   NUM { $$ = new_set_node($1, $1, NULL); }
 | STRING { $$ = new_set_node(0, 0, $1); }
 | NUM DOTDOT NUM {
     if ($1 > $3)
       err("Invalid interval");
     $$ = new_set_node($1, $3, NULL);
   }
 ;

db_list:
   db_node { $$ = CREATE(slist); slist_add_tail($$, $1); }
 | db_list ',' db_node { $$ = $1; slist_add_tail($$, $3); }
 ;

db_node:
   STRING maybe_ID32 {
     struct database *db;
     struct db_node *n = CREATE(struct db_node);
     int i;
     for (db=clist_head(&databases), i=0; db; db=clist_next(&databases, &db->node), i++)
       if (!strcmp(db->name, $1))
         break;
     if (!db)
       err("-120 Unknown database <%s>", $1);
     n->db = db;
     n->id = i;
     if ($2)
       {
	 if (!db->params || db->params->ref_time != (ucw_time_t) $2)
	   err("-121 Unknown version <%x> of database <%s>", $2, $1);
       }
     $$ = &n->n;
   }
 ;

dump_list:
   dump_node { $$ = CREATE(slist); slist_add_tail($$, $1); }
 | dump_list ',' dump_node { $$ = $1; slist_add_tail($$, $3); }
 ;

dump_node:
   maybe_db_no ID32 {
     struct dump_node *n = CREATE(struct dump_node);
     n->db_id = $1;
     n->id = $2;
     $$ = &n->n;
   }
 ;

maybe_db_no:
   NUM ':' { $$ = $1; }
 | /* empty */ { $$ = 0; }
 ;

%%

static jmp_buf err_jmp;

void
err(char *x, ...)
{
  va_list args;
  va_start(args, x);
  error = mp_vprintf(current_query->pool, x, args);
  va_end(args);
  longjmp(err_jmp, 1);
}

byte *
parse_query(byte *b)
{
  error = NULL;
  lex_init(b);
  if (!setjmp(err_jmp))
    yyparse();
  return error;
}

#ifdef CONFIG_LASTMOD
static char *
parse_age(u32 *dest, char *val, uns intval)
{
  if (val)
    return "Incorrect AGE";
  *dest = intval;
  return NULL;
}
#else
static char *parse_age(u32 *dest UNUSED, char *val UNUSED, uns intval UNUSED)
{
  return "Searching by document age not supported in this configuration";
}
#endif

static void
parse_ext(struct query *q, uns id, enum custom_op op, byte *str, uns num, slist *set)
{
  char *(*pfunc)(u32 *dest, char *value, uns intval) = NULL;
  char *(*cfunc)(struct query *q, enum custom_op op, char *value, uns intval) = NULL;
  byte *msg;
  u32 val;
  u32 min = 0, max = ~0U, empty = 0;
  u32 *pmin = NULL, *pmax = NULL, *pset = NULL;

  switch (id)
    {
    case PARAM_AGE:
      pfunc = parse_age;
      pmin = &q->age_raw_min;
      pmax = &q->age_raw_max;
      break;
#define INT_ATTR(id,keywd,gf,pf)		\
    case OFFSETOF(struct query, id##_min):	\
      pfunc = pf;				\
      pmin = &q->id##_min;			\
      pmax = &q->id##_max;			\
      break;
#define SMALL_SET_ATTR(id,keywd,gf,pf) 		\
    case OFFSETOF(struct query, id##_set):	\
      pfunc = pf;				\
      pset = &q->id##_set;			\
      break;
#define LATE_INT_ATTR INT_ATTR
#define LATE_SMALL_SET_ATTR SMALL_SET_ATTR
    EXTENDED_ATTRS
#undef INT_ATTR
#undef SMALL_SET_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR

#define CUSTOM_MATCH_KWD(id,keywd,cf)		\
    case OFFSETOF(struct query, id##_placeholder): \
      cfunc = cf;				\
      break;
    CUSTOM_MATCH_PARSE
#undef CUSTOM_MATCH_KWD

    default:
      die("parse_ext: unknown attribute");
    }

  if (set)
    {
      if (!pset)
	err("Set matching not supported for this attribute");
      if (op != CUSTOM_OP_EQ && op != CUSTOM_OP_NE)
	err("Sets are not ordered");
      *pset = 0;
      SLIST_FOR_EACH(struct set_node *, n, *set)
	{
	  if (n->text)
	    {
	      u32 val;
	      if (msg = pfunc(&val, n->text, 0))
		err(msg);
	      *pset |= 1 << val;
	    }
	  else
	    {
	      u32 min, max;
	      if ((msg = pfunc(&min, NULL, n->min)) || (msg = pfunc(&max, NULL, n->max)))
		err(msg);
	      while (min <= max)
		*pset |= 1 << min++;
	    }
	}
      if (op == CUSTOM_OP_NE)
        *pset = ~*pset;
      return;
    }

  if (cfunc)
    {
      if (msg = cfunc(q, op, str, num))
	err(msg);
      return;
    }

  if (msg = pfunc(&val, str, num))
    err(msg);

  switch (op)
    {
    case CUSTOM_OP_LT:
      /* Unfortunately, we must handle <0 separately as we don't use signed integers */
      if (val) max=val-1; else empty=1;
      break;
    case CUSTOM_OP_GT:
      if (val == ~0U) empty=1; else min=val+1;
      break;
    case CUSTOM_OP_LE:
      max=val;
      break;
    case CUSTOM_OP_GE:
      min=val;
      break;
    case CUSTOM_OP_EQ:
      min=max=val;
      break;
    case CUSTOM_OP_NE:			/* <> supported only for set attributes */
      if (!pset)
	err("<> supported only for set-matched attributes");
      *pset &= ~(1 << val);
      return;
    default:
      ASSERT(0);
    }
  if (pset)
    {
      u32 mask = 0;
      while (min <= MIN(max, 31))
	mask |= 1 << min++;
      *pset &= mask;
    }
  else
    {
      *pmin = MAX(*pmin, min);
      *pmax = MIN(*pmax, max);
      if (*pmin > *pmax || empty)
	{
	  *pmin = ~0U;
	  *pmax = 0;
	}
    }
}

/* Extended attributes */

#ifdef CONFIG_LANG

#include "lang/lang.h"

char *
ext_lang_parse(u32 *dest, char *value, uns intval)
{
  if (value)
    {
      int c = lang_name_to_code(value);
      if (c < 0)
	return "LANG: language name not recognized";
      *dest = c;
    }
  else
    {
      if (intval > 31)
	return "LANG: out of range";
      *dest = intval;
    }
  return NULL;
}

#endif

#ifdef CONFIG_FILETYPE

char *
ext_ft_parse(u32 *dest, char *value, uns intval)
{
  if (value)
    {
      for (uns i=0; i<MAX_FILE_TYPES; i++)
	if (!strcmp(value, custom_file_type_names[i]))
	  {
	    *dest = i;
	    return NULL;
	  }
      return "FILETYPE: unknown type";
    }
  if (intval >= MAX_FILE_TYPES)
    return "FILETYPE: out of range";
  *dest = intval;
  return NULL;
}

#endif

#ifdef CONFIG_AREAS

char *
ext_area_parse(u32 *dest, char *value, uns intval)
{
  if (value)
    return "AREA: integer expected";
  if (intval > 0x7fffffff)
    return "AREA: out of range";
  *dest = intval;
  return NULL;
}

#endif

void
merge_options(struct options *dest, struct options *old, struct options *new)
{
  dest->weight = (new->weight == WEIGHT_DEFAULT) ? old->weight : new->weight;
#define MERGE(a) dest->a = (new->a < 0) ? old->a : new->a;
  MERGE(accent_mode);
  MERGE(morphing);
  MERGE(spelling);
  MERGE(synonyming);
#undef MERGE
  dest->syn_expand = (new->syn_expand == ~0ULL) ? old->syn_expand : new->syn_expand;
  dest->default_word_types = (new->default_word_types == ~0U) ? old->default_word_types : new->default_word_types;
}
