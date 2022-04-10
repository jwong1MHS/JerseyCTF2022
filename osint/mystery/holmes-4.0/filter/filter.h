/*
 *	Sherlock Filter Engine
 *
 *	(c) 1999--2000 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2007 Robert Spalek <robert@ucw.cz>
 */

#include "ucw/regex.h"
#include "ucw/mempool.h"
#include <string.h>
#include <setjmp.h>

struct fastbuf;

/*
 * Basic enumerations for type checking, switching the cases, ...
 */

enum filter_lvalue_cat { F_LVC_RAW, F_LVC_ATTR, F_LVC_CONF, F_LVC_USER};
enum filter_expr_type { F_ET_INT, F_ET_STRING, F_ET_REGEXP, F_ET_UNKNOWN};
enum filter_expr_cat { F_EC_CONST, F_EC_LVALUE, F_EC_UNOP, F_EC_BINOP, F_EC_FUNC};
enum filter_cond_cat { F_CC_CONST, F_CC_EXPR, F_CC_DEFCOND, F_CC_DEFEXPR, F_CC_UNOP, F_CC_BINOP};
enum filter_lookup_type { F_LT_REGEX, F_LT_TRIE };

/*
 * Values of the variables may be undefined by the context (not bound) or by
 * the program (undefined result of the operation).  To save the undefined flag
 * into target raw variables, special values are reserved.  This is used for
 * the I/O operations only.
 */

#define	F_UNDEF_INT	(int)0x80000000
#define	F_UNDEF_STRING	NULL
#define	F_UNDEF_REGEXP	NULL

/*
 * Internal data structures for filter parser and interpreter.
 */

struct filter_lvalue {
	enum filter_lvalue_cat cat;
	enum filter_expr_type type;
	byte ro;
	byte undef;
	union {
		struct filter_binding *bind;
		byte *bind_name;	/* when undef */
		uns name;
		struct filter_cf_item *cfg;
		struct filter_declaration *decl;
	} v;
};

struct filter_regex_value {
	byte *source;
	regex *regex;
	byte icase, precompiled;
};
union filter_raw_value2 {
	int i;
	uns u;
	byte *s;
};
union filter_raw_value {
	int i;
	uns u;
	byte *s;
	struct filter_regex_value *r;
	union filter_raw_value2 interval[2];
};

struct filter_value {
	enum filter_expr_type type;
	byte undef;
	union filter_raw_value v;		/* allocated in run pool */
};

#define	MAX_FUNC_ARGS	4

struct filter_args;
typedef void (*filter_function) (struct filter_args *args, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS]);

struct filter_expr {
	enum filter_expr_type type;
	enum filter_expr_cat cat;
	byte undef;
	union {
		int i;
		byte *s;
		union filter_raw_value2 raw;
		struct filter_lvalue *lv;
		struct {
			struct filter_expr *r;
			int op;
		} un;
		struct {
			struct filter_expr *l, *r;
			int op;
		} bin;
		struct {
			struct filter_expr *a[MAX_FUNC_ARGS];
			struct filter_function *func;
		} func;
	} o;
};

struct filter_cond {
	enum filter_cond_cat cat;
	byte undef;
	union {
		int i;
		struct {
			struct filter_expr *l, *r;	/* l == NULL means partial condition */
			byte neg;
			byte icase;
			int op;
		} expr;
		struct filter_expr *defexpr;
		struct filter_cond *neg;
		struct {
			struct filter_cond *l, *r;
			int op;
		} bin;
	} o;
};

struct filter_case {
	struct filter_case *next, *last;
	enum filter_expr_type type;
	byte undef;
	struct filter_cond *cond;			/* partial condition */
	struct filter_cmd *positive;
	uns case_id;
};

struct filter_hash_table;
struct filter_kmp_table;
struct filter_trie_table;
struct filter_s_tree;
struct filter_is_tree;
struct filter_ud_tree;
struct filter_d_tree;

struct filter_cmd {
	struct filter_cmd *next, *last;
	int op, pruned;
	union {
		struct {
			int level;
			struct filter_expr *expr;
		} print;
		struct {
			struct filter_lvalue *lv;
			struct filter_expr *expr;
		} set;
		struct {
			struct filter_cond *cond;
			struct filter_cmd *positive, *negative, *undefined;
		} cond;
		struct {
			struct filter_expr *expr;
			struct filter_case *cases;
			struct filter_cmd *negative, *undefined;
			struct filter_hash_table *cmp, *icmp;
			struct filter_kmp_table *kmp, *ikmp;
			int pat, ipat;
			union {
				struct {
					struct filter_s_tree *bins;
					struct filter_is_tree *binis;
				};
				struct {
					struct filter_ud_tree *binud;
					struct filter_d_tree *bind;
				};
			};
		} swit;
	} c;
};

/*
 * Link list of known names for lexical parser.  It doesn't touch directly the
 * variable declarations, because it doesn't know the context, it would be
 * confusing.
 *
 * Before a program is parsed, all variables must be declared.  Variable can be
 * of raw type (stored at the offset given in the binding), an additional
 * attribute (indexed by name) or a configuration item.  The syntactic parser
 * needs to know about types and access modes of all this variables.
 * filter_variable lists types and modes of type F_LVC_RAW and F_LVC_ATTR.
 * F_LVC_CONF items are exported automatically in read-write mode (journalling
 * is used to undo the changes).
 *
 * Binding of F_LVC_RAW variables only to real variables.
 *
 * Declaration of all functions.  Two sets are used: predefined and
 * user-defined.
 *
 * The list of known functions is also included (usually set to
 * filter_builtin_func).
 */

struct filter_lex_name {
	struct filter_lex_name *next;
	byte name[0];
};

struct filter_variable {
	byte *name;
	enum filter_lvalue_cat cat;
	enum filter_expr_type type;
	byte ro;
};

struct filter_cf_item {
	byte *name;
	void *ptr;
};

struct filter_binding {				/* for cat == F_LVC_RAW only */
	byte *name;
	int offset;
};

struct filter_function {
	byte *name;
	int args;
	enum filter_expr_type arg[MAX_FUNC_ARGS], ret;
	filter_function f;
};

struct filter_declaration {
	struct filter_declaration *next, *last;
	int nr;
	byte is_local;
	struct filter_variable var;
};

struct filter_lookup {
	enum filter_lookup_type type;
	union {
		void *value;
		struct filter_regex_value *regex;
		struct filter_trie_table *trie;
	};
};

struct filter {
	struct mempool *pool;
	struct filter_lex_name *name_head;
	struct filter_variable *var;
	struct filter_binding *bind;
	struct filter_function *func;
	int user_vars;			/* the number of user-defined variables */
	struct filter_declaration *decl, *deleted_decl;
	struct filter_cmd *body;
	uns lookup_count, lookup_limit;
	struct filter_lookup *lookup;
};

/*
 * When running the filter, pool for concatenation '.' operator, pointer to the
 * raw variable structure and pointer to the argument list must be set here.
 *
 * If you want to let the filter modify your program configuration, you have
 * to set config_changes_mode and call filter_intr_undo() appropriately to
 * restrict the scope of the config changes.
 */

struct filter_args {
	struct filter *filter;
	union filter_raw_value *user_var;
	struct mempool *pool;			/* for '.' operator and switches */
	byte *msg;				/* return message or NULL */
	void *raw;
	struct odes *attr;
	int config_changes_mode;		/* 0 - disabled, 1 - leave changes, 2 - reset changes automatically */
	struct cf_journal_item *oldj;		/* journalling of cf_item changes */
	struct mempool *saved_pool;		/* saved cf_pool for want_config_changes disabled */
};

/* parse.y */

#define	TYPE_MISMATCH(x, y)	((x)!=(y) && (x)!=F_ET_UNKNOWN && (y)!=F_ET_UNKNOWN)
#define	TYPE_COMBINE(x, y)	if (x == F_ET_UNKNOWN) x = y

void *filter_alloc(int size);
byte *filter_strdup(byte *str);

int yyparse(void *);

/* lex.l */

extern jmp_buf filter_jmp_buf;
extern void (*filter_err_hook)(char *err);

void filter_err(char *err) NONRET;
void filter_lex_init(char *name);
void filter_lex_init_fb(struct fastbuf *fb);
void filter_lex_cleanup(void);
int yylex(void);

/* filter.c */

extern struct filter *filter_current;

struct filter *filter_load(char *name, struct filter_variable *var, struct filter_binding *bind, struct filter_function *func);
struct filter *filter_load_fb(struct fastbuf *fb, struct filter_variable *var, struct filter_binding *bind, struct filter_function *func);
struct filter *filter_clone(struct filter *);
void filter_delete(struct filter *);
int filter_lookup_new(struct filter *filter, enum filter_lookup_type type, void *value);
int filter_lookup_new_no_null(struct filter *filter, enum filter_lookup_type type, void *value);

struct filter_args *filter_intr_new(struct filter *);
void filter_intr_delete(struct filter_args *);
int filter_intr_run(struct filter_args *);
void filter_intr_undo_init(struct filter_args *);
void filter_intr_undo(struct filter_args *);

struct filter_cases {
  uns count, size;
  struct filter_args *args;
  struct filter_case **list;
};

void filter_cases_grow(struct filter_cases *r);

static inline void
filter_cases_add(struct filter_cases *r, struct filter_case *cas)
{
  if (unlikely(r->count == r->size))
	filter_cases_grow(r);
  r->list[r->count++] = cas;
}

/* Exported for pruning: */
void filter_eval_expr(struct filter_value *dest, struct filter_args *args, struct filter_expr *expr);
int filter_eval_cond(struct filter_args *args, struct filter_cond *cond, struct filter_expr *partial_expr);

/* prune.c */

void filter_prune(struct filter *);

/* builtin.c */

extern struct filter_function filter_builtin_func[];
extern struct filter_variable filter_builtin_vars[];

/* hashes.c */

struct filter_hash_record {				/* for fast SWITCH testing */
	struct filter_hash_record *next;
	byte *string;
	struct filter_case *cas;
};

struct filter_hash_table
{
	uns size, count;
	uns icase;
	struct mempool *mp;
	struct filter_hash_record *h[0];
};

struct filter_hash_table *filter_ht_new(struct mempool *mp, uns count, uns icase);
void filter_ht_add(struct filter_hash_table *ht, byte *string, struct filter_case *cas);
void filter_ht_find(struct filter_hash_table *ht, byte *string, struct filter_cases *res);

/* kmp.c */

struct filter_kmp_record {
	struct filter_kmp_record *next;
	struct filter_case *cas;
};

struct filter_kmp_table {
	uns icase;
	struct mempool *mp;
	void *kmp;
	struct filter_case *cases;
};

struct filter_kmp_table *filter_kmp_new(struct mempool *mp, uns icase);
void filter_kmp_add(struct filter_kmp_table *kmp, byte *string, struct filter_case *cas);
void filter_kmp_build(struct filter_kmp_table *kmp);
void filter_kmp_find(struct filter_kmp_table *kmp, byte *string, struct filter_cases *res);

/* tries.c */

struct filter_trie_record
{
	uns bitnr;				/* which bit is being tested */
	struct filter_trie_record *son[2];	/* tree on its value 0 or 1 */
	int *cmdids;				/* -1 terminated list of command ID's... */
	char prefix[0];				/* ...with exactly this prefix */
};

#include "ucw/clists.h"
struct filter_trie_result
{
	cnode n;
	uns number;
	uns trivial_pattern;			/* 1 iff the pattern contains only one * and no ? */
	byte passed;				/* number of tests the wildcard-pattern has matched */
	byte init_passed;			/* its initial value */
};

struct filter_trie_table
{
	uns icase;
	uns cmds;				/* table of commands by their ID */
	struct filter_case **cmd;
	int *cmdid;				/* table of lists of command ID's */
	struct filter_trie_result *results;	/* table of results of pattern matchings */
	clist tests, incremented_tests, passed_tests;
	struct filter_trie_record *root[2];	/* 0=prefix, 1=suffix */
};

struct filter_trie_table *filter_trie_new(struct mempool *mp, struct filter_cmd *case_cmd, uns icase);
void filter_trie_search(struct filter_trie_table *trie, byte *string, struct filter_cases *res);
void filter_trie_dump(struct fastbuf *b, struct filter *f, struct filter_trie_table *trie);

/* trees.c */

struct filter_s_tree *filter_s_tree_new(struct mempool *mp, struct filter_cmd *case_cmd);
struct filter_is_tree *filter_is_tree_new(struct mempool *mp, struct filter_cmd *case_cmd);
struct filter_ud_tree *filter_ud_tree_new(struct mempool *mp, struct filter_cmd *case_cmd);
struct filter_d_tree *filter_d_tree_new(struct mempool *mp, struct filter_cmd *case_cmd);
void filter_s_tree_search(struct filter_s_tree *tree, byte *val, struct filter_cases *res);
void filter_is_tree_search(struct filter_is_tree *tree, byte *val, struct filter_cases *res);
void filter_ud_tree_search(struct filter_ud_tree *tree, uns val, struct filter_cases *res);
void filter_d_tree_search(struct filter_d_tree *tree, int val, struct filter_cases *res);
void filter_s_tree_dump(struct fastbuf *fb, struct filter *f, struct filter_s_tree *tree, uns level);
void filter_is_tree_dump(struct fastbuf *fb, struct filter *f, struct filter_is_tree *tree, uns level);
void filter_ud_tree_dump(struct fastbuf *fb, struct filter *f, struct filter_ud_tree *tree, uns level);
void filter_d_tree_dump(struct fastbuf *fb, struct filter *f, struct filter_d_tree *tree, uns level);

/* dumper.c */

void filter_dump_spaces(struct fastbuf *b, uns level);
void filter_dump_condition(struct fastbuf *b, struct filter *f, struct filter_cond *c);
void filter_dump_commands(struct fastbuf *b, struct filter *f, struct filter_cmd *cmd, uns level);
void filter_dump(struct fastbuf *b, struct filter *f);

/* fconfig.c */

extern uns filter_trace, filter_hash_limit, filter_kmp_limit, filter_trie_limit, filter_tree_limit, filter_optimize;
extern char *filter_dump_to;
