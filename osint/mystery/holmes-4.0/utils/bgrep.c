/*
 *	Sherlock Utilities -- Grep On Buckets
 *
 *	(c) 2005 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/bucket.h"
#include "ucw/fastbuf.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/bitarray.h"
#include "ucw/clists.h"
#include "ucw/lizard.h"
#include "ucw/bbuf.h"
#include "ucw/regex.h"
#include "sherlock/object.h"
#include "sherlock/objread.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

static void
help(void)
{
  fprintf(stderr, "\
Usage: bgrep <options>\n\
\n\
General options:\n"
CF_USAGE
"\n\
Input format options: (default is -1)\n\
-1\t\t\tRead one-part (header only) textual buckets from stdin\n\
-2\t\t\tRead two-part (header and body) textual buckets from stdin\n\
-d\t\t\tRead idxdump output (textual, buckets separated by ### lines)\n\
-f<path>\t\t\tRead the bucket file\n\
\n\
Output options: (default is to print whole matching objects)\n\
-A[<attrs>]\t\tPrint selected attributes from merged header and body\n\
-B[<attrs>]\t\tPrint selected attributes from object body\n\
-H[<attrs>]\t\tPrint selected attributes from object header\n\
-V[<attrs>]\t\tLike -A, but print as tab-separated values; implies -qq\n\
-q\t\t\tBe quiet and don't show bucket numbers\n\
-qq\t\t\t... and don't separate objects with blank lines\n\
-r\t\t\tRecursively fold attribute names\n\
\n\
Pattern options: (conditions are ANDed, default is to accept everything)\n\
-b<attrs>=<regex>\tMatch objects with some of <attrs> in their body matching <regex>\n\
-h<attrs>=<regex>\tMatch objects with some of <attrs> in their header matching <regex>\n\
-i\t\t\tIgnore case distinctions (applies only to regexes following it)\n\
-n\t\t\tNegate (print non-matching objects)\n\
\n\
Attribute specifications (<attrs>):\n\
<char>\t\t\tA single attribute name\n\
(<char><attrs>)\t\tAttributes in a nested block\n\
<attrs><attrs>\t\tMultiple attributes can be specified\n\
<empty>\t\t\tAll attributes at a given nesting level\n\
*\t\t\tAll attributes recursively\n\
");
  exit(1);
}

static byte *buck_name;
static struct mempool *pool;
static struct buck2obj_buf *buck_buf;
static struct fastbuf *out;

enum input_type {
  INPUT_TEXT_1PART,
  INPUT_TEXT_2PART,
  INPUT_TEXT_DUMP,
  INPUT_BUCKETS,
} input_type;
static int ignore_case;
static int fold_names;
static int match_all;
static int quiet_please;
static int negate_match;

struct attrs {
  BIT_ARRAY(names, 512);
  struct matcher *matchers[128];	/* position 0 is for `any', 1 for `any recursively' */
  struct attrs *nested[128];
  uns debug_counter;
};

struct matcher {
  struct matcher *next;
  uns expr_id;				/* or column number for output nodes */
  regex *rx;
};

static struct attrs *out_header, *out_body, *out_merged, *out_tabsep;
static struct attrs *match_header, *match_body;
static uns num_exprs;

static uns out_num_columns;
static byte **out_columns;

static void
debug_matchers(struct attrs *a UNUSED)
{
#ifdef LOCAL_DEBUG
  static uns dump_counter;

  static void do_debug_matchers(struct attrs *a)
    {
      if (!a || a->debug_counter == dump_counter)
	return;
      a->debug_counter = dump_counter;
      fprintf(stderr, "@%p:\n", a);
      for (uns i=0; i<128; i++)
	if (bit_array_isset(a->names, i) || a->matchers[i])
	  {
	    fprintf(stderr, "\t%02x -> (%p)", i, a->matchers[i]);
	    if (!bit_array_isset(a->names, i))
	      fprintf(stderr, " [UNFLAGGED]");
	    for (struct matcher *m=a->matchers[i]; m; m=m->next)
	      fprintf(stderr, " %d", m->expr_id);
	    fprintf(stderr, "\n");
	  }
      for (uns i=0; i<128; i++)
	if (bit_array_isset(a->names, i + OBJ_ATTR_SON) || a->nested[i])
	  {
	    fprintf(stderr, "\tR%02x ->", i);
	    if (!bit_array_isset(a->names, i + OBJ_ATTR_SON))
	      fprintf(stderr, " [UNFLAGGED]");
	    fprintf(stderr, " %p\n", a->nested[i]);
	  }
      for (uns i=0; i<128; i++)
	if (a->nested[i])
	  do_debug_matchers(a->nested[i]);
    }

  dump_counter++;
  if (!a)
    fprintf(stderr, "(NULL)\n");
  else
    do_debug_matchers(a);
  fflush(stderr);
#endif
}

static void
add_matcher(struct attrs *a, uns attr, regex *rx, uns id)
{
  struct matcher *m = cf_malloc(sizeof(*m));
  m->next = a->matchers[attr];
  a->matchers[attr] = m;
  if (id != ~0U)
    m->expr_id = id;
  else
    m->expr_id = out_num_columns++;
  m->rx = rx;
  bit_array_set(a->names, attr);
}

static void
add_attr(struct attrs **where, byte *patt, regex *rx, uns id)
{
  struct attrs *a = *where;
  byte *orig_patt = patt;
  if (!a)
    a = *where = xmalloc_zero(sizeof(*a));
  if (!patt[0])
    add_matcher(a, 0, rx, id);
  else if (patt[0] == '*' && !patt[1])
    add_matcher(a, 1, rx, id);
  else
    {
      while (*patt)
	{
	  if (*patt == '(')
	    {
	      patt++;
	      uns sub = *patt++;
	      if (!sub || sub == ')')
		die("Invalid pattern `%s': incorrect bracketed part", orig_patt);
	      if (sub < 0x21 || sub > 0x7e)
		die("Invalid pattern `%s': invalid name of nested block", orig_patt);
	      byte *start = patt;
	      int nest = 1;
	      while (nest)
		{
		  if (!*patt)
		    die("Invalid pattern `%s': improper nesting of brackets", orig_patt);
		  else if (*patt == '(')
		    nest++;
		  else if (*patt == ')')
		    nest--;
		  patt++;
		}
	      patt[-1] = 0;
	      bit_array_set(a->names, sub + OBJ_ATTR_SON);
	      add_attr(&a->nested[sub], start, rx, id);
	      patt[-1] = ')';
	    }
	  else
	    {
	      uns attr = *patt++;
	      if (attr < 0x21 || attr > 0x7e)
		die("Invalid pattern `%s': invalid name of attribute", orig_patt);
	      add_matcher(a, attr, rx, id);
	    }
	}
    }
}

static void
add_out(struct attrs **where, byte *patt)
{
  add_attr(where, cf_strdup(patt), NULL, 0);
}

static void
add_out_tabsep(struct attrs **where, byte *patt)
{
  add_attr(where, cf_strdup(patt), NULL, ~0U);
}

static void
add_match(struct attrs **where, byte *patt)
{
  patt = cf_strdup(patt);
  byte *sep = strchr(patt, '=');
  if (!sep)
    die("Invalid pattern `%s': missing `='", patt);
  *sep++ = 0;

  if (num_exprs >= 32)
    die("At most 32 expressions at once are supported");
  DBG("@@ expr %d: <%s>", num_exprs, sep);
  regex *rx = rx_compile(sep, ignore_case);
  add_attr(where, patt, rx, num_exprs);
  num_exprs++;
}

static struct matcher *
merge_matchers(struct matcher *x, struct matcher *y)
{
  if (!y)
    return x;

  u32 mask = 0;
  for (struct matcher *z=y; z; z=z->next)
    mask |= 1 << z->expr_id;

  struct matcher *first, **h = &first;
  while (x)
    {
      if (!(mask & (1 << x->expr_id)))
	{
	  struct matcher *z = cf_malloc(sizeof(*z));
	  *z = *x;
	  *h = z;
	  h = &z->next;
	}
      x = x->next;
    }
  *h = y;
  return first;
}

static void
do_fold_matchers(struct attrs *a)
{
  if (a->matchers[1])
    {
      a->matchers[0] = merge_matchers(a->matchers[0], a->matchers[1]);
      struct attrs *all = NULL;
      for (uns i=32; i<128; i++)
	{
	  bit_array_set(a->names, i + OBJ_ATTR_SON);
	  if (a->nested[i])
	    {
	      struct attrs *n = a->nested[i];
	      n->matchers[1] = merge_matchers(n->matchers[1], a->matchers[1]);
	      do_fold_matchers(n);
	    }
	  else
	    {
	      if (!all)
		{
		  uns match_cnt = 0;
		  for (uns i=32; i<128; i++)
		    if (a->matchers[i] || a->nested[i])
		      match_cnt++;
		  if (!match_cnt && a->matchers[0] == a->matchers[1])
		    {
		      /* If there are no other matchers, we can use the current matcher for recursion */
		      all = a;
		    }
		  else
		    {
		      /* Otherwise, we have to create a new one */
		      all = xmalloc_zero(sizeof(*all));
		      all->matchers[0] = a->matchers[1];
		      for (uns i=32; i<128; i++)
			{
			  bit_array_set(all->names, i + OBJ_ATTR_SON);
			  all->nested[i] = all;
			}
		      do_fold_matchers(all);
		    }
		}
	      a->nested[i] = all;
	    }
	}
      a->matchers[1] = NULL;
    }
  if (a->matchers[0])
    {
      for (uns i=32; i<128; i++)
	if (bit_array_test_and_set(a->names, i))
	  a->matchers[i] = merge_matchers(a->matchers[i], a->matchers[0]);
	else
	  a->matchers[i] = a->matchers[0];
      a->matchers[0] = NULL;
    }
  bit_array_clear(a->names, 0);
  bit_array_clear(a->names, 1);
  for (uns i=32; i<128; i++)
    if (a->nested[i] && a->nested[i] != a)
      do_fold_matchers(a->nested[i]);
}

static void
fold_matchers(byte *name UNUSED, struct attrs *a)
{
  DBG("### Original %s", name);
  debug_matchers(a);
  if (a)
    {
      do_fold_matchers(a);
      DBG("### Optimized %s", name);
      debug_matchers(a);
    }
}

static u32
match_obj(struct odes *obj, struct attrs *at)
{
  if (!at)
    return 0;

  u32 mask = 0;
  for (struct oattr *a=obj->attrs; a; a=a->next)
    if (bit_array_isset(at->names, a->attr))
      {
	if (a->attr < OBJ_ATTR_SON)
	  {
	    for (struct matcher *m=at->matchers[a->attr]; m; m=m->next)
	      for (struct oattr *b=a; b && !(mask & (1 << m->expr_id)); b=b->same)
		if (rx_match(m->rx, b->val))
		  mask |= 1 << m->expr_id;
	  }
	else
	  mask |= match_obj(a->son, at->nested[a->attr - OBJ_ATTR_SON]);
      }
  return mask;
}

#define MAX_NESTING 100

static void
do_output(struct odes *obj, struct attrs *at, byte *namebuf, uns nesting)
{
  if (nesting >= MAX_NESTING)
    die("Object nesting too deep (maximum is %d)", MAX_NESTING);
  for (struct oattr *a=obj->attrs; a; a=a->next)
    if (bit_array_isset(at->names, a->attr))
      for (struct oattr *b=a; b; b=b->same)
	if (a->attr < OBJ_ATTR_SON)
	  {
	    if (out_tabsep)
	      {
		uns column = at->matchers[a->attr]->expr_id;
		if (!out_columns[column])
		  out_columns[column] = b->val;
		break;
	      }
	    else
	      {
		if (fold_names)
		  {
		    namebuf[nesting] = a->attr;
		    bwrite(out, namebuf, nesting+1);
		  }
		else
		  bputc(out, a->attr);
		bputsn(out, b->val);
	      }
	  }
	else
	  {
	    uns attr = a->attr - OBJ_ATTR_SON;
	    namebuf[nesting] = attr;
	    if (!fold_names && !out_tabsep)
	      bprintf(out, "(%c\n", attr);
	    do_output(b->son, at->nested[attr], namebuf, nesting+1);
	    if (!fold_names && !out_tabsep)
	      bputs(out, ")\n");
	  }
}

static void
output(struct odes *obj, struct attrs *at)
{
  if (!at)
    return;

  byte namebuf[MAX_NESTING+1];
  do_output(obj, at, namebuf, 0);
}

static int
is_matching(struct odes *o_hdr, struct odes *o_body)
{
  if (match_all)
    return 1;

  u32 mask = match_obj(o_hdr, match_header) | match_obj(o_body, match_body);
  u32 all = (num_exprs == 32 ? ~0U : ((1U << num_exprs) - 1));
  DBG(">>> Mask %08x of %08x", mask, all);
  return (mask == all);
}

static void
grep(byte *leader, struct odes *o_hdr, struct odes *o_body)
{
  DBG("<<< Trying %s", leader);
  int match = is_matching(o_hdr, o_body);
  if (negate_match ? match : !match)
    return;

  DBG(">>> OK");
  if (leader && leader[0] && !quiet_please)
    bputsn(out, leader);
  if (out_merged)
    {
      output(o_hdr, out_merged);
      output(o_body, out_merged);
    }
  else if (out_tabsep)
    {
      bzero(out_columns, out_num_columns * sizeof(out_columns[0]));
      output(o_hdr, out_tabsep);
      output(o_body, out_tabsep);
      for (uns i=0; i<out_num_columns; i++)
	{
	  if (i)
	    bputc(out, '\t');
	  if (out_columns[i])
	    bputs(out, out_columns[i]);
	}
      bputc(out, '\n');
    }
  else
    {
      output(o_hdr, out_header);
      if (quiet_please < 2)
	bputc(out, '\n');
      output(o_body, out_body);
    }
  if (quiet_please < 2)
    bputc(out, '\n');
}

static void
parse_buckets(void)
{
  struct fastbuf *b;
  struct obuck obuck;
  struct obuck_header h;

  obuck_init(&obuck, buck_name, 0);
  while (b = obuck_slurp_pool(&obuck, &h, OBUCK_OID_ANY))
    {
      struct odes *o_hdr, *o_body;
      mp_flush(pool);
      o_hdr = obj_new(pool);
      o_body = obj_new(pool);
      if (buck2obj_parse(buck_buf, h.type, h.length, b, o_hdr, NULL, o_body, 1) < 0)
	log(L_ERROR, "Cannot parse bucket %x of type %x and length %d: %m", h.oid, h.type, h.length);
      else
	{
	  byte leader[256];
	  sprintf(leader, "### %08x %6d %08x", h.oid, h.length, h.type);
	  grep(leader, o_hdr, o_body);
	}
    }
  obuck_cleanup(&obuck);
}

static void
parse_text(void)
{
  struct fastbuf *b = bfdopen_shared(0, 65536);
  bb_t line, leader;
  struct odes *o_hdr = NULL, *o_body = NULL;
  struct obj_read_state ors;
  int phase = 0;
  byte *lend;
  bzero(&ors, sizeof(ors));

  bb_init(&line);
  bb_init(&leader);
  bb_grow(&leader, 1024);
  leader.ptr[0] = 0;
  for (;;)
    {
      uns l = bgets_bb(b, &line, ~0U);
      lend = line.ptr + l - 1;
      if (!lend || !line.ptr[0] || input_type == INPUT_TEXT_DUMP && !memcmp(line.ptr, "###", 3))
	{
	  if (!lend)
	    line.ptr[0] = 0xff;
	  if (phase)
	    {
	      obj_read_end(&ors);
	      if (phase == 1 && input_type != INPUT_TEXT_1PART && !line.ptr[0])
		{
		  obj_read_start(&ors, o_body);
		  phase++;
		}
	      else
		{
		  obj_read_end(&ors);
		  grep(leader.ptr, o_hdr, o_body);
		  phase = 0;
		  leader.ptr[0] = 0;
		}
	    }
	  if (!lend)
	    break;
	  if (line.ptr[0] == '#')
	    {
	      uns len = strlen(line.ptr);
	      bb_grow(&leader, len + 1);
	      memcpy(leader.ptr, line.ptr, len + 1);
	    }
	}
      else
	{
	  if (!phase)
	    {
	      mp_flush(pool);
	      o_hdr = obj_new(pool);
	      o_body = obj_new(pool);
	      obj_read_start(&ors, o_hdr);
	      phase = 1;
	    }
	  obj_read_attr(&ors, line.ptr[0], line.ptr+1);
	}
    }

  bb_done(&leader);
  bb_done(&line);
  bclose(b);
}

int
main(int argc, char **argv)
{
  int opt;

  log_init(NULL);
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS "12A:B:H:V:b:df:h:imnqr", CF_NO_LONG_OPTS, NULL)) != -1)
    switch (opt)
      {
      case 'A':
	add_out(&out_merged, optarg);
	break;
      case 'B':
	add_out(&out_body, optarg);
	break;
      case 'H':
	add_out(&out_header, optarg);
	break;
      case 'V':
	add_out_tabsep(&out_tabsep, optarg);
	break;
      case 'b':
	add_match(&match_body, optarg);
	break;
      case 'h':
	add_match(&match_header, optarg);
	break;
      case '1':
	input_type = INPUT_TEXT_1PART;
	break;
      case '2':
	input_type = INPUT_TEXT_2PART;
	break;
      case 'd':
	input_type = INPUT_TEXT_DUMP;
	break;
      case 'f':
	input_type = INPUT_BUCKETS;
	buck_name = optarg;
	break;
      case 'i':
	ignore_case++;
	break;
      case 'n':
	negate_match++;
	break;
      case 'q':
	quiet_please++;
	break;
      case 'r':
	fold_names++;
	break;
      default:
	help();
      }
  if (optind < argc)
    help();

  if (!match_header && !match_body)
    match_all = 1;
  fold_matchers("Match Header", match_header);
  fold_matchers("Match Body", match_body);
  if (out_merged)
    {
      if (out_header || out_body)
	die("-A cannot be mixed with -H and -B");
    }
  else if (out_tabsep)
    {
      if (out_header || out_body || out_merged)
	die("-V cannot be mixed with -A, -H or -B");
      out_columns = xmalloc(out_num_columns * sizeof(out_columns[0]));
      quiet_please = 2;
    }
  else if (!out_header && !out_body)
    {
      if (input_type == INPUT_TEXT_1PART)
	add_out(&out_merged, "*");
      else
	{
	  add_out(&out_header, "*");
	  add_out(&out_body, "*");
	}
    }
  fold_matchers("Out Header", out_header);
  fold_matchers("Out Body", out_body);
  fold_matchers("Out Merged", out_merged);

  pool = mp_new(1<<14);
  buck_buf = buck2obj_alloc();
  out = bfdopen_shared(1, 65536);

  if (input_type == INPUT_BUCKETS)
    parse_buckets();
  else
    parse_text();
  DBG("DONE");

  bclose(out);
  buck2obj_free(buck_buf);
  mp_delete(pool);
  return 0;
}
