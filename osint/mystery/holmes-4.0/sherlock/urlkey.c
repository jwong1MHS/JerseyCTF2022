/*
 *	Sherlock Library -- URL Keys & URL Fingerprints
 *
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "sherlock/index.h"
#include "ucw/url.h"
#include "ucw/fastbuf.h"
#include "ucw/chartype.h"
#include "ucw/hashfunc.h"
#include "ucw/getopt.h"

#include <string.h>
#include <fcntl.h>

/*** Prefix recognition table ***/

struct pxtab_rhs {
  struct pxtab_node *node;
  uns len;
  byte rhs[1];
};

#define PXTAB_FLAG_DOMAIN	0x1
#define PXTAB_FLAG_ASTERISK	0x2

struct pxtab_node {
  struct pxtab_node *parent;
  struct pxtab_rhs *rhs;
  uns len;
  byte flags;
  byte component[0];
};

#define HASH_NODE struct pxtab_node
#define HASH_PREFIX(p) pxtab_##p
#define HASH_KEY_COMPLEX(x) x parent, x component, x len
#define HASH_KEY_DECL struct pxtab_node *parent UNUSED, byte *component UNUSED, uns len UNUSED
#define HASH_WANT_FIND
#define HASH_WANT_LOOKUP
#define HASH_GIVE_HASHFN
#define HASH_GIVE_EQ
#define HASH_GIVE_EXTRA_SIZE
#define HASH_GIVE_INIT_KEY
#define HASH_USE_POOL cf_pool

static inline uns
pxtab_hash(HASH_KEY_DECL)
{
  return ((uns)(uintptr_t)parent) ^ hash_block(component, len);
}

static inline int
pxtab_eq(struct pxtab_node *p1, byte *c1, uns l1, struct pxtab_node *p2, byte *c2, uns l2)
{
  return p1 == p2 && l1 == l2 && !memcmp(c1, c2, l1);
}

static inline int
pxtab_extra_size(HASH_KEY_DECL)
{
  return len;
}

static inline void
pxtab_init_key(struct pxtab_node *node, HASH_KEY_DECL)
{
  node->parent = parent;
  node->len = len;
  memcpy(node->component, component, len);
  node->rhs = NULL;
  node->flags = 0;
}

#include "ucw/hashtable.h"

static inline byte *
pxtab_skip_first_comp(byte *x)
{
  while (*x && *x != ':')
    x++;
  byte *y = x;
  while (*x != '/' || x[1] != '/')
    {
      if (!*x)
	return y;
      x++;
    }
  return x+2;
}

static inline byte *
pxtab_skip_next_comp(byte *x)
{
  for(;;)
    {
      if (!*x)
	return x;
      if (*x == '/')
	return x+1;
      x++;
    }
}

static struct pxtab_node *
pxtab_find_rule(byte *lhs, byte **match_end)
{
  byte *next;
  struct pxtab_node *node, *parent = NULL;

  next = pxtab_skip_first_comp(lhs);
  DBG("\tfirst: %.*s", next-lhs, lhs);
  node = pxtab_find(NULL, lhs, next-lhs);
  *match_end = lhs;
  for (uns i = 0; node; i++)
    {
      *match_end = lhs = next;
      if (!*next)
	return node;
      parent = node;
      next = pxtab_skip_next_comp(lhs);
      DBG("\tnext: %.*s", next-lhs, lhs);
      if (i || next[-1] != '/')
        node = pxtab_find(parent, lhs, next-lhs);
      else
        {
	  struct pxtab_node *domain_parent = parent, *best = NULL;
	  if (*lhs == '.')
	    return NULL;
	  byte *q = next;
	  for (byte *p = next - 1; p >= lhs; p--)
	    if (*p == '.' || p == lhs)
	      {
		parent = node;
		DBG("\tpart: %.*s", q - p, p);
		node = pxtab_find(parent, p, q - p);
		if (!node || !(node->flags & PXTAB_FLAG_DOMAIN))
		  return best ? : domain_parent;
		if (node->flags & PXTAB_FLAG_ASTERISK)
		  best = node;
		q = p;
	      }
	}
    }
  return parent;
}

static struct pxtab_node *
pxtab_add_rule(byte *lhs, struct pxtab_rhs *rhs)
{
  byte *next;
  struct pxtab_node *node, *parent;
#ifdef LOCAL_DEBUG
  byte *lhs_start = lhs;
#endif

  next = pxtab_skip_first_comp(lhs);
  DBG("\tfirst: %.*s", next-lhs, lhs);
  node = pxtab_lookup(NULL, lhs, next-lhs);
  for(uns i = 0; ; i++)
    {
      if (node->rhs)
	return NULL;
      if (!*next)
	break;
      lhs = next;
      next = pxtab_skip_next_comp(lhs);
      parent = node;
      DBG("\tnext: %.*s", next-lhs, lhs);
      if (i || next[-1] != '/')
        node = pxtab_lookup(parent, lhs, next-lhs);
      else
        {
	  byte *q = next;
	  for (byte *p = next - 1; p >= lhs; p--)
	    if (*p == '*' && p == lhs && q == p + 1)
	      {
		DBG("\tfound asterisk");
		node->flags |= PXTAB_FLAG_ASTERISK;
		break;
	      }
	    else if (*p == '.' || p == lhs)
	      {
		parent = node;
		DBG("\tpart: %.*s", q - p, p);
		node = pxtab_lookup(parent, p, q - p);
		node->flags |= PXTAB_FLAG_DOMAIN;
		q = p;
	      }
	}
    }
  DBG("\tsetting rhs, %d to eat", next-lhs_start);
  node->rhs = rhs;
  return node;
}

static struct pxtab_rhs *
pxtab_add_rhs(byte *rhs)
{
  uns len = strlen(rhs);
  struct pxtab_rhs *r = cf_malloc(sizeof(*r) + len);
  r->len = len;
  memcpy(r->rhs, rhs, len+1);
  struct pxtab_node *node = pxtab_add_rule(rhs, r);
  r->node = node;
  return r;
}

static void
pxtab_load(byte *name)
{
  struct fastbuf *f;
  struct pxtab_rhs *rhs = NULL;
  byte line[MAX_URL_SIZE], url[MAX_URL_SIZE], *c, *d;
  int err;
  int lino = 0;

  DBG("Loading prefix table %s", name);
  f = bopen(name, O_RDONLY, 4096);
  while (bgets(f, line, sizeof(line)))
    {
      lino++;
      c = line;
      while (Cblank(*c))
	c++;
      if (!*c || *c == '#')
	continue;
      if (err = url_auto_canonicalize(c, url))
	die("%s, line %d: Invalid URL (%s)", name, lino, url_error(err));
      if (!(d = strrchr(c, '/')) || d[1])
	die("%s, line %d: Prefix rules must end with a slash", name, lino);
      if (c == line)
	{
	  DBG("Creating RHS <%s>", c);
	  if (!(rhs = pxtab_add_rhs(c)))
	    die("%s, line %d: Right-hand side already mapped", name, lino);
	}
      else if (!rhs)
	die("%s, line %d: Syntax error", name, lino);
      else
	{
	  DBG("Adding LHS <%s>", c);
	  if (!pxtab_add_rule(c, rhs))
	    die("%s, line %d: Duplicate rule", name, lino);
	}
    }
  bclose(f);
}

/*** Configuration ***/

static uns urlkey_www_hack;
static char *urlkey_pxtab_path;

static struct cf_section urlkey_config = {
  CF_ITEMS {
    CF_UNS("WWWHack", &urlkey_www_hack),
    CF_STRING("PrefixTable", &urlkey_pxtab_path),
    CF_END
  }
};

static void CONSTRUCTOR urlkey_conf_init(void)
{
  cf_declare_section("URLKey", &urlkey_config, 0);
}

void
url_key_init(void)
{
  pxtab_init();
  if (urlkey_pxtab_path)
    pxtab_load(urlkey_pxtab_path);
}

static inline byte *
url_key_remove_www(byte *url, byte **pbuf)
{
  if (urlkey_www_hack && !strncmp(url, "http://www.", 11))
    {
      byte *buf = *pbuf;
      strcpy(buf, "http://");
      strcpy(buf+7, url+11);
      DBG("\tWWW hack: %s -> %s", url, buf);
      url = buf;
      *pbuf = buf + MAX_URL_SIZE;
    }
  return url;
}

byte *
url_key(byte *url, byte *buf)
{
  DBG("Generating URL key for %s", url);
  url = url_key_remove_www(url, &buf);
  byte *match_end;
  struct pxtab_node *rule = pxtab_find_rule(url, &match_end);
  if (rule && rule->rhs && rule->rhs->node != rule)
    {
      struct pxtab_rhs *rhs = rule->rhs;
      DBG("\tApplying rule <%s>, remove %d, add %d", rhs->rhs, match_end - url, rhs->len);
      memcpy(buf, rhs->rhs, rhs->len);
      strcpy(buf + rhs->len, match_end);
      url = buf;
      buf += MAX_URL_SIZE;
    }
  DBG("\tOutput: %s", url);
  return url;
}

void
url_fingerprint(byte *url, struct fingerprint *fp)
{
  byte buf[URL_KEY_BUF_SIZE];
  fingerprint(url_key(url, buf), fp);
}

#ifdef TEST

int main(int argc, char **argv)
{
  cf_load(cf_def_file);
  url_key_init();
  for (int i=1; i<argc; i++)
    {
      byte buf[URL_KEY_BUF_SIZE];
      struct fingerprint fp;
      byte *key = url_key(argv[i], buf);
      fingerprint(key, &fp);
      for (int j=0; j<12; j++)
	printf("%02x", fp.hash[j]);
      printf(" %s\n", key);
    }
  return 0;
}

#endif
