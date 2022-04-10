/*
 *	Sherlock Content Analyser -- IP Range Aliases
 *
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "analyser/analyser.h"
#include "ucw/fastbuf.h"
#include "ucw/chartype.h"
#include "ucw/binsearch.h"
#include "ucw/clists.h"
#include "sherlock/conf.h"

#include <stdio.h>
#include <stdlib.h>

#ifdef LOCAL_DEBUG
#define TRACE(x...) do{log(L_DEBUG, "IPRanges: " x);}while(0)
#else
#define TRACE(x...) do{}while(0)
#endif

struct ip_alias {
  cnode n;
  char *name;
  clist nodes;
};

struct ip_node {
  cnode n;
  struct ip_node *next, *parent;
  struct ip_alias *alias;
  u32 ip[2];
};

static uns ip_ranges_attr;
static clist ip_ranges_list;
static struct ip_node **ip_nodes;
static uns ip_nodes_count;

#define TRY(x) do{ byte *_err=(x); if (_err) return _err; }while(0)

static byte *
parse_node(uns n UNUSED, byte **w, struct ip_node *p)
{
  byte *r = strchr(w[0], '-');
  cf_journal_block(p->ip, sizeof(u32) * 2);
  if (r)
    {
      *r++ = 0;
      TRY(cf_parse_ip(w[0], &p->ip[0]));
      TRY(cf_parse_ip(r, &p->ip[1]));
      if (p->ip[1] < p->ip[0])
        return "Invalid IP range";
    }
  else
    {
      TRY(cf_parse_ip(w[0], &p->ip[0]));
      p->ip[1] = p->ip[0];
    }
  return NULL;
}

static byte *
ip_ranges_commit(void *p UNUSED)
{
  CF_JOURNAL_VAR(ip_nodes);
  ip_nodes = NULL;
  return NULL;
}

static struct cf_section node_config = {
  CF_TYPE(struct ip_node),
  CF_ITEMS {
    CF_PARSER("IP", PTR_TO(struct ip_node, n), parse_node, 1),
    CF_END
  }
};

static struct cf_section alias_config = {
  CF_TYPE(struct ip_alias),
  CF_ITEMS {
    CF_STRING("Name", PTR_TO(struct ip_alias, name)),
    CF_LIST("Range", PTR_TO(struct ip_alias, nodes), &node_config),
    CF_END
  }
};

static struct cf_section an_ip_ranges_config = {
  CF_COMMIT(ip_ranges_commit),
  CF_ITEMS {
    CF_USER("Attr", &ip_ranges_attr, &cf_type_attr),
    CF_LIST("Alias", &ip_ranges_list, &alias_config),
    CF_END
  }
};

static void CONSTRUCTOR
an_iprange_conf_init(void)
{
  cf_declare_section("IPRanges", &an_ip_ranges_config, 0);
}

#define IP_MAX_LEN 16

static void
sprint_ip(byte *dest, uns ip)
{
  sprintf(dest, "%d.%d.%d.%d", (ip >> 24) & 255, (ip >> 16) & 255, (ip >> 8) & 255, ip & 255);
}

static inline int
ip_nodes_cmp(struct ip_node *x, struct ip_node *y)
{
  return x->ip[0] < y->ip[0] || (x->ip[0] == y->ip[0] && x->ip[1] > y->ip[1]);
}

#define ASORT_PREFIX(x) ip_nodes_sort_##x
#define ASORT_KEY_TYPE struct ip_node *
#define ASORT_ELT(i) (ip_nodes[i])
#define ASORT_LT(x,y) (ip_nodes_cmp((x),(y)))
#include "ucw/sorter/array-simple.h"

static void
an_iprange_init(struct an_hook *h)
{
  h->need_mask = AN_NEED_ALL_URLS;

  if (ip_nodes || clist_empty(&ip_ranges_list))
    return;
  if (!ip_ranges_attr)
    die("Undefined IPRanges.Attr");

  TRACE("Preprocessing IP ranges");
  /* Compute array primary sorted by ip, secondary by -sec */
  ip_nodes_count = 0;
  CLIST_FOR_EACH(struct ip_alias *, a, ip_ranges_list)
    CLIST_FOR_EACH(struct ip_node *, n, a->nodes)
      {
        ip_nodes_count++;
        if (n->ip[0] < n->ip[1])
	  ip_nodes_count++;
      }
  TRACE("Sorting %d nodes", ip_nodes_count);
  ip_nodes = cf_malloc(ip_nodes_count * sizeof(struct ip_node *));
  uns index = 0;
  CLIST_FOR_EACH(struct ip_alias *, a, ip_ranges_list)
    CLIST_FOR_EACH(struct ip_node *, n, a->nodes)
      {
        ip_nodes[index++] = n;
	n->alias = a;
        if (n->ip[0] < n->ip[1])
          {
	    struct ip_node *n2 = ip_nodes[index++] = cf_malloc(sizeof(struct ip_node));
            n2->ip[0] = n->ip[1];
            n2->ip[1] = n->ip[0];
            n2->alias = a;
            n2->next = n;
            n->parent = n2;
	  }
	else
	  n->parent = n;
      }
  ip_nodes_sort_sort(ip_nodes_count);

  /* Handle duplicate intervals */
  TRACE("Looking for unique intervals");
  uns j = 0;
  ip_nodes[0]->next = NULL;
  ip_nodes[0]->parent = ip_nodes[0];
  for (uns i = 1; i < ip_nodes_count; i++)
    {
      if (ip_nodes[i]->ip[0] > ip_nodes[i]->ip[1])
        {
	  if (ip_nodes[i]->next)
	    ip_nodes[++j] = ip_nodes[i];
	}
      else if (ip_nodes[i]->ip[0] == ip_nodes[j]->ip[0] && ip_nodes[i]->ip[1] == ip_nodes[j]->ip[1])
        {
	  ip_nodes[i]->parent->next = ip_nodes[i]->next = NULL;
	  ip_nodes[j]->parent = ip_nodes[j]->parent->next = ip_nodes[i];
        }
      else
        {
	  ip_nodes[i]->next = NULL;
	  ip_nodes[++j] = ip_nodes[i]->parent = ip_nodes[i];
	}
    }
  ip_nodes_count = j + 1;

  /* Build tree for subintervals */
  TRACE("Building tree from %d unique nodes", ip_nodes_count);
  struct ip_node *cur = NULL;
  for (uns i = 0; i < ip_nodes_count; i++)
    {
      if (ip_nodes[i]->ip[0] <= ip_nodes[i]->ip[1])
        {
	  ip_nodes[i]->parent = ip_nodes[i]->parent->next = cur;
	  if (ip_nodes[i]->ip[0] != ip_nodes[i]->ip[1])
	    cur = ip_nodes[i];
	}
      else
        {
	  if (unlikely(cur != ip_nodes[i]->next))
	    {
	      byte buf[4][IP_MAX_LEN];
	      sprint_ip(buf[0], ip_nodes[i]->next->ip[1]);
	      sprint_ip(buf[1], ip_nodes[i]->next->ip[0]);
	      sprint_ip(buf[2], cur->ip[0]);
	      sprint_ip(buf[3], cur->ip[1]);
	      die("Invalid pair of crossing IP intervals [%s, %s] and [%s, %s] in IPRanges.Alias entries",
		  buf[0], buf[1], buf[2], buf[3]);
	    }
	  ip_nodes[i]->next = cur->next;
	  cur = ip_nodes[i]->parent = cur->parent;
	}
    }
}

static struct ip_node *
ip_ranges_find(uns ip)
{
#define IP_RANGES_FIND_LT(ary,i,x) (ary[i]->ip[0] < x || (ary[i]->ip[0] == x && ary[i]->ip[1] > x))
  uns i = BIN_SEARCH_FIRST_GE_CMP(ip_nodes, ip_nodes_count, ip, IP_RANGES_FIND_LT);
  if (i == ip_nodes_count)
    return NULL;
  struct ip_node *x = ip_nodes[i];
  if (x->ip[0] > x->ip[1] || (x->ip[0] == x->ip[1] && x->ip[0] == ip))
    return x;
  else
    return x->parent;
}

static void
an_iprange_analyse(struct an_hook *h UNUSED, struct an_iface *ai)
{
  if (!ip_nodes)
    return;
  struct odes *o;
  for (uns i=0; o = ai->all_urls[i]; i++)
    {
      byte *k = obj_find_aval(o, 'k');
      if (k)
	{
	  uns ip;
	  sscanf(k, "%x", &ip);
	  TRACE("Analysing IP 0x%x", ip);
	  for (struct ip_node *x = ip_ranges_find(ip); x; x = x->next)
	    {
	      obj_add_attr(o, ip_ranges_attr, x->alias->name);
	      TRACE("Found alias %s", x->alias->name);
	    }
	}
    }
}

struct analyser an_ip_ranges = {
  .name = "iprange",
  .init = an_iprange_init,
  .analyse = an_iprange_analyse
};
