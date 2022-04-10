/*
 *	Sherlock Language Processing Library -- Basic functions
 *
 *	(c) 2003--2006 Martin Mares <mj@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/conf.h"
#include "ucw/chartype.h"
#include "ucw/stkstring.h"
#include "lang/lang.h"

#include <string.h>

/* Configuration */

struct lang_alias {
  cnode n;
  byte *name;
};

struct lang_name {
  cnode n;
  uns id;
  byte *name;
  clist aliases;
};

static clist lang_names;
static struct lang_name *lang_names_by_id[MAX_LANGUAGES];
static uns lang_counter = 1;

static byte *
lang_cf_init_name(struct lang_name *n)
{
  if (lang_counter >= MAX_LANGUAGES)
    return "Too many languages defined";
  /* This is a bit awkward, but we want to have language id's available for other modules' config. */
  CF_JOURNAL_VAR(lang_counter);
  CF_JOURNAL_VAR(lang_names_by_id[lang_counter]);
  n->id = lang_counter++;
  lang_names_by_id[n->id] = n;
  return NULL;
}

static byte *
lang_cf_parse_new(uns c UNUSED, byte **pars, byte **ptr)
{
  if (lang_name_to_code(pars[0]) >= 0)
    return "Language already defined";
  *ptr = pars[0];
  return NULL;
}

static struct cf_section lang_cf_alias = {
  CF_TYPE(struct lang_alias),
  CF_ITEMS {
    CF_PARSER("Name", PTR_TO(struct lang_alias, name), lang_cf_parse_new, 1),
    CF_END
  }
};

static struct cf_section lang_cf_name = {
  CF_TYPE(struct lang_name),
  CF_INIT(lang_cf_init_name),
  CF_ITEMS {
    CF_PARSER("Name", PTR_TO(struct lang_name, name), lang_cf_parse_new, 1),
    CF_LIST("Alias", PTR_TO(struct lang_name, aliases), &lang_cf_alias),
    CF_END
  }
};

static struct cf_section lang_config = {
  CF_ITEMS {
    CF_LIST("Language", &lang_names, &lang_cf_name),
    CF_END
  }
};

static void CONSTRUCTOR lang_init_config(void)
{
  cf_declare_section("Lang", &lang_config, 0);
}

/* Config parsers for other modules */

byte *
lang_cf_parse_set(uns c, byte **pars, uns *ptr)
{
  byte *langs[MAX_LANG_LIST_ITEMS];
  int codes[MAX_LANG_LIST_ITEMS];
  if (c == 1 && !strcmp(pars[0], "*"))
    *ptr = ~(~0U << MAX_LANGUAGES);
  else
    {
      uns set = 0;
      for (uns i=0; i<c; i++)
        {
          int n = lang_parse_list(pars[i], langs, codes, 0);
          if (n < 0)
	    return "Invalid language set syntax";
          for (int j=0; j<n; j++)
	    if (codes[j] < 0)
	      return cf_printf("Unknown language `%s'", langs[j]);
	    else
	      set |= 1 << codes[j];
        }
      *ptr = set;
    }
  return NULL;
}

/* Language names and codes */

int
lang_name_to_code(byte *name)
{
  byte *sep = strchr(name, '-');
  int len = (sep ? sep-name : (int)strlen(name));

  CLIST_FOR_EACH(struct lang_name *, n, lang_names)
    {
      /* Need to check n->name, since lookups can be called in the middle of parsing the configuration. */
      if (n->name && !strncasecmp(n->name, name, len) && !n->name[len])
	return n->id;
      CLIST_FOR_EACH(struct lang_alias *, a, n->aliases)
	if (a->name && !strncasecmp(a->name, name, len) && !a->name[len])
	  return n->id;
    }
  return -1;
}

int
lang_code_exists(uns code)
{
  return (code < MAX_LANGUAGES && lang_names_by_id[code]);
}

byte *
lang_code_to_name(uns code)
{
  if (!code)
    return "?";
  else if (!lang_code_exists(code))
    return "??";
  else
    return lang_names_by_id[code]->name;
}

int
lang_normalize_tag(byte *tag)
{
  byte *t = tag;
  while (*t)
    {
      if (*t == '-')
	{
	  if (t == tag || t > tag+8)
	    return -1;
	  tag = ++t;
	}
      else if (!Calpha(*t))
	return 0;
      else
	{
	  *t = Clocase(*t);
	  t++;
	}
    }
  return (t > tag && t <= tag+8);
}

int
lang_parse_list(byte *in, byte **langs, int *codes, int strip_subtags)
{
  byte *t = in;
  uns n = 0;
  while (*t)
    {
      byte *tag = t;
      while (*t && !Cspace(*t))
	t++;
      if (*t)
	*t++ = 0;
      if (*tag)
	{
	  if (!lang_normalize_tag(tag))
	    return -1;
	  if (strip_subtags)
	    {
	      byte *sep = strchr(tag, '-');
	      if (sep)
		*sep = 0;
	    }
	  int code = lang_name_to_code(tag);

	  for (uns i = 0; i < n; i++)
	    if (langs ? !strcmp(langs[i], tag) : codes[i] == code)
	      goto cont2;
	  if (n >= MAX_LANG_LIST_ITEMS)
	    return -1;
	  if (langs)
	    langs[n] = tag;
	  if (codes)
	    codes[n] = code;
	  n++;
	}
    cont2: ;
    }
  if (t > in + MAX_LANG_LIST_SIZE)
    return -1;
  return n;
}

void
lang_construct_list(byte *buf, byte **langs, uns n)
{
  byte *b = buf;
  for (uns i=0; i<n; i++)
    {
      if (i)
	*b++ = ' ';
      uns l = strlen(langs[i]);
      ASSERT(b+l+1 <= buf+MAX_LANG_LIST_SIZE);
      memcpy(b, langs[i], l);
      b += l;
    }
  *b = 0;
}

int
lang_normalize_list(byte *buf, byte *in)
{
  byte *langs[MAX_LANG_LIST_ITEMS];
  int n = lang_parse_list(stk_strdup(in), langs, NULL, 0);
  if (n >= 0)
    lang_construct_list(buf, langs, n);
  return n;
}

int
lang_primary_language(byte *in)
{
  int codes[MAX_LANG_LIST_ITEMS];
  if (lang_parse_list(stk_strdup(in), NULL, codes, 0) <= 0)
    return -1;
  else if (codes[0] >= 0)
    return codes[0];
  else
    return LANG_UNKNOWN;
}

uns
lang_list_to_set(byte *in)
{
  int codes[MAX_LANG_LIST_ITEMS];
  int n = lang_parse_list(stk_strdup(in), NULL, codes, 0);
  uns set = 0;
  for (int i=0; i<n; i++)
    if (codes[i] < 0)
      set |= (1 << LANG_UNKNOWN);
    else
      set |= (1 << codes[i]);
  return set;
}
