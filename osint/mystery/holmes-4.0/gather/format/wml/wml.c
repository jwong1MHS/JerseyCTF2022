/*
 *	Sherlock WML 1.0--1.3
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 *	FIXME:
 *	-- share some code with HTML parser
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "sherlock/conf.h"
#include "sherlock/xml/xml.h"
#include "sherlock/xml/dtd.h"
#include "ucw/chartype.h"
#include "ucw/unicode.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-unicode.h"
#include "ucw/mempool.h"
#include "ucw/conf.h"
#include "charset/unicat.h"
#include "lang/lang.h"
#include "gather/gather.h"
#include "gather/format/wml/wml.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* Configuration parameters */

enum cards_mode {
  CARDS_MODE_ALL,
  CARDS_MODE_ALL_REFS,
  CARDS_MODE_FIRST,
};

static uns trace_warnings;
static int cards_mode;
static uns accept_subst_refs;
static uns accept_subst_attrs;
static uns log_metas;
static uns robot_metas;
static uns robot_comments;
static char **ignored_metas;
static uns ontimer_redirects;
static uns ontimer_redirect_threshold;
static uns onenterforward_redirects;

struct user_meta {
  cnode n;
  uns attr;
  char *meta;
};
static clist user_metas;

static struct cf_section wml_config_meta = {
  CF_TYPE(struct user_meta),
  CF_ITEMS {
    CF_USER("Attr", PTR_TO(struct user_meta, attr), &cf_type_attr),
    CF_STRING("Meta", PTR_TO(struct user_meta, meta)),
    CF_END
  }
};

static struct cf_section wml_config = {
  CF_ITEMS {
    CF_UNS("MaxWarnings", &trace_warnings),
    CF_LOOKUP("CardsMode", &cards_mode, ((const char * const []){"all", "all-refs", "first", NULL})),
    CF_UNS("AcceptSubstRefs", &accept_subst_refs),
    CF_UNS("AcceptSubstAttrs", &accept_subst_attrs),
    CF_UNS("LogMetas", &log_metas),
    CF_UNS("RobotMetas", &robot_metas),
    CF_UNS("RobotComments", &robot_comments),
    CF_LIST("MetaAttr", &user_metas, &wml_config_meta),
    CF_STRING_DYN("IgnoreMetas", &ignored_metas, CF_ANY_NUM),
    CF_UNS("OnTimerRedirects", &ontimer_redirects),
    CF_UNS("OnTimerRedirectThreshold", &ontimer_redirect_threshold),
    CF_UNS("OnEnterForwardRedirects", &onenterforward_redirects),
    CF_END
  }
};

static void CONSTRUCTOR
wml_init(void)
{
  cf_declare_section("WML", &wml_config, 0);
}

/* Global variables */

static struct xml_context xml_ctx;		/* LibShXML context */
static uns num_warnings;			/* Number of found warnings */

static struct fastbuf *wml_out, *meta_out;

static uns cur_flags;				/* See enum wml_tag_flag in wml.h */

/* Robot control */

static uns global_robot_control;		/* META ROBOTS */
static uns local_robot_control;			/* <!-- robots:... --> */
static uns local_robot_accum;			/* Which robot control bits were used locally */
#define RC_INDEX	1
#define RC_FOLLOW	2
#define RC_ARCHIVE	4			/* Google's extension */

static void
init_robots(void)
{
  global_robot_control = RC_INDEX | RC_FOLLOW | RC_ARCHIVE;
  local_robot_control = RC_INDEX | RC_FOLLOW | RC_ARCHIVE;
  local_robot_accum = 0;
}

#define MTRACE(x...) do { if (log_metas >= 2) log(L_DEBUG, x); } while (0)
#define MWARN(x...) do { if (log_metas) log(L_WARN_R, x); } while (0)

static void
add_robot_note(byte *text, uns r, uns mask)
{
  obj_add_attr_format(gthis->aa, '.', "%s:%s%s%s", text,
    (mask & RC_INDEX)   ? ((r & RC_INDEX)   ? " INDEX"   : " NOINDEX")   : "",
    (mask & RC_FOLLOW)  ? ((r & RC_FOLLOW)  ? " FOLLOW"  : " NOFOLLOW")  : "",
    (mask & RC_ARCHIVE) ? ((r & RC_ARCHIVE) ? " ARCHIVE" : " NOARCHIVE") : "");
}

static uns
chew_robot_control(byte *t, uns *maskp)
{
  uns r = 0;
  uns mask = 0;
  byte *p;
  byte z;

  while (*t)
    {
      if (Cspace(*t) || *t == ',' || *t == ';')
        { /* `;' is not legal, but frequently used */
	  t++;
	  continue;
	}
      p = t;
      while (*t && *t != ',' && *t != ';' && !Cspace(*t))
        t++;
      z = *t;
      *t = 0;
      if (!strcasecmp(p, "NOINDEX"))
        {
	  r &= ~RC_INDEX;
	  mask |= RC_INDEX;
	}
      else if (!strcasecmp(p, "NOFOLLOW"))
        {
	  r &= ~RC_FOLLOW;
	  mask |= RC_FOLLOW;
	}
      else if (!strcasecmp(p, "NOARCHIVE"))
        {
	  r &= ~RC_ARCHIVE;
	  mask |= RC_ARCHIVE;
	}
      else if (!strcasecmp(p, "INDEX"))
        {
	  r |= RC_INDEX;
	  mask |= RC_INDEX;
	}
      else if (!strcasecmp(p, "FOLLOW"))
        {
	  r |= RC_FOLLOW;
	  mask |= RC_FOLLOW;
	}
      else if (!strcasecmp(p, "ARCHIVE"))
        {
	  r |= RC_ARCHIVE;
	  mask |= RC_ARCHIVE;
	}
      else if (!strcasecmp(p, "ALL"))
        {
	  r = RC_FOLLOW | RC_INDEX | RC_ARCHIVE;
	  mask = r;
	}
      else if (!strcasecmp(p, "NONE"))
        {
	  r = 0;
	  mask = RC_FOLLOW | RC_INDEX | RC_ARCHIVE;
	}
      else if (*p)
        MWARN("Unknown robot control keyword: %s", p);
      *t = z;
    }
  *maskp = mask;
  return r;
}

static void
finish_robots(void)
{
  if (!(global_robot_control & RC_INDEX))
    gthis->dont_save_contents = 1;
  if (!(global_robot_control & RC_FOLLOW))
    gthis->dont_follow_links = 1;
  if (!(global_robot_control & RC_ARCHIVE))
    obj_add_attr(gthis->aa, 'q', "");
  if (local_robot_accum)
    add_robot_note("Robot control comments tweaked", local_robot_accum, local_robot_accum);
}

/* Fake redirects */

static char *fake_redirect;			/* Redirect */
static enum {
  REDIR_PRI_NONE,
  REDIR_PRI_FIRSTCARD_ONTIMER,
  REDIR_PRI_FIRSTCARD_ONENTERFORWARD,
} fake_redirect_priority;			/* And its source */

static void
init_redirects(void)
{
  fake_redirect = NULL;
  fake_redirect_priority = REDIR_PRI_NONE;
}

static void
set_redirect(char *target, uns pri)
{
  DBG("set_redirect(url=%s, pri=%u)", target, pri);
  if (pri > fake_redirect_priority)
    {
      fake_redirect = gstrdup(target);
      fake_redirect_priority = pri;
    }
}

static void
finish_redirects(void)
{
  if (fake_redirect)
    {
      /* Convert the whole page to a redirect if asked for. However, we keep
       * at least the meta-data for the indexer to use when the destination
       * page is not gathered yet. */
      gthis->dont_save_contents = 1;
      gobj_add_ref('Y', fake_redirect);
      obj_add_attr(gthis->aa, '.', (fake_redirect_priority == REDIR_PRI_FIRSTCARD_ONTIMER) ? "ontimer refresh turned into redirect" : "onenterforward refresh turned into redirect");
    }
}

/* Processing of cards */

static uns gather_refs;				/* Gather references */
static uns gather_content;			/* Gather text */

static uns card_no;				/* Card index (1 for the first card) */

static uns card_timer;				/* Timer value in 1/10 s (0=disabled timer) */
static char *card_ontimer_redirect;		/* Where to jump when the timer exceeds */
static char *card_onenterforward_redirect;	/* Where to jump when the card is loaded */

static char *template_ontimer_redirect;		/* The template value for card_ontimer_redirect */
static char *template_onenterforward_redirect;	/* The template value for card_onenterforward_redirect */

static void
setup_gather_mode(void)
{
  if (cards_mode == CARDS_MODE_ALL || !(cur_flags & F_CARD) || !card_no)
    {
      gather_refs = 1;
      gather_content = 1;
    }
  else if (cards_mode == CARDS_MODE_ALL_REFS)
    {
      gather_refs = 1;
      gather_content = 0;
    }
  else
    {
      gather_refs = 0;
      gather_content = 0;
    }
}

static void
begin_card(void)
{
  /* Entering <card> */
  card_no++;
  card_timer = 0;
  card_ontimer_redirect = template_ontimer_redirect;
  card_onenterforward_redirect = template_onenterforward_redirect;
  setup_gather_mode();
}

static void
end_card(void)
{
  /* Leaving </card> */
  if (ontimer_redirects && card_no == 1 && card_ontimer_redirect && card_timer && card_timer <= ontimer_redirect_threshold)
    set_redirect(card_ontimer_redirect, REDIR_PRI_FIRSTCARD_ONTIMER);
  if (onenterforward_redirects && card_no == 1 && card_onenterforward_redirect)
    set_redirect(card_onenterforward_redirect, REDIR_PRI_FIRSTCARD_ONENTERFORWARD);
}

static void
init_cards(void)
{
  card_no = 0;
  template_ontimer_redirect = NULL;
  template_onenterforward_redirect = NULL;
  setup_gather_mode();
}

/* Languages */

static char *found_language;
static enum {
  LANG_PRI_NONE,
  LANG_PRI_CARD,
  LANG_PRI_WML,
  LANG_PRI_META,
} found_language_priority;

static void
add_language(char *language, uns priority)
{
  if (language && priority > found_language_priority && gather_content)
    {
      char buf[MAX_LANG_LIST_SIZE];
      int n = lang_normalize_list(buf, language);
      if (n > 0)
        {
	  found_language = gstrdup(buf);
	  found_language_priority = priority;
	  DBG("Found language %s with priority %u", found_language, priority);
	}
      else if (n < 0)
	DBG("Cannot parse language list `%s'", language);
    }
}

static void
init_langs(void)
{
  found_language = NULL;
  found_language_priority = LANG_PRI_NONE;
}

/* References */

static struct gobj_ref *
wml_add_ref_full(uns type, char *url, char *ctype, struct url *base_url, uns nofollow)
{
  if (!gather_refs)
    return NULL;
  struct gobj_ref *r = gobj_add_ref_full(type, url, ctype, base_url);
  if (r && (!(local_robot_control & RC_FOLLOW) || nofollow))
    r->dont_follow = 1;
  return r;
}

static struct gobj_ref *
wml_add_ref(uns type, char *url, uns nofollow)
{
  return wml_add_ref_full(type, url, NULL, NULL, nofollow);
}

/* Word gathering */

static int force_class;

static uns
flags_to_class(uns f)
{
  if (force_class >= 0)
    return force_class;
  if (f & F_BIG)
    return WT_BIG_HEADING;
  else if (f & F_SMALL)
    return WT_SMALL;
  else if (f & (F_EM | F_STRONG | F_B | F_I | F_U))
    return WT_EMPH;
  else
    return WT_TEXT;
}

static uns current_class;

/*
 * Word_buf codes:
 *
 *   0x00000001 -- 0x0000ffff ... a character
 *
 *   0x80000000 -- 0x8000000f ... WT changer/space
 *               | 0x00000010 ... sentence break
 *               | 0x00000020 ... skipped word
 *               | 0x00000080 ... MT changer/space
 *
 *   0x40010000 -- 0x4001ffff ... starting bracket
 *                 0x40020000 ... ending bracket
 *                 0x40000000 ... reserved space for a bracket (inside '<anchor>' element)
 */

#define GBUF_PREFIX(x) word_buf_##x
#define GBUF_TYPE u32
#include "ucw/gbuf.h"

static word_buf_t word_buf;

static uns word_flags;
static uns word_start;
static uns word_pos;

static uns anchor_pos;
static struct gobj_ref *anchor_ref;
static char *anchor_href;
static char *anchor_enctype;
static uns anchor_get_method;
static clist anchor_postfields;

struct anchor_postfield {
  cnode n;
  char *name;
  char *value;
};

static void
put_bracket(uns c)
{
  if (c & 0x20000)
    bputc(wml_out, 0xb0);
  else if (c & 0x10000)
    {
      bputc(wml_out, 0xa0 | ((c >> 6) & 0xf));
      bputc(wml_out, 0x80 | (c & 0x3f));
    }
}

static void
word_buf_flush(void)
{
  DBG("word_buf_flush");
  struct fastbuf *f;
  u32 *p = word_buf.ptr;
  u32 *end = p + word_pos;
  while (p < end)
    /* Print word */
    if (!(*p & 0x20))
      {
	uns class = *p & 0xff;
	f = (class & 0x80) ? meta_out : wml_out;
	if (class != current_class)
	  {
	    bputc(f, ((class & 0x80) ? 0x90 : 0x80) | class);
	    current_class = class & 0xef;
	  }
	else
	  bputc(f, ' ');
        for (p++; !(*p & 0x80000000); p++)
	  if (*p & 0x40000000)
	    put_bracket(*p);
	  else
	    bput_utf8(f, *p);
      }
    /* Skip word (only write brackets -- we need them balanced) */
    else
      for (p++; !(*p & 0x80000000); p++)
	if (*p & 0x40000000)
	  put_bracket(*p);
  word_pos = word_start = 0;
  word_buf.ptr[0] = *end;
}

static inline void
add_space(void)
{
  DBG("add_space");
  if (word_pos != word_start)
    {
      if (!gather_content || word_pos - word_start >= MAX_WORD_CHARS || !(local_robot_control & RC_INDEX))
	word_buf.ptr[word_start] |= 0x20;
      else
        word_buf.ptr[word_start] |= flags_to_class(word_flags);
      word_buf.ptr[word_start = ++word_pos] = 0x80000000;
      word_flags = 0;
      if (!anchor_pos)
        word_buf_flush();
    }
}

static inline void
add_char(uns c)
{
  DBG("add_char(%c)", (c >= 0x20 && c < 0x80) ? c : '?');
  if (Uprint(c) && !Uspace(c))
    {
      word_buf_grow(&word_buf, word_pos + 4);
      word_buf.ptr[++word_pos] = c;
      word_flags |= cur_flags;
    }
  else if (word_pos != word_start)
    add_space();
}

static inline void
add_sentence_break(void)
{
  DBG("add_sentence_break");
  add_space();
  word_buf.ptr[word_start] |= 0x10;
}

static void
start_ref(struct gobj_ref *ref)
{
  DBG("start_ref");
  if (ref && ref->id < 1024)
    {
      word_buf_grow(&word_buf, word_pos + 4);
      word_buf.ptr[++word_pos] = 0x40010000 | ref->id;
    }
}

static void
end_ref(struct gobj_ref *ref)
{
  DBG("end_ref");
  if (ref && ref->id < 1024)
    word_buf.ptr[++word_pos] = 0x40020000;
}

static void
end_anchor(void)
{
  DBG("end_anchor");
  if (anchor_ref && anchor_ref->id < 1024)
    word_buf.ptr[++word_pos] = 0x40020000;
  anchor_ref = NULL;
  anchor_pos = 0;
  anchor_href = NULL;
}

static void
set_anchor(struct gobj_ref *ref)
{
  DBG("set_anchor");
  if (ref && anchor_pos && !anchor_ref)
    {
      anchor_ref = ref;
      if (ref->id < 1024)
        word_buf.ptr[anchor_pos] = 0x40010000 | ref->id;
      anchor_pos = 0;
    }
}

static void
start_anchor(struct gobj_ref *ref)
{
  DBG("start_anchor");
  end_anchor();
  word_buf_grow(&word_buf, word_pos + 4);
  word_buf.ptr[++word_pos] = 0x40000000;
  anchor_pos = word_pos;
  set_anchor(ref);
}

static void
do_add_string(char *s, uns class)
{
  add_space();
  force_class = class;
  while (*s)
    {
      uns u;
      s = utf8_get(s, &u);
      add_char(u);
    }
  add_space();
  force_class = -1;
}

static inline void
add_string(char *s, uns class)
{
  if (s)
    do_add_string(s, class);
}

static void
add_meta(char *s, uns class)
{
  if (s)
    {
      current_class = -1;
      do_add_string(s, 0x80 + class);
    }
}

enum norm_flags {
  NORM_CDATA = 0x1,	/* Do not normalize whitespace in CDATA */
  NORM_REF = 0x2,	/* The value is an URL reference */
};

static char *
normalize_attr(char *value, uns flags)
{
  if (!value || !*value || (flags & NORM_CDATA))
    return value;
  uns accept_subst = 0;
  if (flags & NORM_REF)
    accept_subst = accept_subst_refs;
  else
    accept_subst = accept_subst_attrs;
  /* Attribute value normalization (XML) and variable substitution (WML) */
  DBG("normalizing [%s]", value);
  char *r = value, *w = value;
  uns c;
  while (c = (byte)*r++)
    {
      if (c == '$')
	if ((c = *r++) == '$')		/* '$$' */
	  *w++ = '$';
        else if (!accept_subst)
	  return NULL;
	else if (c == '(')		/* '$(variable)' */
	  while (*r && *r++ != ')')
	    ;
        else if (Ccat(c, _C_WSTART))	/* '$variable' */
	  while (*r && Cword(*r))
	    r++;
        else				/* Invalid substitution syntax, try to recover */
	  {
	    *w++ = '$';
	    r--;
	  }
      else if (Cspace(c))
        {
	  if (w != value && w[-1] != ' ')
	    *w++ = ' ';			/* Normalize spaces in non-CDATA attributes */
	}
      else
	*w++ = c;
    }
  if (w != value && w[-1] == ' ')
    w--;
  *w = 0;
  DBG(" -> [%s]", value);
  return value;
}

static inline char *
read_attr(char *name, uns flags)
{
  struct xml_attr *attr = xml_attr_find(&xml_ctx, xml_ctx.node, name);
  return !attr ? NULL : !(flags & NORM_CDATA) ? normalize_attr(attr->val, flags) : attr->val;
}

static void
init_words(void)
{
  force_class = current_class = -1;
  word_buf_grow(&word_buf, 1024);
  word_buf.ptr[0] = 0x80000000;
  word_flags = cur_flags = 0;
  word_pos = word_start = word_flags = 0;
  anchor_pos = 0;
  anchor_ref = NULL;
}

/* Processing of meta tags */

static void
chew_meta(void)
{
  char *name = read_attr("name", NORM_CDATA) ? : read_attr("http-equiv", NORM_CDATA);
  char *content = read_attr("content", NORM_CDATA);
  if (!name || !content)
    return;
  MTRACE("META %s=%s", name, content);

  struct user_meta *um;
  CLIST_WALK(um, user_metas)
    if (!strcasecmp(name, um->meta))
      {
        MTRACE("User-defined META -> attribute %c", um->attr);
	obj_add_attr(gthis->aa, um->attr, content);
	return;
      }

  if (!strcasecmp(name, "keywords"))
    add_meta(content, MT_KEYWORD);
  else if (!strcasecmp(name, "title") || !strcasecmp(name, "description"))
    add_meta(content, MT_MISC);
  else if (!strcasecmp(name, "content-language"))
    add_language(content, LANG_PRI_META);
  else if (!strcasecmp(name, "robots"))
    {
      if (robot_metas)
        {
          uns mask;
          uns r = chew_robot_control(content, &mask);
          add_robot_note("Meta robot control", r, mask);
          global_robot_control = ((RC_INDEX | RC_FOLLOW | RC_ARCHIVE) & ~mask) | r;
	}
    }
  else
    {
      for (uns i=0; i<DARY_LEN(ignored_metas); i++)
        if (!strcasecmp(name, ignored_metas[i]))
	  return;
      MWARN("Unknown META %s=%s", name, content);
    }
}

/* Processing of comments */

static void
look_for_robot_comment(void)
{
  char *p = xml_ctx.node->text;
  uns negative = 0;
  while (Cspace(*p))
    p++;
  if (*p == '/')
    {
      p++;
      negative++;
    }
  if (strncasecmp(p, "robots:", 7))
    return;
  add_space();
  uns r, mask;
  r = chew_robot_control(p + 7, &mask);
  if (negative)
    r ^= mask;
  local_robot_control = (local_robot_control & ~mask) | r;
  local_robot_accum |= mask;
  DBG("Local robot control: %d", local_robot_control);
}

/* Processing of tags */

static void
chew_tag(const struct wml_tag *tag, uns start)
{
  if (!tag)
    return;
  DBG("chew_tag(start=%u)", start);
  switch (tag->type & T_MASK)
    {
      case T_A:
	{
	  char *href;
	  if (start && (href = read_attr("href", NORM_REF)))
	    {
	      start_anchor(wml_add_ref('R', href, 0));
	      add_string(read_attr("title", 0), WT_ALT);
	    }
	  else
	    end_anchor();
	  break;
	}

      case T_ANCHOR:
	if (start)
	  {
	    start_anchor(NULL);
	    add_string(read_attr("title", 0), WT_ALT);
	  }
	else
	  end_anchor();
	break;

      case T_GO:
	{
	  char *href;
	  if (start && (href = read_attr("href", NORM_REF)) && !anchor_href)
	    {
	      anchor_href = href;
	      anchor_enctype = read_attr("enctype", 0) ? : "application/x-www-form-urlencoded";
	      char *method = read_attr("method", 0);
	      anchor_get_method = !method || !strcmp(method, "get");
	      clist_init(&anchor_postfields);
	    }
	  else if (!start && anchor_href)
	    {
	      if (!clist_empty(&anchor_postfields))
	        {
		  struct url url;
		  char buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
		  if (!url_deescape(anchor_href, buf1) && !url_split(buf1, &url, buf2))
		    {
		      uns sep = strchr(url.rest, '?') ? '&' : '?';
		      uns l = strlen(anchor_href);
		      byte *s = mp_start_noalign(gthis->pool, l + 1);
		      memcpy(s, anchor_href, l);
		      s += l;
		      CLIST_FOR_EACH(struct anchor_postfield *, p, anchor_postfields)
		        {
			  *s++ = sep;
			  sep = '&';
			  s = mp_spread(gthis->pool, s, 2 * MAX_URL_SIZE + 2);
			  if (url_enescape(p->name, s))
			    goto anchor_err;
			  s += strlen(s);
			  *s++ = '=';
			  if (url_enescape(p->value, s))
			    goto anchor_err;
			  s += strlen(s);
			}
		      *s++ = 0;
		      anchor_href = mp_end(gthis->pool, s);
		    }
		}
	      struct gobj_ref *ref = wml_add_ref_full('R', anchor_href, anchor_enctype, NULL, 0);
	      set_anchor(ref);
	      struct xml_node *parent = xml_ctx.node->parent;
	      if (ref && parent && parent->parent && !strcmp(parent->name, "onevent"))
	        {
		  struct xml_attr *type_attr = xml_attr_find(&xml_ctx, parent, "type");
		  if (!type_attr)
		    goto anchor_err;
		  char *type = normalize_attr(type_attr->val, 0);
		  if (!strcmp(parent->parent->name, "card"))
		    {
		      if (!strcmp(type, "ontimer"))
			card_ontimer_redirect = ref->url;
		      else if (!strcmp(type, "onenterforward"))
			card_onenterforward_redirect = ref->url;
		    }
		  else if (!strcmp(parent->parent->name, "template"))
		    {
		      if (!strcmp(type, "ontimer"))
			template_ontimer_redirect = ref->url;
		      else if (!strcmp(type, "onenterforward"))
			template_onenterforward_redirect = ref->url;
		    }
		}
anchor_err:
	      anchor_href = NULL;
	    }
	  break;
	}

      case T_POSTFIELD:
	{
	  char *name, *value;
	  if (start && (name = read_attr("name", 0)) && *name && (value = read_attr("value", 0)) && anchor_href && anchor_get_method)
	    {
	      struct anchor_postfield *p = mp_alloc(gthis->pool, sizeof(*p));
	      clist_add_tail(&anchor_postfields, &p->n);
	      p->name = name;
	      p->value = value;
	    }
	  break;
	}

      case T_IMG:
	{
	  if (!start)
	    break;
	  struct gobj_ref *ref;
#ifdef CONFIG_IMAGES
	  ref = image_add_ref(read_attr("src", NORM_REF), read_attr("width", 0), read_attr("height", 0), 0);
#else
	  ref = wml_add_ref('I', read_attr("src", NORM_REF), 1);
#endif
	  char *alt = read_attr("alt", 0);
	  /* Even if ALT is empty, we wish to store the URL brackets if the image is indexable */
	  if (ref && !alt && !ref->dont_follow)
	    alt = "";
	  if (alt)
	    {
	      add_space();
	      start_ref(ref);
	      add_string(alt, WT_ALT);
	      end_ref(ref);
	    }
	  break;
	}

      case T_WML:
	if (start)
	  add_language(read_attr("xml:lang", 0), LANG_PRI_WML);
	break;

      case T_CARD:
	if (start)
	  {
	    add_meta(read_attr("title", 0), MT_TITLE);
	    add_language(read_attr("xml:lang", 0), LANG_PRI_CARD);
	    begin_card();
	    char *a;
	    if (a = read_attr("ontimer", NORM_REF))
	      card_ontimer_redirect = a;
	    if (a = read_attr("enenterforward", NORM_REF))
	      card_ontimer_redirect = a;
	  }
	else
	  end_card();
	break;

      case T_META:
	if (start)
	  chew_meta();
	break;

      case T_TITLED_ELEMENT:
	{
	  char *title;
	  if (start && (title = read_attr("title", 0)))
	    {
	      if (tag->type & T_HARD)
	        add_sentence_break();
	      add_string(title, WT_TEXT);
	    }
	  break;
	}

      case T_TIMER:
	{
	  char *value;
	  if (start && (value = read_attr("value", 0)))
	    {
	      unsigned long x;
	      char *end;
	      x = strtoul(value, &end, 10);
	      if (x < ~0U && !*end)
		card_timer = x;
	    }
	  break;
	}
    }
  if (tag->type & T_HARD)
    add_sentence_break();
  else if (!(tag->type & T_FLOW))
    add_space();
}

static void
h_error(struct xml_context *ctx)
{
  if (num_warnings++ < trace_warnings)
    msg(L_WARN_R, "WML: %s", ctx->err_msg);
  else
    ctx->h_error = NULL;
}

static void
h_xml_decl(struct xml_context *ctx)
{
  if (ctx->standalone)
    xml_fatal(ctx, "WML document must not be standalone");
}

static void
h_doctype_decl(struct xml_context *ctx)
{
  if (strcmp(ctx->doctype, "wml"))
    xml_fatal(ctx, "The document type is not WML");
  if (ctx->flags & XML_HAS_INTERNAL_SUBSET)
    xml_fatal(ctx, "WML document must not contain internal subset");
}

static struct xml_dtd_entity *
h_find_entity(struct xml_context *ctx, char *name)
{
#define ENT(n, t) ent_##n = { .name = #n, .text = t, .flags = XML_DTD_ENTITY_DECLARED | XML_DTD_ENTITY_TRIVIAL }
  static struct xml_dtd_entity ENT(nbsp, "\xc2\xa0"), ENT(shy, "\xc2\xad");
#undef ENT
  switch (name[0])
    {
      case 'n':
	if (!strcmp(name, "nbsp"))
	  return &ent_nbsp;
        break;
      case 's':
	if (!strcmp(name, "shy"))
	  return &ent_shy;
	break;
    }
  return xml_def_find_entity(ctx, name);
}

struct element_data {
  const struct wml_tag *tag;
  uns saved_flags;
};

static void
h_stag(struct xml_context *ctx)
{
  const struct wml_tag *tag = is_wml_tag(ctx->node->name, strlen(ctx->node->name));
  if (!tag)
    return;
  struct element_data *d = ctx->node->user = mp_alloc(ctx->stack, sizeof(*d));
  d->saved_flags = cur_flags;
  d->tag = tag;
  cur_flags |= tag->flags;
  chew_tag(tag, 1);
}

static void
h_first_stag(struct xml_context *ctx)
{
  if (!ctx->doctype)
    xml_fatal(ctx, "Missing the document type declaration");
  ctx->h_stag = h_stag;
  h_stag(ctx);
}

static void
h_etag(struct xml_context *ctx)
{
  struct element_data *d = ctx->node->user;
  if (!d)
    return;
  chew_tag(d->tag, 0);
  cur_flags = d->saved_flags;
}

static void
h_pi(struct xml_context *ctx)
{
  xml_fatal(ctx, "Processing instructions are not allowed in WML");
}

static void
h_comment(struct xml_context *ctx UNUSED)
{
  look_for_robot_comment();
}

static void
h_chars(struct xml_context *ctx)
{
  char *s = ctx->node->text;
  uns subst = 0;
  uns c;
  while (*s)
    {
      s = utf8_get(s, &c);
      if (!subst && c == '$')
        {
	  subst = 1;
	  continue;
	}
      if (subst)
	if (subst == 1)
	  {
	    if (c == '(') /* '$(' */
	      {
		subst = 3;
		continue;
	      }
	    else if (c < 0x80 && Ccat(c, _C_WSTART)) /* '$' (alpha|'_')  */
	      {
		subst = 2;
		continue;
	      }
	    else if (c != '$') /* Invalid syntax, try to recover */
	      add_char('$');
	  }
	else if (subst == 3)
	  {
	    if (c == ')') /* '$(' ... ')' */
	      subst = 0;
	    continue;
	  }
	else if (c < 0x80 && Cword(c)) /* '$' (alpha|'_') (alpha|digit|'_')* */
	  continue;
      subst = 0;
      add_char(c);
    }
  if (subst == 1)
    add_char('$');
}

static void
init_sax(void)
{
  xml_ctx.h_error = h_error;
  xml_ctx.h_find_entity = h_find_entity;
  xml_ctx.h_xml_decl = h_xml_decl;
  xml_ctx.h_doctype_decl = h_doctype_decl;
  xml_ctx.h_stag = h_first_stag;
  xml_ctx.h_etag = h_etag;
  xml_ctx.h_pi = h_pi;
  xml_ctx.h_comment = h_comment;
  xml_ctx.h_chars = h_chars;
  xml_ctx.flags |= XML_ALLOC_TAGS;
}

int
wml_parse(char **args UNUSED)
{
  DBG("WML: First pass");
  num_warnings = 0;
  xml_init(&xml_ctx);
  init_sax();
  xml_ctx.pull = XML_PULL_XML_DECL;
  xml_push_fastbuf(&xml_ctx, fbmem_clone_read(gthis->contents));
  xml_next(&xml_ctx);
  if (xml_ctx.err_code)
    goto error;
  char *encoding = xml_ctx.src->decl_encoding;
  if (encoding)
    encoding = gstrdup(encoding);
  convert_charset(encoding);

  DBG("WML: Second pass");
  xml_reset(&xml_ctx);
  init_sax();
  xml_push_fastbuf(&xml_ctx, fbmem_clone_read(gthis->contents));
  xml_ctx.src->fb_encoding = "UTF-8";
  xml_ctx.src->expected_encoding = encoding;

  wml_out = gthis->text = fbmem_create(16384);
  meta_out = gthis->meta = fbmem_create(256);

  init_robots();
  init_redirects();
  init_cards();
  init_langs();
  init_words();

  xml_parse(&xml_ctx);
  if (xml_ctx.err_code)
    goto error;
  end_anchor();
  add_space();
  if (word_pos)
    word_buf_flush();
  gthis->language = found_language;
  finish_redirects();
  finish_robots();

  xml_cleanup(&xml_ctx);

#ifdef CONFIG_IMAGES
  /* Image references need finishing */
  image_filter_refs();
#endif

  return 1;

error: ;
  char *m = gstrdup(xml_ctx.err_msg);
  xml_cleanup(&xml_ctx);
  gerror(2251, "WML error: %s", m);
}
