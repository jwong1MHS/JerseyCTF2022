/*
 *	Sherlock HTML Parser
 *
 *	(c) 1997--2006 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/chartype.h"
#include "sherlock/index.h"
#include "sherlock/conf.h"
#include "ucw/unicode.h"
#include "ucw/ff-unicode.h"
#include "ucw/clists.h"
#include "charset/unicat.h"
#include "lang/lang.h"
#include "gather/gather.h"
#include "gather/format/html/html.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Configuration parameters */

static uns comment_mode;
static uns quote_hack;
static uns script_hack;
static uns refresh_hack;
static uns refresh_threshold;
static uns log_metas;
static uns meta_charset;
static uns meta_language;
static uns xml_charset;
static uns xml_language;
static uns char_ref_hack;
static uns ignore_unknown_char_refs;
static uns robot_comments;
static uns lt_hack;
static char **ignored_metas;

struct user_meta {
  cnode n;
  uns attr;
  char *meta;
};
static clist user_metas;

static struct cf_section html_config_meta = {
  CF_TYPE(struct user_meta),
  CF_ITEMS {
    CF_USER("Attr", PTR_TO(struct user_meta, attr), &cf_type_attr),
    CF_STRING("Meta", PTR_TO(struct user_meta, meta)),
    CF_END
  }
};

static struct cf_section html_config = {
  CF_ITEMS {
    CF_UNS("CommentMode", &comment_mode),
    CF_UNS("QuoteHack", &quote_hack),
    CF_UNS("ScriptHack", &script_hack),
    CF_UNS("CharRefHack", &char_ref_hack),
    CF_UNS("RefreshHack", &refresh_hack),
    CF_UNS("RefreshThreshold", &refresh_threshold),
    CF_UNS("IgnoreUnknownCRs", &ignore_unknown_char_refs),
    CF_UNS("LogMetas", &log_metas),
    CF_UNS("MetaCharset", &meta_charset),
    CF_UNS("MetaLanguage", &meta_language),
    CF_UNS("XMLCharset", &xml_charset),
    CF_UNS("XMLLanguage", &xml_language),
    CF_UNS("RobotComments", &robot_comments),
    CF_UNS("LtHack", &lt_hack),
    CF_LIST("MetaAttr", &user_metas, &html_config_meta),
    CF_STRING_DYN("IgnoreMetas", &ignored_metas, CF_ANY_NUM),
    CF_END
  }
};

static void CONSTRUCTOR html_init(void)
{
  cf_declare_section("HTML", &html_config, 0);
}

/* Global variables */

static struct fastbuf *html_in, *html_out, *meta_out;
static int head_mode, inside_head, attrs_seen;
static byte *found_meta_charset, *found_meta_language, *found_xml_charset;
static byte *fake_redirect;

static uns global_robot_control;	/* META ROBOTS */
static uns local_robot_control;		/* <!-- robots:... --> */
static uns local_robot_accum;		/* Which robot control bits were used locally */
#define RC_INDEX 1
#define RC_FOLLOW 2
#define RC_ARCHIVE 4			/* Google's extension */

/* Reading of input file */

static int ungot_char;

static int
get_char(void)
{
  if (ungot_char >= 0)
    {
      int c = ungot_char;
      ungot_char = -1;
      return c;
    }
  else if (head_mode)			/* We don't know the charset yet */
    {
      int c = bgetc(html_in);
      return (c < 0x80) ? c : (0xe000+c);
    }
  else					/* Already recoded to UTF-8 */
    return bget_utf8(html_in);
}

static inline void
unget_char(int i)
{
  ASSERT(ungot_char < 0);
  ungot_char = i;
}

/* Parsing and decoding of HTML entities, returns UniCode value of the entity */

static int
decode_entity(byte *e)
{
  const struct entity *n;
  struct entity ent;

  if (e[0] == '#')
    {
      n = &ent;
      if (e[1] == 'x' || e[1] == 'X')
	ent.value = strtol(e+2, NULL, 16);
      else
	ent.value = strtol(e+1, NULL, 10);
      if (ent.value >= 0x10000)
	ent.value = UNI_REPLACEMENT;
    }
  else n = is_entity(e, strlen(e));
  if (n)
    {
      DBG("&%s; = %02x", e, n->value);
      return n->value;
    }
  else
    {
      DBG("&%s; = ???", e);
      return 0;
    }
}

static int
get_entity(uns hack)
{
  byte buf[80];
  uns i;
  int c;
  uns startpos = btell(html_in);

  i = 0;
  c = get_char();
  if (c == '#')
    {
      buf[i++] = c;
      c = get_char();
    }
  while (c >= 0 && c < 0x100 && Calnum(c))
    {
      if (i >= sizeof(buf) - 1)		/* Suspiciously long entity, try to recover */
	goto fail;
      buf[i++] = c;
      c = get_char();
    }
  if (c < 0)
    goto fail;
  buf[i] = 0;
  if (c != ';')				/* Broken ending */
    {
      if (!char_ref_hack)
	{
	  log(L_WARN_R, "Invalid character reference &%s<%x>", buf, c);
	  goto fail;
	}
      if (hack)				/* Inside parameter -- take as plain text */
	goto fail;
      unget_char(c);
    }
  c = decode_entity(buf);
  if (c > 0 || ignore_unknown_char_refs)
    return c;

fail:					/* Syntax error, interpret as plain text */
  get_char();				/* Reset unget_char() */
  bsetpos(html_in, startpos);
  return '&';
}

/* References */

static struct gobj_ref *
html_add_ref_full(int type, byte *url, byte *ctype, struct url *base_url, uns nofollow)
{
  struct gobj_ref *r = gobj_add_ref_full(type, url, ctype, base_url);
  if (r && (!(local_robot_control & RC_FOLLOW) || nofollow))
    r->dont_follow = 1;
  return r;
}

static inline struct gobj_ref *
html_add_ref(int type, byte *url, uns nofollow)
{
  return html_add_ref_full(type, url, NULL, NULL, nofollow);
}

static int
contains_frames(void)
{
  CLIST_FOR_EACH(struct gobj_ref *, r, gthis->ref_list)
    if (r->type == 'F')
      return 1;
  return 0;
}

/* Word gathering */

static int force_class;

static int
attrs_to_class(uns a)
{
  if (force_class)
    return force_class;
  if (a & A_TITLE)
    return 0x80 + MT_TITLE;
  if (a & (A_H1 | A_H2 | A_H3))
    return WT_BIG_HEADING;
  if (a & (A_H4 | A_H5 | A_H6))
    return WT_SMALL_HEADING;
  if (a & (A_SUP | A_SUB | A_SMALL | A_SMALLFONT))
    return WT_SMALL;
  if (a & (A_EM | A_B | A_I | A_STRONG | A_DFN | A_BIG | A_LARGEFONT))
    return WT_EMPH;
  if (a & A_OBJECT)
    return WT_ALT;
  return WT_TEXT;
}

static int current_class;
static int queued_sentence_break;

static void
add_word(uns *wo, int class)
{
  struct fastbuf *f = html_out;

  if (class >= 0x80)
    {
      f = meta_out;
      if (class != current_class)
	{
	  bputc(f, 0x90 + class - 0x80);
	  current_class = class;
	}
      else
	bputc(f, ' ');
      class &= 0x0f;
    }
  else
    {
      if (class != current_class || queued_sentence_break)
	{
	  bputc(f, (queued_sentence_break ? 0x90 : 0x80) + class);
	  current_class = class;
	  queued_sentence_break = 0;
	}
      else
	bputc(f, ' ');
    }
  while (*wo)
    {
      if (*wo < 0x80000000)
	bput_utf8(f, *wo);
      else if (*wo < 0x80020000)
	{
	  uns ref = *wo - 0x80020000;
	  /* Yes, really html_out, not f */
	  bputc(html_out, 0xa0 | (ref >> 6));
	  bputc(html_out, 0x80 | (ref & 0x3f));
	}
      else
	bputc(html_out, 0xb0);
      wo++;
    }
}

static uns wordbuf[MAX_WORD_CHARS];
static uns wordlen;
static uns attrs, wattrs;

static void
flush_word(void)
{
  if (wordlen >= MAX_WORD_CHARS || !(local_robot_control & RC_INDEX))
    {
      /*
       * We want to throw the superlong word out, but we really need to write
       * the reference brackets to keep them balanced.
       */
      uns l = wordlen;
      uns i;
      wordlen = 0;
      for (i=0; i<l; i++)
	if (wordbuf[i] >= 0x80000000)
	  wordbuf[wordlen++] = wordbuf[i];
    }
  if (wordlen)
    {
      wordbuf[wordlen] = 0;
      add_word(wordbuf, attrs_to_class(wattrs));
      wordlen = 0;
    }
  wattrs = 0;
}

static inline void
add_char(uns c)
{
  if (c >= 0x80000000)
    {
      /* URL brackets can split extremely long words */
      if (wordlen >= MAX_WORD_CHARS)
	flush_word();
      wordbuf[wordlen++] = c;
    }
  else if (Uprint(c) && !Uspace(c))
    {
      if (wordlen < MAX_WORD_CHARS)
	{
	  wordbuf[wordlen++] = c;
	  wattrs |= attrs;
	}
    }
  else if (wordlen)			/* Redundant condition, speedup only */
    flush_word();
}

static void
emit_ref_start(struct gobj_ref *ref)
{
  if (ref && ref->id < 1024)
    add_char(0x80010000 + ref->id);
}

static void
emit_ref_end(struct gobj_ref *ref)
{
  if (ref && ref->id < 1024)
    add_char(0x80020000);
}

static void
do_add_string(byte *s, int class)
{
  flush_word();
  force_class = class;
  while (*s)
    {
      uns u;
      s = utf8_get(s, &u);
      add_char(u);
    }
  flush_word();
  force_class = 0;
}

static inline void
add_string(byte *s, int class)
{
  if (s)
    do_add_string(s, class);
}

static inline void
add_string_ref(byte *s, int class, struct gobj_ref *ref)
{
  if (s)
    {
      flush_word();
      emit_ref_start(ref);
      do_add_string(s, class);
      emit_ref_end(ref);
    }
}

static void
add_meta(byte *s, uns class)
{
  current_class = -1;
  add_string(s, 0x80 + class);
}

static void
add_obj_ref(byte *base_url, byte *ref, byte *ctype)
{
  byte bbuf1[MAX_URL_SIZE], bbuf2[MAX_URL_SIZE];
  struct url bu, *base = NULL;
  int e;

  if (base_url)
    {
      if (e = url_canon_split_rel(base_url, bbuf1, bbuf2, &bu, gobj_base_url()))
	{
	  log(L_ERROR_R, "Error parsing object base URL %s: %s", base_url, url_error(e));
	  return;
	}
      base = &bu;
    }
  html_add_ref_full('A', ref, ctype, base, 0);
}

#ifndef CONFIG_IMAGES
struct gobj_ref *
image_add_ref(byte *src, byte *width UNUSED, byte *height UNUSED, uns nofollow UNUSED)
{
  struct gobj_ref *r = html_add_ref_full('I', src, NULL, NULL, 1);
  return r;
}
#endif

/* Processing of META tags */

#define MTRACE(x...) do { if (log_metas >= 2) log(L_DEBUG, x); } while (0)
#define MWARN(x...) do { if (log_metas) log(L_WARN_R, x); } while (0)

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
	{		/* `;' is not legal, but frequently used */
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
add_robot_note(byte *text, uns r, uns mask)
{
  obj_add_attr_format(gthis->aa, '.', "%s:%s%s%s", text,
	  (mask & RC_INDEX)   ? ((r & RC_INDEX)   ? " INDEX"   : " NOINDEX")   : "",
	  (mask & RC_FOLLOW)  ? ((r & RC_FOLLOW)  ? " FOLLOW"  : " NOFOLLOW")  : "",
	  (mask & RC_ARCHIVE) ? ((r & RC_ARCHIVE) ? " ARCHIVE" : " NOARCHIVE") : "");
}

static int
chew_meta_refresh(byte *t)
{
  int after = atoi(t);
  byte *k;

  if (k = strchr(t, ';'))
    {
      k++;
      while (Cspace(*k))
	k++;
      if (strncasecmp(k, "URL=", 4))
	return 0;
      k += 4;
    }
  else if (k = strchr(t, ','))
    {
      k++;
      while (Cspace(*k))
	k++;
    }
  else
    return 1;
  if (!*k)
    return 0;

  if (!refresh_hack || after > (int)refresh_hack)
    {
      html_add_ref('R', k+4, 0);
      return 1;
    }
  byte url[MAX_URL_SIZE];
  if (url_auto_canonicalize_rel(k, url, gobj_base_url()))
    return 0;
  if (strcmp(url, gthis->url))
    fake_redirect = gstrdup(url);
  return 1;
}

static void
chew_meta(byte *name, byte *text)
{
  MTRACE("META %s=%s", name, text);

  struct user_meta *um;
  CLIST_WALK(um, user_metas)
    if (!strcasecmp(name, um->meta))
      {
	MTRACE("User-defined META -> attribute %c", um->attr);
	obj_add_attr(gthis->aa, um->attr, text);
	return;
      }

  if (!strcasecmp(name, "description"))
    add_meta(text, MT_MISC);
  else if (!strcasecmp(name, "robots"))
    {
      uns mask;
      uns r = chew_robot_control(text, &mask);
      add_robot_note("Meta robot control", r, mask);
      global_robot_control = ((RC_INDEX | RC_FOLLOW | RC_ARCHIVE) & ~mask) | r;
    }
  else if (!strcasecmp(name, "keywords"))
    add_meta(text, MT_KEYWORD);
  else if (!strcasecmp(name, "title"))
    add_meta(text, MT_MISC);
  else if (!strcasecmp(name, "refresh"))
    {
      if (!chew_meta_refresh(text))
	MWARN("Cannot parse META %s=%s", name, text);
    }
  else if (!strcasecmp(name, "content-type") || !strcasecmp(name, "content-language"))	/* Parsed in head mode */
    ;
  else
    {
      for (uns i=0; i<DARY_LEN(ignored_metas); i++)
	if (!strcasecmp(name, ignored_metas[i]))
	  return;
      MWARN("Unknown META %s=%s", name, text);
    }
}

static void
chew_meta_lang(byte *lang)
{
  byte buf[MAX_LANG_LIST_SIZE];
  int n = lang_normalize_list(buf, lang);
  if (n > 0)
    {
      found_meta_language = gstrdup(buf);
      MTRACE("Found META language %s", found_meta_language);
    }
  else if (n < 0)
    MWARN("Cannot parse META language list `%s'", lang);
}

static void
chew_meta_head(byte *name, byte *text)
{
  MTRACE("HEAD META %s=%s", name, text);
  if (meta_charset && !strcasecmp(name, "content-type"))
    {
      parse_content_type(text, &found_meta_charset);
      if (found_meta_charset)
	{
	  found_meta_charset = gstrdup(found_meta_charset);
	  MTRACE("Found META charset %s", found_meta_charset);
	}
    }
  else if (meta_language && !strcasecmp(name, "content-language"))
    chew_meta_lang(text);
}

/* Chew an already parsed HTML tag */

static byte **attr_names, **attr_vals;
static uns attr_count;
static struct gobj_ref *current_a_ref;
static int base_font_size;

static byte *
getarg(byte *name)
{
  uns i;

  for(i=0; i<attr_count; i++)
    if (!strcmp(attr_names[i], name))
      return attr_vals[i] ? attr_vals[i] : (byte *) "";
  return NULL;
}

static inline byte *
check_lt(byte *arg)
{
  return (lt_hack && arg && strchr(arg, '<')) ? NULL : arg;
}

static int
skip_script(int is_style)
{
  int r = 0;

  if (script_hack)			/* Work-around for broken documents */
    {
      byte *end = is_style ? "</STYLE>" : "</SCRIPT>"; /* Trick: no character occurs twice */
      byte *start = is_style ? "<STYLE>" : "<SCRIPT>";
      int ei = 0, si = 0;
      int nesting = 1;
      while (nesting)
	{
	  r = get_char();
	  if (r < 0)
	    {
	      log(L_ERROR_R, "End of file at nesting level %d when searching for </script>", nesting);
	      return 1;
	    }
	  if (r < 128)
	    r = Cupcase(r);
	  if (start[si] == r || start[si=0] == r)
	    si++;
	  if (end[ei] == r || end[ei=0] == r)
	    ei++;
	  if (!start[si])
	    nesting++;
	  else if (!end[ei])
	    nesting--;
	}
    }
  else					/* Standard behaviour */
    {
      do
	{
	  if (r < 0)
	    return 1;
	  if (r != '<')
	    {
	      r = get_char();
	      continue;
	    }
	}
      while ((r = get_char()) != '/' ||
	     (r = get_char()) < 0 || r < 0 || r >= 256 || !Calpha(r));
      while ((r = get_char()) >= 0 && r != '>')
	;
    }
  return 0;
}

static int
rel_nofollow_p(void)
{
  byte *rel = getarg("REL");
  return (rel && !strcasecmp(rel, "nofollow"));
}

static int
chew_tag(byte *name, byte **anames, byte **avals, uns acount)
{
  const struct tag *t;
  byte *k, *l;
  uns tag;
  static struct tag unknown_tag = { name: "?", type: T_IGNORE | T_FLOW | T_HEAD, arg: 0 };
  struct gobj_ref *ref;

  attr_names = anames;
  attr_vals = avals;
  attr_count = acount;
  t = is_tag(name, strlen(name));
#ifdef LOCAL_DEBUG
  {
    byte buf[4096];
    byte *x = buf;
    uns i;
    x += sprintf(x, "<%s", name);
    for(i=0; i<acount; i++)
      x += sprintf(x, " %s=%s", anames[i], avals[i]);
    *x++ = '>';
    if (t)
      x += sprintf(x, " = %x:%x", t->type, t->arg);
    *x = 0;
    DBG(buf);
  }
#endif
  if (!t)
    t = &unknown_tag;
  tag = t->type;
  if (head_mode)
    {
      if (!(tag & (T_HEAD | T_BOTH)))
	return 1;
      switch (tag & ~(T_HEAD | T_FLOW | T_HARD | T_BOTH))
	{
	case T_META:
	  if ((k = getarg("HTTP-EQUIV")) && (l = getarg("CONTENT")))
	    chew_meta_head(k, l);
	  break;
	case T_HTML:
	  if ((k = getarg("LANG")) && meta_language)
	    chew_meta_lang(k);
	  if ((k = getarg("XML:LANG")) && xml_language)
	    chew_meta_lang(k);
	  break;
	}
      return 0;
    }
  if (inside_head)
    {
      if (!(tag & (T_HEAD | T_BOTH)))
	{
	  inside_head = 0;
	  attrs &= ~ATTRIB_CLEAR_IN_BODY;
	}
    }
  else if (tag & T_HEAD)
    return 0;
  if (!(tag & T_FLOW))
    flush_word();
  if (tag & T_HARD)
    {
      queued_sentence_break = 1;
      attrs &= ~ATTRIB_CLEAR_ON_T_HARD;
    }
  if (k = getarg("LONGDESC"))	/* Attributes which can occur in many different tags */
    html_add_ref('d', k, 0);
  switch (tag & ~(T_HEAD | T_FLOW | T_HARD | T_BOTH))
    {
    case T_IGNORE:
    case T_HTML:		/* Processed only in the header pass */
      break;
    case T_ON:			/* Switch an attribute on */
      if (t->arg & ATTRIB_ONLY_ONCE & attrs_seen)
	break;
      attrs |= t->arg;
      attrs_seen |= t->arg;
      break;
    case T_OFF:			/* Switch an attribute off */
      attrs &= ~t->arg;
      break;
    case T_BASE:			/* Set base URL */
      if (k = getarg("HREF"))
	gthis->base_url = gobj_parse_url(&gthis->base_url_s, k, "base", 0);
      break;
    case T_META:			/* Meta-information */
      k = getarg("NAME");
      if (!k)
	k = getarg("HTTP-EQUIV");
      l = getarg("CONTENT");
      if (k && l)
	chew_meta(k, l);
      break;
    case T_A:				/* Anchor */
      if (t->arg)			/* End */
	{
	  emit_ref_end(current_a_ref);
	  current_a_ref = NULL;
	}
      else if (k = getarg("HREF"))
	{
	  struct gobj_ref *ref = html_add_ref_full('R', k, getarg("TYPE"), NULL, rel_nofollow_p());
	  emit_ref_end(current_a_ref);
	  current_a_ref = ref;
	  emit_ref_start(current_a_ref);
	  if (k = check_lt(getarg("TITLE")))
	    add_string(k, WT_ALT);
	}
      break;
    case T_LINK:			/* A structured link */
      html_add_ref_full('R', getarg("HREF"), getarg("TYPE"), NULL, rel_nofollow_p());
      break;
    case T_IMG:				/* An image */
      ref = image_add_ref(getarg("SRC"), getarg("WIDTH"), getarg("HEIGHT"), rel_nofollow_p());
      byte *alt = check_lt(getarg("ALT"));
      /* Even if ALT is empty, we wish to store the URL brackets if the image is indexable */
      if (ref && !alt && !ref->dont_follow)
	alt = "";
      if (alt)
	{
	  flush_word();
	  emit_ref_start(ref);
	  add_string(alt, WT_ALT);
	  byte *title = check_lt(getarg("TITLE"));
	  if (title && strcmp(title, alt))
	    add_string(title, WT_ALT);
	  emit_ref_end(ref);
	}
      break;
    case T_FRAME:			/* Frame -- who would think such an ugliness will make its way to the HTML specs? */
      html_add_ref('F', getarg("SRC"), rel_nofollow_p());
      break;
    case T_IFRAME:			/* Inline frame -- we need to index it in a different way */
      ref = html_add_ref('a', getarg("SRC"), rel_nofollow_p());
      add_string_ref(check_lt(getarg("TITLE")), WT_ALT, ref);
      break;
    case T_AREA:			/* Area -- another nastie */
      ref = html_add_ref('R', getarg("HREF"), rel_nofollow_p());
      add_string_ref(check_lt(getarg("ALT")), WT_ALT, ref);
      break;
    case T_OBJECT:			/* An embedded object */
      if (!t->arg)
	{
	  byte *classid = getarg("CLASSID");
	  if (classid && !strcasecmp(classid, "clsid:D27CDB6E-AE6D-11cf-96B8-444553540000")) /* Flash movie */
	    attrs |= A_OBJECT_FLASH;
	  else
	    {
	      k = getarg("CODEBASE");
	      add_obj_ref(k, classid, getarg("CODETYPE"));
	      add_obj_ref(k, getarg("DATA"), getarg("TYPE"));
	      /* add_string(getarg("STANDBY"), WT_ALT); */
	      attrs |= A_OBJECT;
	      add_string(check_lt(getarg("TITLE")), WT_ALT);
	    }
	}
      else
	attrs &= ~(A_OBJECT | A_OBJECT_FLASH);
      break;
    case T_PARAM:
      if (attrs & A_OBJECT_FLASH)
        {
	  byte *name = getarg("NAME");
	  if (name && !strcasecmp(name, "MOVIE"))
	    html_add_ref_full('A', getarg("VALUE"), "application/x-shockwave-flash", NULL, rel_nofollow_p());
	}
      break;
    case T_EMBED:
      html_add_ref_full('A', getarg("SRC"), getarg("TYPE"), NULL, rel_nofollow_p());
      break;
    case T_APPLET:			/* An applet */
      add_string(check_lt(getarg("ALT")), WT_ALT);
      add_string(check_lt(getarg("TITLE")), WT_ALT);
      k = getarg("CODEBASE");
      add_obj_ref(k, getarg("CODE"), "application/x-java");
      add_obj_ref(k, getarg("OBJECT"), "application/x-java");
      break;
    case T_FORM:			/* A form */
      html_add_ref('f', getarg("ACTION"), rel_nofollow_p());
      break;
    case T_BLOCKQUOTE:
    case T_INSDEL:
      html_add_ref('R', getarg("CITE"), rel_nofollow_p());
      break;
    case T_TABLE:
      add_string(check_lt(getarg("SUMMARY")), WT_TEXT);
      break;
    case T_BASEFONT:
      if (k = getarg("SIZE"))
	base_font_size = strtol(k, NULL, 10);
      break;
    case T_FONT:
      if (!t->arg)
	{
	  if (k = getarg("SIZE"))
	    {
	      int r = strtol(k, NULL, 10);
	      if (k[0] == '+' || k[0] == '-')
		r += base_font_size;
	      if (r < base_font_size)
		attrs |= A_SMALLFONT;
	      else if (r > base_font_size)
		attrs |= A_LARGEFONT;
	    }
	}
      else
	attrs &= ~(A_SMALLFONT | A_LARGEFONT);
      break;
    case T_SCRIPT:			/* Skip until "</[a-zA-Z]" */
      add_string(check_lt(getarg("TITLE")), WT_ALT);
      return skip_script(t->arg);
    default:
      die("Unknown tag type %x", t->type);
    }
  return 0;
}

/* Parse a SGML control tag (<!...> or <?...>) */

static void
look_for_robot_comment(void)
{
  byte ctrl[256], *p;
  int c;
  uns cnt = 0;
  int negative = 0;

  while ((c = get_char()) >= 0 && Cspace(c))
    ;
  while (c > 0 && c != '-' && !Cspace(c))
    {
      if (cnt >= sizeof(ctrl) - 1)
	return;
      ctrl[cnt++] = c;
      c = get_char();
    }
  if (c < 0)
    return;
  if (c == '-')
    unget_char(c);
  ctrl[cnt] = 0;
  DBG("<!-- %s ... -->", ctrl);

#ifdef CUSTOM_HTML_COMMENT
  { CUSTOM_HTML_COMMENT(ctrl); }
  if (!robot_comments)
    return;
#endif

  p = ctrl;
  if (*p == '/')
    {
      negative = 1;
      p++;
    }
  if (strncasecmp(p, "robots:", 7))
    return;
  flush_word();
  uns r, mask;
  r = chew_robot_control(p+7, &mask);
  if (negative)
    r ^= mask;
  local_robot_control = (local_robot_control & ~mask) | r;
  local_robot_accum |= mask;
  DBG("Local robot control: %d", local_robot_control);
}

static void
parse_sgml_tag(void)
{
  int c;

  DBG("<!some-control-tag>");
  c = get_char();
  for(;;)
    {
      if (c < 0 || c == '>')
	return;
      if (c == '-')			/* Maybe a comment */
	{
	  c = get_char();
	  if (c != '-')
	    continue;
#ifndef CUSTOM_HTML_COMMENT	  
	  if (robot_comments)
#endif
	    look_for_robot_comment();
	  switch (comment_mode)
	    {
	    case 0:			/* Standard behaviour */
	      c = get_char();
	      for(;;)
		{
		  if (c < 0)
		    return;
		  if (c == '-')
		    {
		      c = get_char();
		      if (c == '-')
			break;
		    }
		  else
		    c = get_char();
		}
	      break;
	    case 1:			/* Silly implementation: end with ">" */
	      for(;;)
		{
		  c = get_char();
		  if (c < 0 || c == '>')
		    return;
		}
	      /* Cannot get here */
	    default:			/* Buggy Netscape compatibility: end with "-->" */
	      c = get_char();
	      for(;;)
		{
		  if (c < 0)
		    return;
		  if (c == '-')
		    {
		      c = get_char();
		      while (c == '-')
			{
			  c = get_char();
			  if (c == '>' || c < 0)
			    return;
			}
		    }
		  else
		    c = get_char();
		}
	    }
	}
      else if (c == '"' || c == '\'')	/* Quoted part */
	{
	  int k = c;
	  int hacking_quotes = 0;
	  while ((c = get_char()) != k)
	    {
	      if (c < 0)
		return;
	      if (c == '\r' || c == '\n')
		{
		  if (quote_hack == 1)
		    return;
		  if (quote_hack)
		    hacking_quotes = 1;
		}
	      if (hacking_quotes && c == '>')
		return;
	    }
	}
      c = get_char();
    }
}

/* Parse a HTML tag */

#define MAX_ATTRS 64

static int
parse_tag(void)
{					/* Ugly state-machine-like code :-| */
  byte buf[1024];
  byte *aname[MAX_ATTRS+1], *aval[MAX_ATTRS+1];
  int c, attr;
  byte *k, *stop;

  stop = buf + sizeof(buf) - 8;		/* Parse tag name */
  k = buf;
  attr = 0;
  for(;;)
    {
      c = get_char();
      if (c < 0)
	return 1;
      if (c == '>')
	goto finished;
      if (Uspace(c) || c == '\r' || c == '\n')
	break;
      if (c >= 0x80)			/* Tag names: non-ASCII characters are replaced by DEL */
	c = 0x7f;
      if (k < stop)
	*k++ = Cupcase(c);
    }
  if (k < stop)
    *k++ = 0;

  for(;;)				/* Parse attributes */
    {
      while (c >= 0 && (Uspace(c) || c == '\r' || c == '\n'))
	c = get_char();
      if (c < 0)
	return 1;
      if (c == '>')
	break;
      aname[attr] = k;
      for(;;)
	{
	  if (c == '>')
	    {
	      aval[attr++] = NULL;
	      goto finished;
	    }
	  if (Uspace(c) || c == '\r' || c == '\n')
	    {
	      aval[attr] = NULL;
	      goto cont;
	    }
	  if (c == '=')
	    {
	      if ((c = get_char()) < 0)
		return 1;
	      break;
	    }
	  if (c >= 0x80)		/* Attribute names: non-ASCII characters are replaced by DEL */
	    c = 0x7f;
	  if (k < stop)
	    *k++ = Cupcase(c);
	  if ((c = get_char()) < 0)
	    return 1;
	}
      if (k < stop)
	*k++ = 0;
      aval[attr] = k;
      if (c == '"' || c == '\'')
	{
	  int d = c;
	  int hacking_quotes = 0;

	  c = get_char();
	  while (c != d)
	    {
	      if (c < 0)
		return 1;
	      if (c == '\r' || c == '\n')
		{
		  if (quote_hack == 1)
		    return 0;
		  if (quote_hack)
		    hacking_quotes = 1;
		}
	      if (hacking_quotes && c == '>')
		return 0;
	      if (c == '&')		/* Honor entities in attribute values */
		{
		  c = get_entity(1);
		  if (!c)
		    continue;
		}
	      if (k < stop)		/* Attribute values: non-ASCII characters are encoded in UTF-8 */
		k = utf8_put(k, c);
	      c = get_char();
	    }
	  c = get_char();
	}
      else
	for(;;)
	  {
	    if (c == '>')
	      {
		attr++;
		goto finished;
	      }
	    if (Uspace(c) || c == '\r' || c == '\n')
	      break;
	    else if (k < stop)
	      k = utf8_put(k, c);
	    if ((c = get_char()) < 0)
	      return 1;
	  }
    cont:
      if (k < stop)
	*k++ = 0;
      if (attr < MAX_ATTRS)
	attr++;
    }

finished:
  if (k < stop)
    {
      *k = 0;
      return chew_tag(buf, aname, aval, attr);
    }
  return 0;
}

/* Main loop of the HTML parser */

static void
chew_html(void)
{
  int c;

  ungot_char = -1;
  wordlen = wattrs = 0;
  current_a_ref = NULL;
  base_font_size = 3;
  current_class = WT_TEXT;
  queued_sentence_break = 1;
  global_robot_control = RC_INDEX | RC_FOLLOW | RC_ARCHIVE;
  local_robot_control = RC_INDEX | RC_FOLLOW | RC_ARCHIVE;
  inside_head = 1;

  DBG("### Start ###");
  while ((c = get_char()) >= 0)
    {
      if (c == '<')			/* Tag or control tag */
	{
	  c = get_char();
	  if (c < 0)
	    break;
	  if (c == '!' || c == '?')
	    parse_sgml_tag();
	  else
	    {
	      unget_char(c);
	      if (parse_tag())
		break;
	    }
	  continue;
	}
      if (head_mode)
	continue;
      if (c == '&')			/* Entity */
	{
	  c = get_entity(0);
	  if (!c)
	    continue;
	}
      add_char(c);
    }
  if (current_a_ref)
    emit_ref_end(current_a_ref);
  if (gthis->truncated)
    {
      /*
       * Fill the word buffer, so that the incomplete word at the end
       * will be ignored, but all ref brackets will be properly flushed.
       */
      uns i;
      for (i=0; i<MAX_WORD_CHARS; i++)
	add_char('.');
    }
  flush_word();
}

static uns
skip_white(void)
{
  uns c, n = 0;
  while ((c = get_char()) < 128 && Cspace(c))
    n++;
  unget_char(c);
  return n;
}

static void
chew_xml_decl(void)
{
  uns c, q;
  char name[32], val[64], *p;
  ungot_char = -1;
  for (p = "<?xml"; *p; p++)
    if (get_char() != *p)
      return;
  while (1)
    {
      if (!skip_white())
	return;
      for (p = name; (c = get_char()) < 128 && Calpha(c) && p < name + sizeof(name) - 1; p++)
	*p = c;
      *p = 0;
      unget_char(c);
      skip_white();
      if (get_char() != '=')
	return;
      skip_white();
      if ((q = get_char()) != '\'' && q != '"')
	return;
      for (p = val;  (int)(c = get_char()) >= 0 && c != q && p < val + sizeof(val) - 1; p++)
	*p = c;
      *p = 0;
      if (c != q)
	return;
      if (!strcmp(name, "encoding"))
        {
	  found_xml_charset = gstrdup(val);
	  MTRACE("Found XML charset %s", found_xml_charset);
	  return;
	}
    }
}

int
html_parse(char **args UNUSED)
{
  /* Scan the document for META tags carrying charset info if asked */
  found_meta_charset = NULL;
  found_meta_language = NULL;
  found_xml_charset = NULL;
  head_mode = 1;
  if (meta_charset || meta_language || xml_charset)
    {
      html_in = fbmem_clone_read(gthis->contents);
      if (meta_charset || meta_language)
        chew_html();
      brewind(html_in);
      if (xml_charset)
	chew_xml_decl();
      bclose(html_in);
      if (found_meta_language)			/* META language is to be trusted over server language */
	gthis->language = found_meta_language;
    }

  /* Guess charset and convert to UTF-8 */
  convert_charset(found_meta_charset ? : found_xml_charset);

  /* Run raw HTML analyser */
  gather_raw_analyse();

  /* Parse the document */
  head_mode = 0;
  attrs_seen = 0;
  fake_redirect = NULL;
  html_in = fbmem_clone_read(gthis->contents);
  html_out = gthis->text = fbmem_create(16384);
  meta_out = gthis->meta = fbmem_create(256);
  chew_html();
  bclose(html_in);

  if (fake_redirect && btell(gthis->text) < refresh_threshold && !contains_frames())
    {
      /* Convert the whole page to a redirect if asked for. However, we keep
       * at least the meta-data for the indexer to use when the destination
       * page is not gathered yet.
       */
      gthis->dont_save_contents = 1;
      gobj_add_ref('Y', fake_redirect);
      obj_add_attr(gthis->aa, '.', "META refresh turned into redirect");
    }

  if (!(global_robot_control & RC_INDEX))
    gthis->dont_save_contents = 1;
  if (!(global_robot_control & RC_FOLLOW))
    gthis->dont_follow_links = 1;
  if (!(global_robot_control & RC_ARCHIVE))
    obj_add_attr(gthis->aa, 'q', "");
  if (local_robot_accum)
    add_robot_note("Robot control comments tweaked", local_robot_accum, local_robot_accum);

#ifdef CONFIG_IMAGES
  /* Image references need finishing */
  image_filter_refs();
#endif

  return 1;
}
