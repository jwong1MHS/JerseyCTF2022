/*
 *	Sherlock Gatherer: Parser for robots.txt Files
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/chartype.h"
#include "ucw/ff-unicode.h"
#include "gather/gather.h"

#include <string.h>

static uns robots_trace;
static uns robots_work_arounds;
static char *my_robot_name;

static char *
robots_commit_config(void *p UNUSED)
{
  if (!my_robot_name)
    return "Robots.RobotName must be set";
  return NULL;
}

static struct cf_section robots_config = {
  CF_COMMIT(robots_commit_config),
  CF_ITEMS {
    CF_UNS("Trace", &robots_trace),
    CF_UNS("WorkArounds", &robots_work_arounds),
    CF_STRING("RobotName", &my_robot_name),
    CF_END
  }
};

static void CONSTRUCTOR robots_init(void)
{
  cf_declare_section("Robots", &robots_config, 0);
}

#define TRACE(x,y...) do { if (robots_trace) log(L_DEBUG, x,##y); } while (0)
#define XTRACE(x,y...) do { if (robots_trace > 1) log(L_DEBUG, x,##y); } while (0)

int
robots_parse(char **args UNUSED)
{
  byte buf[1024], *i, *j, *k, *cmt;
  struct fastbuf *in;
  int c;
  uns cnt;
  int inside = 0;
  int itsme = 0;
  int nonascii = 0;
  int complained = 0;
  uns comment_p;

  convert_charset(NULL);
  in = gthis->temp = fbmem_clone_read(gthis->contents);

  for(;;)
    {
      cnt = 0;
      nonascii = 0;
      while ((c = bget_utf8(in)) >= 0 && c != '\r' && c != '\n')
	{
	  if (cnt < sizeof(buf) - 1)
	    {
	      if ((c < ' ' && c != '\t') || c >= 127)
		nonascii = 1;
	      else
		buf[cnt++] = c;
	    }
	}
      if (c < 0 && !cnt)
	break;
      while (c == '\r')
	c = bgetc(in);
      if (c >= 0 && c != '\n')
	bungetc(in);
      if (cnt == sizeof(buf) - 1)
	gerror(2201, "Robots: Line too long");
      if (nonascii)
	{
	  if (!complained++)
	    log(L_ERROR_R, "Robots: Ignoring rules with non-ASCII characters");
	  continue;
	}
      buf[cnt] = 0;
      XTRACE("Robots: %s", buf);
      i = buf;				/* Remove leading blanks */
      while (Cspace(*i))
	i++;
      cmt = i;				/* Remove comments and trailing blanks */
      while (*cmt && *cmt != '#')
	cmt++;
      comment_p = *cmt;
      while (cmt > i && Cspace(cmt[-1]))
	cmt--;
      *cmt = 0;
      if (!*i)				/* Record separator */
	{
	  if (comment_p)		/* A comment is not a RS */
	    continue;
	  inside = 0;
	  if (itsme && !robots_work_arounds)
	    {
	      /* Extra blank lines are a common error */
	      XTRACE("Resetting context");
	      itsme = 0;
	    }
	  continue;
	}
      for(j=i; *j && !Cspace(*j) && *j !=':'; j++)	/* Find the argument */
	;
      if (*j)
	{
	  if (*j == ':')
	    *j++ = 0;
	  while (Cspace(*j))
	    *j++ = 0;
	}
      for(k=j; *k && !Cspace(*k); k++)	/* Chop off trailing blanks */
	;
      *k = 0;
      if (!strcasecmp(i, "User-Agent"))
	{
	  if (!inside && itsme)
	    itsme = 0;
	  inside = 1;
	  if (!strncasecmp(my_robot_name, j, strlen(my_robot_name)) || !strcmp(j, "*"))
	    {
	      XTRACE("Matched name");
	      itsme = 1;
	    }
	  else
	    XTRACE("Mismatched name");
	  continue;
	}
      if (!inside && !robots_work_arounds)
	log(L_ERROR_R, "Robots: %s before first User-Agent", i);
      inside = 1;
      if (!strcasecmp(i, "Disallow") || (robots_work_arounds && !strcasecmp(i, "Disalow")))
	{
	  if (itsme)
	    {
	      if (!*j)			/* Allow all */
		{
		  TRACE("Allowing all");
		  obj_set_attr(gthis->aa, 'r', NULL);
		}
	      else			/* Disallow something */
		{
		  TRACE("Disallowing %s", j);
		  if (j[0] == '/')
		    obj_add_attr(gthis->aa, 'r', j);
		  else
		    log(L_ERROR_R, "Robots: Misplaced relative path %s", j);
		}
	    }
	}
      else if (robots_work_arounds &&
	       (!strcasecmp(i, "<!doctype") ||
		((j = strchr(i, '<')) && strchr(j+1, '>'))))
	gerror(2200, "Robot file contains HTML tags");
      else
	log(L_ERROR_R, "Robots: Unknown keyword %s", i);
    }

  bclose(in);
  gthis->temp = NULL;
  return 1;
}
