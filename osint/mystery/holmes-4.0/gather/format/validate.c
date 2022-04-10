/*
 *	Sherlock Gatherer: Document Validator
 *
 *	(c) 2002--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "gather/gather.h"

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>

struct validator {
  cnode n;
  char *ctype_patt;
  char *command;
};

static clist validator_list;
static uns validate_switch;

static struct cf_section validator_config = {
  CF_TYPE(struct validator),
  CF_ITEMS {
    CF_STRING("Type", PTR_TO(struct validator, ctype_patt)),
    CF_STRING("Command", PTR_TO(struct validator, command)),
    CF_END
  }
};

static struct cf_section validate_config = {
  CF_ITEMS {
    CF_UNS("Validate", &validate_switch),
    CF_LIST("Validator", &validator_list, &validator_config),
    CF_END
  }
};

static void CONSTRUCTOR validate_init_config(void)
{
  cf_declare_section("Validate", &validate_config, 0);
}

static void
validate_child(byte *cmd, int srcfd, int resfd)
{
  close(0);
  close(1);
  close(2);
  dup(srcfd);
  dup(resfd);
  dup(resfd);
  close(srcfd);
  close(resfd);
  execlp(cmd, cmd, NULL);
}

static void
validate_parent(int srcfd, int resfd)
{
  fd_set rset, wset;
  struct fastbuf *in = fbmem_clone_read(gthis->contents);
  struct fastbuf *out = fbmem_create(4096);
  int nfd = MAX(srcfd, resfd) + 1;
  int len;
  byte *buf;
  byte line[1024];

  gthis->temp = out;
  fcntl(srcfd, F_SETFL, fcntl(srcfd, F_GETFL, 0) | O_NONBLOCK);
  fcntl(resfd, F_SETFL, fcntl(resfd, F_GETFL, 0) | O_NONBLOCK);
  FD_ZERO(&rset);
  FD_ZERO(&wset);
  for(;;)
    {
      if (srcfd >= 0)
	FD_SET(srcfd, &wset);
      FD_SET(resfd, &rset);
      if (select(nfd, &rset, &wset, NULL, NULL) < 0)
	die("select: %m");
      if (srcfd >= 0 && FD_ISSET(srcfd, &wset))
	{
	  len = bdirect_read_prepare(in, &buf);
	  if (len > 0)
	    {
	      len = write(srcfd, buf, len);
	      if (len < 0)
		gerror(2601, "Error writing validator input: %m");
	      bdirect_read_commit(in, buf+len);
	    }
	  else
	    {
	      FD_CLR(srcfd, &wset);
	      close(srcfd);
	      srcfd = -1;
	    }
	}
      if (FD_ISSET(resfd, &rset))
	{
	  len = bdirect_write_prepare(out, &buf);
	  len = read(resfd, buf, len);
	  if (len < 0)
	    gerror(2601, "Error reading validator output: %m");
	  if (!len)
	    break;
	  bdirect_write_commit(out, buf+len);
	}
    }
  if (srcfd >= 0)
    close(srcfd);
  close(resfd);
  bclose(in);
  bflush(out);
  in = fbmem_clone_read(out);
  struct oattr *oa = NULL;
  while (bgets(in, line, sizeof(line)))
    oa = obj_add_attr(gthis->aa, 'j', line);
  if (!oa)
    obj_add_attr(gthis->aa, 'j', "# OK");
  bclose(in);
  bclose(out);
  gthis->temp = NULL;
}

static void
do_validate(struct validator *val)
{
  pid_t child, ch;
  int status, pipesrc[2], piperes[2];

  if (pipe(pipesrc) || pipe(piperes))
    die("pipe: %m");
  child = fork();
  if (child < 0)
    die("fork: %m");
  else if (child)
    {
      close(pipesrc[0]);
      close(piperes[1]);
      validate_parent(pipesrc[1], piperes[0]);
      ch = wait(&status);
      if (ch != child)
	die("wait: received pid %d instead of %d", ch, child);
      byte err[EXIT_STATUS_MSG_SIZE];
      if (format_exit_status(err, status))
	gerror(2600, "Validator %s", err);
    }
  else
    {
      close(pipesrc[1]);
      close(piperes[0]);
      validate_child(val->command, pipesrc[0], piperes[1]);
    }
}

void
validate_document(void)
{
  if (!validate_switch)
    return;
  CLIST_FOR_EACH(struct validator *, val, validator_list)
    if (match_ct_patt(val->ctype_patt, gthis->content_type))
      {
	do_validate(val);
	break;
      }
}
