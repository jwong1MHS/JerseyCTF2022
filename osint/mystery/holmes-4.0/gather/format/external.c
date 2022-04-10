/*
 *	Sherlock Gatherer: Calling External Parsers
 *
 *	(c) 2003 Robert Spalek <robert@ucw.cz>
 *	(c) 2002--2006 Martin Mares <mj@ucw.cz>
 */

#undef	LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/fastbuf.h"
#include "gather/gather.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>

static void
parser_parent(int srcfd, int resfd)
{
  struct fastbuf *in, *out;
  in = fbmem_clone_read(gthis->contents);
  bclose(gthis->contents);
  out = gthis->contents = fbmem_create(16384);
  fcntl(srcfd, F_SETFL, fcntl(srcfd, F_GETFL, 0) | O_NONBLOCK);
  fcntl(resfd, F_SETFL, fcntl(resfd, F_GETFL, 0) | O_NONBLOCK);

  fd_set rset, wset;
  FD_ZERO(&rset);
  FD_ZERO(&wset);
  DBG("Transferring data to/from the parser");
  for(;;)
    {
      int nfd = MAX(srcfd, resfd) + 1;
      int len;
      byte *buf;
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
	      DBG("Written %d bytes", len);
	      if (len < 0)
		gerror(2241, "Error writing parser input: %m");
	      bdirect_read_commit(in, buf+len);
	    }
	  else
	    {
	      FD_CLR(srcfd, &wset);
	      close(srcfd);
	      DBG("Closed output stream");
	      srcfd = -1;
	    }
	}
      if (FD_ISSET(resfd, &rset))
	{
	  len = bdirect_write_prepare(out, &buf);
	  len = read(resfd, buf, len);
	  DBG("Read %d bytes", len);
	  if (len < 0)
	    gerror(2241, "Error reading parser output: %m");
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
}

static void
parser_child(byte **command, int srcfd, int resfd)
{
  dup2(srcfd, 0);
  dup2(resfd, 1);
  dup2(resfd, 2);
  close(srcfd);
  close(resfd);
  execvp(command[0], (char **) command);
  die("execvp(%s): %m", command[0]);
}

int
external_parse(char **args)
{
  /* args[0]=dest-ctype, args[1..n-1]=command */
  uns n = DARY_LEN(args);
  if (n < 2)
    gerror(2242, "Misconfigured external parser");

  int pipesrc[2], piperes[2];
  if (pipe(pipesrc) || pipe(piperes))
    die("pipe: %m");
  pid_t pid = fork();
  if (pid < 0)
    die("fork: %m");
  else if (pid > 0)
  {
    close(pipesrc[0]);
    close(piperes[1]);
    signal(SIGPIPE, SIG_IGN);
    parser_parent(pipesrc[1], piperes[0]);
    int status;
    pid_t p = wait(&status);
    if (p != pid)
      die("wait: received pid %d instead of %d", p, pid);
    byte err[EXIT_STATUS_MSG_SIZE];
    if (format_exit_status(err, status))
      gerror(2240, "External parser %s", err);
  }
  else
  {
    log_fork();
    close(pipesrc[1]);
    close(piperes[0]);
    byte *cmd[n];
    for (uns i=1; i<n; i++)
      cmd[i-1] = args[i];
    cmd[n-1] = NULL;
    parser_child(cmd, pipesrc[0], piperes[1]);
  }
  gthis->content_type = args[0];
  gthis->file_name = "";	// avoid detection of the same content-type
  return 0;
}
