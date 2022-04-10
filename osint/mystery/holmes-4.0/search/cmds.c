/*
 *	Sherlock Search Engine -- Administration Commands
 *
 *	(c) 1997--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "ucw/string.h"
#include "indexer/params.h"
#include "search/sherlockd.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static void
cmd_databases(int argc UNUSED, char **argv UNUSED)
{
  add_err("+000 OK");
  add_reply("^DATABASES");
  add_reply("%s", "");
  CLIST_FOR_EACH(struct database *, db, databases)
    {
      add_reply("D%s", db->name);
      if (db->params)
	{
	  add_reply("N%d", db->num_ids);
	  add_reply("I%d", db->params->objects_in);
	  add_reply("L%d", (int) db->params->ref_time);
	  add_reply("V%08x", (int) db->params->database_version);
	  add_reply("W%d", db->lexicon_words);
	  add_reply("C%d", db->lexicon_complexes);
	  add_reply("U%d", db->string_count);
	  add_reply("S%d", (int)((db->num_ids * sizeof(struct card_attr)
				  + db->card_file_size
				  + db->ref_file_size
				  + db->lexicon_file_size
				  + db->stems_file_size
				  + db->string_hash_file_size
				  + db->string_map_file_size
				  + 511) / 1024));
	}
      add_reply("%s", "");
    }
}

struct command {
  char *name;
  void (*handler)(int argc, char **argv);
  int argmin, argmax, passwd;
};

static struct command cmds[] = {
  { "databases",	cmd_databases,	0, 0, 0 },
  { NULL,		NULL,		0, 0, 0 }
};

void
do_command(struct query *q)
{
  byte *c = q->cmd;
  char *words[16], **argv;
  int argc;
  struct command *cmd = cmds;

  argc = str_wordsplit(c, words, ARRAY_SIZE(words));
  if (argc <= 0)
    {
      add_err("-109 Malformed command");
      return;
    }
  argv = words;
  argc--;
  while (cmd->name)
    {
      if (!strcasecmp(cmd->name, argv[0]))
	{
	  argv++;
	  if (cmd->passwd)
	    {
	      if (!argc || strcmp(control_password, argv[0]))
		{
		  add_err("-109 Invalid control password");
		  log(L_ERROR, "Invalid control password");
		  return;
		}
	      argc--, argv++;
	    }
	  if (argc < cmd->argmin || argc > cmd->argmax)
	    {
	      add_err("-109 Invalid number of parameters");
	      return;
	    }
	  cmd->handler(argc, argv);
	  return;
	}
      cmd++;
    }
  add_err("-109 Unknown control command");
}
