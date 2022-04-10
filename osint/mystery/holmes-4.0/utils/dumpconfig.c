/*
 *	Sherlock Gatherer -- Configuration of dumping the files
 *
 *	(c) 2001 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"

char *terminal_charset = "utf-8";
uns line_len = 78;

static struct cf_section dumper_config = {
  CF_ITEMS {
    CF_STRING("TerminalCharset", &terminal_charset),
    CF_UNS("TerminalWidth", &line_len),
    CF_END
  }
};

static void CONSTRUCTOR dumperconfig_init(void)
{
  cf_declare_section("Dumper", &dumper_config, 0);
}

