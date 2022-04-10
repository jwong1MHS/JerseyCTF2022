/*
 *	Sherlock: Custom Parts of Configuration for the free version
 *
 *	(c) 2001--2003 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "sherlock/index.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void
custom_create_attrs(struct odes *odes UNUSED, struct card_attr *ca UNUSED)
{
#ifdef CONFIG_FILETYPE
  char *ctype = obj_find_aval(odes, 'T');
  uns tf;
  if (!ctype)
    tf = 0x00;
  else if (!strcasecmp(ctype, "text/html"))
    tf = 0x10;
  else if (!strcasecmp(ctype, "application/pdf"))
    tf = 0x20;
  else if (!strncasecmp(ctype, "text/", 5))
    tf = 0x30;
  else
    tf = 0x00;
  ca->type_flags = tf;
#endif
}

#ifdef CONFIG_FILETYPE
char *custom_file_type_names[MAX_FILE_TYPES] = {
  "unknown", "html", "pdf", "text", "RFU4", "RFU5", "RFU6", "RFU7",
  "RFU8", "RFU9", "RFU10",  "RFU11", "RFU12", "RFU13", "RFU14", "RFU15" };
#endif
