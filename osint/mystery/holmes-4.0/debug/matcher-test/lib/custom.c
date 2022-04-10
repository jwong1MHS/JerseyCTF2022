/*
 *	Testing of custom statictics and matchers
 *
 *	(c) 2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "search/sherlockd.h"
#include "search/refs.h"

char *custom_file_type_names[MAX_FILE_TYPES] = {
  "unknown", "html", "pdf", "text", "msword", "excel", "RFU6", "RFU7",
  "jpeg", "png", "gif",  "RFU11", "RFU12", "RFU13", "RFU14", "RFU15" };

char *lng_parse(struct query *q, enum custom_op op, char *val, uns intval)
{
  if (op != CUSTOM_OP_EQ)
    return "LNG: Only equality supported";
  if (val)
    return "LNG: Numeric value expected";
  q->lng_only = intval;
  return NULL;
}

int custom_match(struct query *q, struct ref_context *c UNUSED, struct card_attr *ca, int *QQ)
{
  q->lng_value = CA_GET_FILE_LANG(ca);
  if (q->lng_only >= 0 && (int)q->lng_value != q->lng_only)
    return 0;
  if (q->lng_value)
    *QQ = 1000;
  return 1;
}
