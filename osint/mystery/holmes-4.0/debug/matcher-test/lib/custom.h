/* Customization file for the matcher-test config */

#include "customs/bare/lib/custom.h"

#define CUSTOM_STAT_VARS u32 lng_defined_count;
#define CUSTOM_INIT_STATS(q,s) s->lng_defined_count=0;
#define CUSTOM_EARLY_STATS(q,s,a)
#define CUSTOM_LATE_STATS(q,s,a) if (CA_GET_FILE_LANG(a)) s->lng_defined_count++;
#define CUSTOM_MERGE_STATS(q,s,t) t->lng_defined_count += s->lng_defined_count;
#define CUSTOM_SHOW_STATS(q,s,add) add(".Seen %d cards with recognized language", s->lng_defined_count);

#define CUSTOM_MATCH_VARS u32 lng_value; int lng_only;
#define CUSTOM_MATCH_INIT(q) q->lng_value=0; q->lng_only=-1;
#define CUSTOM_MATCH_PARSE CUSTOM_MATCH_KWD(lng, LNG, lng_parse)
#define CUSTOM_MATCH_CACHE_KEY(q,mp) mp_printf(mp, "[LNG=%d] ", q->lng_only)
#define CUSTOM_MATCH(q,c,ca,Q) custom_match(q,c,ca,&Q)
#define CUSTOM_MATCH_SHOW(q,ca,f) f(".Santa was here");

struct query;
struct ref_context;
struct card_attr;

char *lng_parse(struct query *q, enum custom_op op, char *val, uns intval);
int custom_match(struct query *q, struct ref_context *c, struct card_attr *ca, int *QQ);
