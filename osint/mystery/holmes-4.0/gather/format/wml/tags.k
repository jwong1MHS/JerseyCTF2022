%{

/*
 *	Sherlock WML Parser -- Tag Table
 *
 *      (c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include <string.h>

#include "gather/format/wml/wml.h"

%}
struct wml_tag {
  char *name;
  unsigned int type;
  unsigned int arg;
};
%%
wml,		T_WML | T_HARD,			0
card,		T_CARD | T_HARD,		F_CARD
em,		T_FLOW,				F_EM
strong,		T_FLOW,				F_STRONG
i,		T_FLOW,				F_I
b,		T_FLOW,				F_B
u,		T_FLOW,				F_U
big,		T_FLOW,				F_BIG
small,		T_FLOW,				F_SMALL
p,		T_HARD,				0
br,		T_HARD,				0
img,		T_IMG,				0
table,		T_TITLED_ELEMENT | T_HARD,	0
tr,		T_HARD,				0
td,		0,				0
pre,		T_HARD,				0
a,		T_A | T_FLOW,			0
anchor,		T_ANCHOR | T_FLOW,		0
head,		T_HARD,				0
access,		0,				0
meta,		T_META,				0
do,		0,				0
fieldset,	T_TITLED_ELEMENT | T_HARD,	0
go,		T_GO | T_FLOW,			0
input,		T_TITLED_ELEMENT,		0
noop,		T_FLOW,				0
postfield,	T_POSTFIELD | T_FLOW,		0
pre,		T_HARD,				0
prev,		T_FLOW,				0
onevent,	0,				0
optgroup,	T_TITLED_ELEMENT | T_HARD,	0
option,		T_TITLED_ELEMENT,		0
refresh,	T_FLOW,				0
select,		T_TITLED_ELEMENT | T_HARD,	0
setvar,		T_FLOW,				0
template,	T_HARD,				F_TEMPLATE
timer,		T_TIMER | T_FLOW,		0
