%{	/* Hey, Emacs, this should look like -*- C -*- */

/*
 *	Sherlock HTML Parser -- Tag Table
 *
 *	(c) 1997--2001 Martin Mares <mj@ucw.cz>
 */

#include <string.h>

#include "gather/format/html/html.h"

%}
struct tag {
  char *name;
  unsigned int type;
  unsigned int arg;
};
%%
H1,	   	T_ON | T_HARD,			A_H1
/H1,		T_OFF | T_HARD,			A_H1
H2,	   	T_ON | T_HARD,			A_H2
/H2,		T_OFF | T_HARD,			A_H2
H3,	   	T_ON | T_HARD,			A_H3
/H3,		T_OFF | T_HARD,			A_H3
H4,	   	T_ON | T_HARD,			A_H4
/H4,		T_OFF | T_HARD,			A_H4
H5,	   	T_ON | T_HARD,			A_H5
/H5,		T_OFF | T_HARD,			A_H5
H6,	   	T_ON | T_HARD,			A_H6
/H6,		T_OFF | T_HARD,			A_H6
TT,		T_ON | T_FLOW,			A_TT
/TT,		T_OFF | T_FLOW,			A_TT
B,		T_ON | T_FLOW,			A_B
/B,		T_OFF | T_FLOW,			A_B
I,		T_ON | T_FLOW,			A_I
/I,		T_OFF | T_FLOW,			A_I
STRONG,		T_ON | T_FLOW,			A_STRONG
/STRONG,	T_OFF | T_FLOW,			A_STRONG
STRIKE,		T_ON | T_FLOW,			A_STRIKE
/STRIKE,	T_OFF | T_FLOW,			A_STRIKE
BIG,		T_ON | T_FLOW,			A_BIG
/BIG,		T_OFF | T_FLOW,			A_BIG
SMALL,		T_ON | T_FLOW,			A_SMALL
/SMALL,		T_OFF | T_FLOW,			A_SMALL
SUB,		T_ON | T_FLOW,			A_SUB
/SUB,		T_OFF | T_FLOW,			A_SUB
SUP,		T_ON | T_FLOW,			A_SUP
/SUP,		T_OFF | T_FLOW,			A_SUP
CODE,		T_ON | T_FLOW,			A_CODE
/CODE,		T_OFF | T_FLOW,			A_CODE
EM,		T_ON | T_FLOW,			A_EM
/EM,		T_OFF | T_FLOW,			A_EM
DFN,		T_ON,				A_DFN
/DFN,		T_OFF,				A_DFN
SAMP,		T_ON,				A_SAMP
/SAMP,		T_OFF,				A_SAMP
KBD,		T_ON,				A_KBD
/KBD,		T_OFF,				A_KBD
VAR,		T_ON,				A_VAR
/VAR,		T_OFF,				A_VAR
ADDRESS,    	T_ON | T_HARD,			A_ADDRESS
/ADDRESS,   	T_OFF | T_HARD,			A_ADDRESS
ACRONYM,	T_ON,				A_ACRONYM
/ACRONYM,	T_OFF,				A_ACRONYM
ABBR,		T_ON,				A_ACRONYM	/* We consider ABBR and ACRONYM equivalent */
/ABBR,		T_OFF,				A_ACRONYM
TITLE,		T_ON | T_HEAD | T_HARD,		A_TITLE
/TITLE,		T_OFF | T_HEAD | T_HARD,	A_TITLE
P,		T_IGNORE | T_HARD,		0
/P,		T_IGNORE | T_HARD,		0
BR,		T_IGNORE | T_HARD,		0
HR,		T_IGNORE | T_HARD,		0
BASE,		T_BASE | T_HEAD,		0
META,		T_META | T_HEAD,		0
A,		T_A | T_FLOW,			0
/A,		T_A | T_FLOW,			1
IMG,		T_IMG,				0
APPLET,		T_APPLET,			0
OBJECT,		T_OBJECT | T_HARD | T_BOTH,	0
/OBJECT,	T_OBJECT | T_HARD | T_BOTH,	1
PARAM,		T_PARAM,			0
EMBED,		T_EMBED | T_HARD,		0
FORM,		T_FORM | T_HARD,		0
PRE,		T_IGNORE | T_HARD,		0
/PRE,		T_IGNORE | T_HARD,		0
LI,		T_IGNORE | T_HARD,		0
/LI,		T_IGNORE | T_HARD,		0
DL,		T_IGNORE,			0
/DL,		T_IGNORE,			0
OL,		T_IGNORE,			0
/OL,		T_IGNORE,			0
UL,		T_IGNORE,			0
/UL,		T_IGNORE,			0
DT,		T_IGNORE | T_HARD,		0
/DT,		T_IGNORE | T_HARD,		0
DD,		T_IGNORE | T_HARD,		0
/DD,		T_IGNORE | T_HARD,		0
DIR,		T_IGNORE,			0
/DIR,		T_IGNORE,			0
MENU,		T_IGNORE,			0
/MENU,		T_IGNORE,			0
DIV,		T_IGNORE,			0
/DIV,		T_IGNORE,			0
CENTER,		T_IGNORE | T_HARD,		0
/CENTER,	T_IGNORE | T_HARD,		0
BLOCKQUOTE,	T_BLOCKQUOTE | T_HARD,		0
/BLOCKQUOTE,	T_IGNORE | T_HARD,		0
Q,		T_BLOCKQUOTE,			0	/* We consider BLOCKQUOTE and Q equivalent */
/Q,		T_IGNORE,			0
TABLE,		T_TABLE | T_HARD,		0
/TABLE,		T_IGNORE | T_HARD,		0
CAPTION,	T_IGNORE | T_HARD,		0
/CAPTION,	T_IGNORE | T_HARD,		0
TR,		T_IGNORE | T_HARD,		0
/TR,		T_IGNORE | T_HARD,		0
TH,		T_IGNORE | T_HARD,		0
/TH,		T_IGNORE | T_HARD,		0
TD,		T_IGNORE,			0
/TD,		T_IGNORE,			0
SCRIPT,		T_SCRIPT | T_HARD | T_BOTH,	0	/* SCRIPT and STYLE have the same ending */
STYLE,		T_SCRIPT | T_HARD | T_BOTH,	1	/* STYLE should be T_HEAD, but it's often misplaced */
LINK,		T_LINK | T_HEAD,		0
FRAME,		T_FRAME,			0
IFRAME,		T_IFRAME,			0
AREA,		T_AREA,				0
/HEAD,		T_IGNORE,			0	/* These three are ignored, but they serve as common HEAD endings */
BODY,		T_IGNORE,			0	/* ... */
FRAMESET,	T_IGNORE,			0	/* ... */
INS,		T_INSDEL | T_HARD,		0
/INS,		T_IGNORE | T_HARD,		0
DEL,		T_INSDEL | T_HARD,		0
/DEL,		T_IGNORE | T_HARD,		0
FONT,		T_FONT | T_FLOW,		0
/FONT,		T_FONT | T_FLOW,		1
BASEFONT,	T_BASEFONT,			0
HTML,		T_HTML | T_HEAD,		0
