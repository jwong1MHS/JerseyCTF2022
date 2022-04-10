/*
 *  Sherlock Gatherer: PDF parser
 *
 *  (c) 2002--2003 Milan Vancura <milan@ucw.cz>
 *  (c) 2003--2006 Martin Mares <mj@ucw.cz>
 *
 *  see pdf.c for TODO list
 */

/*
 * Error codes for fatal errors (22xx)
 *
 * 20 base structure of objects is corrupted (xref tables etc.)
 * 21 don't know how to decrypt
 * 22 filepos stack (under|over)flow
 * 23 PDF stream error
 * 24 no permission to extract text
 */

/* Configuration */

extern uns pdf_trace;
extern uns xreft_size;
extern uns respect_user_rights;
extern uns pdf_warnings;

/* Logging and warnings */

#define LOG(f...) log(L_DEBUG, f)

#ifdef LOCAL_DEBUG
#define MAX_TRACE 1000000
#else
#define MAX_TRACE 1000
#endif

#define TRACE(t,f...) do { if ((t) < MAX_TRACE && (int)pdf_trace>=(t)) LOG(f); } while(0)
#define BTRACE(t,p,l,f...) do { int i; if((t) < MAX_TRACE && (int)pdf_trace>=(t)) { LOG(f); for(i=0;i<(l);i++) fputc((p)[i],stderr); fputc('\n',stderr); } } while(0)
#define HTRACE(t,p,l,f...) do { uns i; byte *T=(char *)(p); if((t) < MAX_TRACE && (int)pdf_trace>=(t)) { LOG(f); for(i=0;i<(l);i++) fprintf(stderr,"%.2x",*T++); } } while(0)
#define PRINTOBJ(t,o) do { if ((t) < MAX_TRACE && pdf_trace>=(t)) printobj(o); } while(0)

#define pdf_warn(x...) do { if (pdf_warnings) log(L_WARN_R, "PDF: " x); } while(0)

/* Limits */

#define STRINGBUFLEN	 65536
/* TEMPBUFLEN must be > 256 */
#define TEMPBUFLEN	  1024
#define STREAMBLKLEN	   263
#define UNICODESTRINGCNT   256
#define UNICODESTRMAXLEN   512

#define MAX_BUFF_SIZE	100
#define MAX_TOK_SIZE	100
#define MAX_TREE_SIZE  1000
#define MAX_SEEK_POS	 10
#define MAX_SUB_PAGES	 10
#define MAX_STREAMS	 15
#define LOOK4STARTXREF	 50
#define GS_MAX_DEPTH	128

/* Space types */

#define SP_NOEOL	1
#define SP_EOL		2
#define SP_COMMENT	4
#define SP_DELIM	8
#define SP_ANYSPACE	(SP_NOEOL | SP_EOL)
#define SP_ANYWCOMM	(SP_NOEOL | SP_EOL | SP_COMMENT)
#define SP_ANYWDELIM	(SP_NOEOL | SP_EOL | SP_DELIM)

typedef struct {
    s32 position;
    u16 generation;
} OBJ_START;

/* OBJECT types */

enum ot_type {
    OT_UNKNOWN,
    OT_BOOL,
    OT_INT,
    OT_REAL,
    OT_OBJREF,
    OT_STRING,
    OT_NAME,
    OT_ARRAY,
    OT_DICT,
    OT_STREAM,
    OT_NULL,
    OT_UTOK,
    OT_MAX
};

typedef struct object {
    enum ot_type type;
    union {
	s32 n;			/* for bool, int (values), references (obj id) */
	byte *s;		/* for names */
	struct dict_entry *d;	/* for dictionaries */
	struct array_entry *a;	/* for arrays */
	float r;		/* for reals */
	struct {		/* for strings: pointer and length */
	    byte *i;
	    int l;
	} str;
    } value;
} OBJECT;

typedef struct dict_entry {
    char *name;
    OBJECT obj;
    struct dict_entry *next;
} DICT_ENTRY;

typedef struct array_entry {
    OBJECT obj;
    struct array_entry *next;
} ARRAY_ENTRY;

typedef struct {
    char *name;
    OBJECT *obj;
} OBJ_GET;

/*
 * VERY incomplete struct of PDF graphic state. Store the needed parts only.
 * For complete list and default values see PDF Reference Manual.
 */
struct gsdata {
    s32 ctm_sy;		/* CTM.scale_y; default=1 */
    struct fencoding *tf;/* text font; no default */
    s32 tfs;		/* text font size; no default */
    s32 tm_sy;		/* TM.scale_y; default=1 */
    enum {
	trm_nochange=-1,
	trm_fill,
	trm_stroke,
	trm_fillstroke,
	trm_nothing,
	trm_fillclip,
	trm_strokeclip,
	trm_fillstrokeclip,
	trm_clip,
	trm_max
    } trm;		/* text rendering mode; default=fill */
};

struct gstate {
    struct gstate *next;
    uns obj;
    struct gsdata *data;
};

struct gsname {
    struct gsname *next;
    uns obj;
    uns namehash;
    byte *name;
    struct gsdata *gsd;
};

/*
 * Font data structures
 */

struct unichar {
    u16 fcode;
    u16 unicode;
};

struct fencoding {
    byte bytes;
    struct unichar *table;
    uns len;
    uns start;
};

struct font {
    struct font *next;
    uns obj;
    struct fencoding *enc;
};

struct fontname {
    struct fontname *next;
    uns obj;
    uns namehash;
    byte *name;
    struct fencoding *enc;
};

struct resources {
    struct resources *next;
    uns obj;
    struct fontname *fonts;
    struct gsname *gstates;
};

/* Main module */

extern struct mempool *page_pool;	/* Page-local stuff; discarded in out_pages() */
extern struct mempool *global_pool;	/* Global stuff; discarded at the end of parsing */
extern struct mempool *sf_pool;		/* Stream functions' data */

/* adobeencs.c: standard font encodings */

extern struct fencoding StandardEncoding, MacRomanEncoding, WinAnsiEncoding, PDFDocEncoding, MacExpertEncoding, SymbolEncoding;

/* lex.c: lexical analysis and low-level parsing */

extern ucw_off_t pdf_filesize;
extern struct fastbuf *pdf_in, *pdf_stream_in;
extern s32 pdf_rootref, pdf_inforef;

void pdf_setup(void);
void set_input_method(struct fastbuf *inf);
void set_input_method_raw(struct fastbuf *inf);
int ingetc(struct fastbuf *in);
int is_space(int c, uns type);
int skip_space(uns type);
int in_check_nschar(int spacetype);
int pdf_check_nschar(int spacetype);
void pdf_seek(ucw_off_t pos);
void savefilepos(void);
void restorefilepos(void);
int in_get_line(char * buf);
uns hexbyte(uns hi, uns lo);
OBJECT get_obj(void);
OBJECT get_i_obj(s32 n);
OBJECT get_tok(void);
void printobj(OBJECT obj);
void parse_dict(DICT_ENTRY *dict_start,OBJ_GET *items);

#define pdf_get_char()	    ingetc(pdf_in)
#define pdf_check_char()    bpeekc(pdf_in)
#define pdf_unget_char()    bungetc(pdf_in)
#define pdf_read(b,l)	    bread(pdf_in,b,l)
#define pdf_tell()	    ((int)btell(pdf_in))

static inline void
obj_deref(OBJECT *obj) {
    if(obj->type==OT_OBJREF)
	*obj=get_i_obj(obj->value.n);
}

static inline s32
obj_int_real(OBJECT obj) {
    if(obj.type==OT_INT)
	return obj.value.n;
    if(obj.type==OT_REAL)
	return obj.value.r;
    return 0;
}

/* stream.c: analysis of streams */

extern uns stream_array[MAX_STREAMS+1];

void stream_init(struct fastbuf *f);
int check_i_stream(s32 n);

/* crypt.c: encrypted documents */

struct rc4_state {
    byte x;
    byte y;
    byte m[256];
};

extern int encrypted;			/* bit 0: decrypt now; bit 1: document is encrypted */
extern byte decryptkey[21];
extern uns decryptkey_length;
extern struct rc4_state rc4_state, rc4_state_stream;

void rc4_setup(struct rc4_state *s, u32 obj, u32 gen);
int rc4_conv(struct rc4_state *rs, void *dest, void *src, u32 n);
void copy_and_decrypt(void *dest, void *src, uns n);
void decrypt_init(OBJECT encrypt, OBJECT fileid);

/* fonts.c: fonts and encodings */

struct fencoding *parse_fontenc(uns fobj);

/*
 * This range disallow Adobe additional Unicode Values too
 * http://partners.adobe.com/asn/developer/type/corporateuse.txt
 */
static inline int
in_unirange(int u) {
    return ((u>=0x20 && u<0xe000) || (u>=0xf900 && u<=0xffff));
}

extern u16 *unistr[UNICODESTRINGCNT];
extern int unistrp;
