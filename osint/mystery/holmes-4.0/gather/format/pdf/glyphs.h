/*
 *  PDF parser
 *
 *  (c) 2002 Milan Vancura <milan@ucw.cz>
 */

struct glyph {
  char *name;
  unsigned int unival;
};

const struct glyph *is_glyph(const char *, unsigned int);

