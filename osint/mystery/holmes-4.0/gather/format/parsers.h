/*
 *	Sherlock Gatherer: List of known parsers
 */

P(sink)
P(text)
P(html)
P(gzip)
P(zip)
P(deflate)
P(compress)
P(robots)
P(external)
#ifdef CONFIG_IMAGES
  P(image)
#endif
#ifdef CONFIG_PDF
  P(pdf)
#endif
#ifdef CONFIG_MSWORD
  P(msword)
#endif
#ifdef CONFIG_EXCEL
  P(excel)
#endif
#ifdef CONFIG_MP3
  P(mp3)
#endif
#ifdef CONFIG_OGG
  P(ogg)
#endif
#ifdef CONFIG_WML
  P(wml)
#endif
