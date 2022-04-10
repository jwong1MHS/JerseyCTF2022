/*
 * output.c
 * Copyright (C) 2002,2003 A.J. van Os; Released under GPL
 *
 * Description:
 * Generic output generating functions
 */

#include "sherlock/sherlock.h"
#include "antiword.h"

static conversion_type	eConversionType = conversion_unknown;
static encoding_type	eEncoding = encoding_neutral;

/*
 * vPrologue1 - get options and call a specific initialization
 */
static void
vPrologue1(diagram_type *pDiag, const char *szTask UNUSED, const char *szFilename UNUSED)
{
	options_type	tOptions;

	fail(pDiag == NULL);
	fail(szTask == NULL || szTask[0] == '\0');

	vGetOptions(&tOptions);
	eConversionType = tOptions.eConversionType;
	eEncoding = tOptions.eEncoding;

	switch (eConversionType) {
	case conversion_text:
		vPrologueTXT(pDiag, &tOptions);
		break;
#if 0
	case conversion_ps:
		vProloguePS(pDiag, szTask, szFilename, &tOptions);
		break;
	case conversion_xml:
		vPrologueXML(pDiag);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vPrologue1 */

/*
 * vEpilogue - clean up after everything is done
 */
static void
vEpilogue(diagram_type *pDiag)
{
	switch (eConversionType) {
	case conversion_text:
		vEpilogueTXT(pDiag->pOutFile);
		break;
#if 0
	case conversion_ps:
		vEpiloguePS(pDiag);
		break;
	case conversion_xml:
		vEpilogueXML(pDiag);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vEpilogue */

/*
 * vImagePrologue - perform image initialization
 */
void
vImagePrologue(diagram_type *pDiagp UNUSED, const imagedata_type *pImg UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		vImageProloguePS(pDiag, pImg);
		break;
	case conversion_xml:
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vImagePrologue */

/*
 * vImageEpilogue - clean up an image
 */
void
vImageEpilogue(diagram_type *pDiag UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		vImageEpiloguePS(pDiag);
		break;
	case conversion_xml:
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vImageEpilogue */

/*
 * bAddDummyImage - add a dummy image
 *
 * return TRUE when successful, otherwise FALSE
 */
BOOL
bAddDummyImage(diagram_type *pDiag UNUSED, const imagedata_type *pImg UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		return FALSE;
#if 0
	case conversion_ps:
		return bAddDummyImagePS(pDiag, pImg);
	case conversion_xml:
		return FALSE;
#endif
	default:
		DBG_DEC(eConversionType);
		return FALSE;
	}
} /* end of bAddDummyImage */

/*
 * pCreateDiagram - create and initialize a diagram
 *
 * remark: does not return if the diagram can't be created
 */
diagram_type *
pCreateDiagram(struct fastbuf *outbuf)
{
	diagram_type	*pDiag;

	DBG_MSG("pCreateDiagram");

	/* Get the necessary memory */
	pDiag = xmalloc(sizeof(diagram_type));
	/* Initialization */
	pDiag->pOutFile = outbuf;

	vPrologue1(pDiag, "not-used", "not-used");
	/* Return success */
	return pDiag;
} /* end of pCreateDiagram */

/*
 * vDestroyDiagram - remove a diagram by freeing the memory it uses
 */
void
vDestroyDiagram(diagram_type *pDiag)
{
	DBG_MSG("vDestroyDiagram");

	fail(pDiag == NULL);

	if (pDiag == NULL) {
		return;
	}
	vEpilogue(pDiag);
	pDiag = xfree(pDiag);
} /* end of vDestroyDiagram */

/*
 * vPrologue2 - call a specific initialization
 */
void
vPrologue2(diagram_type *pDiag UNUSED, int iWordVersion UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		vAddFontsPS(pDiag);
		break;
	case conversion_xml:
		vCreateBookIntro(pDiag, iWordVersion, eEncoding);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vPrologue2 */

/*
 * vMove2NextLine - move to the next line
 */
void
vMove2NextLine(diagram_type *pDiag, draw_fontref tFontRef UNUSED, USHORT usFontSize UNUSED)
{
	fail(pDiag == NULL);
	fail(pDiag->pOutFile == NULL);
	fail(usFontSize < MIN_FONT_SIZE || usFontSize > MAX_FONT_SIZE);

	switch (eConversionType) {
	case conversion_text:
		vMove2NextLineTXT(pDiag);
		break;
#if 0
	case conversion_ps:
		vMove2NextLinePS(pDiag, usFontSize);
		break;
	case conversion_xml:
		vMove2NextLineXML(pDiag);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vMove2NextLine */

/*
 * vSubstring2Diagram - put a sub string into a diagram
 */
void
vSubstring2Diagram(diagram_type *pDiag,
	char *szString, size_t tStringLength, long lStringWidth,
	UCHAR ucFontColor UNUSED, USHORT usFontstyle UNUSED, draw_fontref tFontRef UNUSED,
	USHORT usFontSize UNUSED, USHORT usMaxFontSize UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		vSubstringTXT(pDiag, szString, tStringLength, lStringWidth);
		break;
#if 0
	case conversion_ps:
		vSubstringPS(pDiag, szString, tStringLength, lStringWidth,
				ucFontColor, usFontstyle, tFontRef,
				usFontSize, usMaxFontSize);
		break;
	case conversion_xml:
		vSubstringXML(pDiag, szString, tStringLength, lStringWidth,
				usFontstyle);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
	pDiag->lXleft += lStringWidth;
} /* end of vSubstring2Diagram */

/*
 * Create a start of paragraph (phase 1)
 * Before indentation, list numbering, bullets etc.
 */
void
vStartOfParagraph1(diagram_type *pDiag, long lBeforeIndentation)
{
	fail(pDiag == NULL);

	switch (eConversionType) {
	case conversion_text:
		vStartOfParagraphTXT(pDiag, lBeforeIndentation);
		break;
#if 0
	case conversion_ps:
		vStartOfParagraphPS(pDiag, lBeforeIndentation);
		break;
	case conversion_xml:
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vStartOfParagraph1 */

/*
 * Create a start of paragraph (phase 2)
 * After indentation, list numbering, bullets etc.
 */
void
vStartOfParagraph2(diagram_type *pDiag UNUSED)
{
	fail(pDiag == NULL);

	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		break;
	case conversion_xml:
		vStartOfParagraphXML(pDiag, 1);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vStartOfParagraph2 */

/*
 * Create an end of paragraph
 */
void
vEndOfParagraph(diagram_type *pDiag,
	draw_fontref tFontRef UNUSED, USHORT usFontSize UNUSED, long lAfterIndentation)
{
	fail(pDiag == NULL);
	fail(pDiag->pOutFile == NULL);
	fail(usFontSize < MIN_FONT_SIZE || usFontSize > MAX_FONT_SIZE);
	fail(lAfterIndentation < 0);

	switch (eConversionType) {
	case conversion_text:
		vEndOfParagraphTXT(pDiag, lAfterIndentation);
		break;
#if 0
	case conversion_ps:
		vEndOfParagraphPS(pDiag,
				tFontRef, usFontSize, lAfterIndentation);
		break;
	case conversion_xml:
		vEndOfParagraphXML(pDiag, 1);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vEndOfParagraph */

/*
 * Create an end of page
 */
void
vEndOfPage(diagram_type *pDiag, long lAfterIndentation)
{
	switch (eConversionType) {
	case conversion_text:
		vEndOfPageTXT(pDiag, lAfterIndentation);
		break;
#if 0
	case conversion_ps:
		vEndOfPagePS(pDiag);
		break;
	case conversion_xml:
		vEndOfPageXML(pDiag);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vEndOfPage */

/*
 * vSetHeaders - set the headers
 */
void
vSetHeaders(diagram_type *pDiag UNUSED, USHORT usIstd UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		break;
	case conversion_xml:
		vSetHeadersXML(pDiag, usIstd);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vSetHeaders */

/*
 * Create a start of list
 */
void
vStartOfList(diagram_type *pDiag UNUSED, UCHAR ucNFC UNUSED, BOOL bIsEndOfTable UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		break;
	case conversion_xml:
		vStartOfListXML(pDiag, ucNFC, bIsEndOfTable);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vStartOfList */

/*
 * Create an end of list
 */
void
vEndOfList(diagram_type *pDiag UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		break;
	case conversion_xml:
		vEndOfListXML(pDiag);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vEndOfList */

/*
 * Create a start of a list item
 */
void
vStartOfListItem(diagram_type *pDiag UNUSED, BOOL bNoMarks UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		break;
	case conversion_xml:
		vStartOfListItemXML(pDiag, bNoMarks);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vStartOfListItem */

/*
 * Create an end of a table
 */
void
vEndOfTable(diagram_type *pDiag UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		break;
	case conversion_xml:
		vEndOfTableXML(pDiag);
		break;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
} /* end of vEndOfTable */

/*
 * Add a table row
 *
 * Returns TRUE when conversion type is XML
 */
BOOL
bAddTableRow(diagram_type *pDiag UNUSED, char **aszColTxt UNUSED,
	int iNbrOfColumns UNUSED, const short *asColumnWidth UNUSED, UCHAR ucBorderInfo UNUSED)
{
	switch (eConversionType) {
	case conversion_text:
		break;
#if 0
	case conversion_ps:
		break;
	case conversion_xml:
		vAddTableRowXML(pDiag, aszColTxt,
				iNbrOfColumns, asColumnWidth,
				ucBorderInfo);
		return TRUE;
#endif
	default:
		DBG_DEC(eConversionType);
		break;
	}
	return FALSE;
} /* end of bAddTableRow */
