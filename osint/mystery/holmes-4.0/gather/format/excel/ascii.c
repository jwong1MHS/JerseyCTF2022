#include "sherlock/sherlock.h"
#include "xlhtml.h"

extern int  center_tables;
extern int  first_sheet;
extern int  last_sheet;
extern int  sheet_count;
extern uni_string  default_font;
extern void trim_sheet_edges(unsigned int);
extern int  next_ws_title;
extern void update_default_font(unsigned int);
extern void OutputString(uni_string * );
extern int  default_fontsize;
extern char *default_alignment; 
extern int	aggressive;
extern char *lastUpdated; 
extern int  file_version;
extern int  NoFormat;
extern int  notAccurate;
extern int  formula_warnings;
extern int  NoHeaders;
extern int  NotImplemented;
extern int  Unsupported;
extern int  MaxWorksheetsExceeded;
extern int  MaxRowExceeded;
extern int  MaxColExceeded;
extern int  MaxStringsExceeded;
extern int  MaxFontsExceeded;
extern int  MaxPalExceeded;
extern int  MaxXFExceeded;
extern int  MaxFormatsExceeded;
extern char colorTab[MAX_COLORS];
extern char *default_text_color;
extern char *default_background_color;
extern char *default_image;
extern char filename[256];
extern int  UnicodeStrings;
extern int  CodePage; 
extern char	*title;
extern void update_default_alignment(unsigned int, int);
extern uni_string author;
extern int null_string(U8 *);
extern int Csv;
extern work_sheet **ws_array;
extern font_attr **font_array;
extern xf_attr **xf_array;

extern struct fastbuf *outstream;

extern int IsCellNumeric(cell *);
extern int IsCellSafe(cell *);
extern int IsCellFormula(cell *);
extern void output_formatted_data(uni_string *, U16, int, int);
extern void SetupExtraction(void);

void OutputPartialTableAscii(void);

void OutputPartialTableAscii(void)
{
  int i, j, k;
  int empty_cell, empty_row;

  HARD_MAX_ROWS=HARD_MAX_ROWS;
  SetupExtraction();
  first_sheet=0;
  last_sheet=sheet_count;

	/* Here's where we dump the Html Page out */
	for (i=first_sheet; i<=last_sheet; i++)	/* For each worksheet */
	{
		if (ws_array[i] == 0)
			continue;
		if ((ws_array[i]->biggest_row == -1)||(ws_array[i]->biggest_col == -1))
			continue;
		if (ws_array[i]->c_array == 0)
			continue;

      		/* Print sheet name */
		if (next_ws_title > 0)
		{
			if (ws_array[i]->ws_title.str)
			{
 			  bputs(outstream, "#### ");
       			  OutputString(&ws_array[i]->ws_title);
       			  bputs(outstream, " ####\n");
			}
			else
			  bputs(outstream, "#### ?? ####\n");
		}

		/* Now dump the table */
		for (j=ws_array[i]->first_row; j<=ws_array[i]->biggest_row; j++)
		{
		        empty_row= 1;

			for (k=ws_array[i]->first_col; k<=ws_array[i]->biggest_col; k++)
			{
				int safe, numeric=0;
				cell *c = ws_array[i]->c_array[(j*ws_array[i]->max_cols)+k]; /* This stuff happens for each cell... */
                                empty_cell= 0;
				if (c)
				{
					numeric = IsCellNumeric(c);
					if (!numeric && Csv)
						printf("\"");
					safe = IsCellSafe(c);

					if (c->ustr.str)
					{
					  if (safe)
					  {
					    output_formatted_data(&(c->ustr), xf_array[c->xfmt]->fmt_idx, numeric, IsCellFormula(c));
					  }
					  else
					  {
					    OutputString(&(c->ustr));
					  }
                                          bputc(outstream, ' ');
					}
        				else
         					empty_cell=1;   //  Empty cell...
				}
				else
				   empty_cell=1;	/* Empty cell... */

				if (ws_array[i]->c_array[(j*ws_array[i]->max_cols)+k])	/* Honor Column spanning ? */
				{
					if (ws_array[i]->c_array[(j*ws_array[i]->max_cols)+k]->colspan != 0)
						k += ws_array[i]->c_array[(j*ws_array[i]->max_cols)+k]->colspan-1;
				}
				if (!numeric && Csv)
					printf("\"");

				if (Csv && (k < ws_array[i]->biggest_col))
				{	/* big cheat here: quoting everything! */
					putchar(',');	/* Csv Cell Separator */
				}
				else
				{
				  if(( !Csv )&&( k != ws_array[i]->biggest_col ) && !empty_cell)
				  {
				    bputs(outstream, "| ");	/* Ascii Cell Separator */
                                    empty_row= 0;
				  }
				}
			}
       			if(!empty_row) bputc(outstream, 0x0A);		/* Row Separator */
		}
	}
}
