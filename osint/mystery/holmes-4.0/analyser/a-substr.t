# Tests for substr analyser

Run:	(../obj/analyser/atest -C/dev/null -S 'Analyser { HookScanner substr needed ctx }; SubStrings { Context { Name=ctx; Attr=8; Parts=text; Groups=grp1 0x1 grp2 0x2 grp4 0x4; }; Group { Name=grp1; Search=ahoj; }; Group { Name=grp2; Search=aho; }; Group { Name=grp4; Search=cau; } }' -h scanner | grep '^8')
In:	Ux:x
	Xhdsajsaahojdsadasjkdsa
Out:	83

Run:	(../obj/analyser/atest -C/dev/null -S 'Analyser { HookGather substr needed ctx }; SubStrings { Context { Name=ctx; Attr=8; Parts=text metas; Groups=grp1 0x1 grp2 0x2 grp4 0x4; }; Group { Name=grp1; Search=ahoj; }; Group { Name=grp2; Search=aho; }; Group { Name=grp4; Search=cau; } }' -h gather | grep '^8')
In:	Ux:x
	Xhdsajsaahoojdsadasjkdcausa
Out:	86

Run:	(../obj/analyser/atest -C/dev/null -S 'Analyser { HookScanner substr needed ctx }; SubStrings { Context { Name=ctx; Attr=8; Parts=text metas urls; Groups=grp1 0x1 grp2 0x2 grp4 0x4; }; Group { Name=grp1; Search=ahoj; }; Group { Name=grp2; Search=aho; }; Group { Name=grp4; Search=cau; } }' -h scanner | grep '^8')
In:	Ux:x
	Xhdsajsaahhoojdsadasjkdcaausa
Out:	80

