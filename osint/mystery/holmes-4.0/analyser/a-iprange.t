# Tests for iprange analyser

Run:	(../obj/analyser/atest -C/dev/null -S 'Analyser { HookChewer iprange needed }; IPRanges { Attr=9; Alias=A1 0x12345678; }' -h chewer | grep '^9')
In:	Ux:x
	k12345678
	Xhdsajsaahojdsadasjkdsa
Out:	9A1

Run:	(../obj/analyser/atest -C/dev/null -S 'Analyser { HookChewer iprange needed }; IPRanges { Attr=9; Alias=A1 0x12345678; Alias=A2 0x12345678-0x12345999;}' -h chewer | grep '^9')
In:	Ux:x
	k12345678
	Xhdsajsaahojdsadasjkdsa
Out:	9A1
	9A2

Run:	(../obj/analyser/atest -C/dev/null -S 'Analyser { HookChewer iprange needed }; IPRanges { Attr=9; Alias=A1 0x12345678; Alias=A2 0x12345678-0x12345999;}' -h chewer | grep '^9')
In:	Ux:x
	k12345999
	Xhdsajsaahojdsadasjkdsa
Out:	9A2

