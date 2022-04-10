# Tests of the HTML parser

Run:	bin/parser-test -ctext/html
In:	<body>123<script>this is a script</script>456
Out:	<text!>123<text!>456

In:	<body>123<script>this is <!-- </em>a script</script>456
Out:	<text!>123<text!>456

In:	<body>123<script>this is <!-- <script>hoot!</em></script>a script</script>456
Out:	<text!>123<text!>456
