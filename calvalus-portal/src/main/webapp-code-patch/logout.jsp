<html>
<head>
    <title>Calvalus Logout</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
</head>
<body>

<%
    session.invalidate();
    response.sendRedirect("https://tsedos.eoc.dlr.de/cas-codede/logout?service=https://processing.code-de.org/");
%>

</body>
</html>
