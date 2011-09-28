<%@ page import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<html>
<head>
    <title>Calvalus Login</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
</head>
<body>

<table class="headerPanel">
    <tr>
        <td>
            <img src="images/esa-logo.jpg" alt="ESA logo"/>
        </td>
        <td>
            <h1 class="title">Calvalus</h1>

            <h2 class="subTitle">Portal for Earth Observation Cal/Val and User Services</h2>
        </td>
    </tr>
</table>
<hr/>

<div class="loginPanel">
    Invalid user name and/or password,</br>
    please try <a href='<%= response.encodeURL("calvalus.jsp") %>'>again</a>.
</div>

<p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; 2011 Brockmann Consult GmbH &nbsp;-&nbsp;<a href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

</body>
</html>
