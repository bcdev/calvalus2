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
        <td class="userName">
            <a href="about.jsp" target="_blank">About Calvalus</a>
        </td>
    </tr>
</table>
<hr/>


<div class="loginPanelWrapper">
  <div class="loginPanel">
    <form method="POST" action='<%= response.encodeURL("j_security_check") %>'>
        <table border="0" cellspacing="5">
            <tr>
                <th align="right">Username:</th>
                <td align="left"><input type="text" name="j_username"></td>
            </tr>
            <tr>
                <th align="right">Password:</th>
                <td align="left"><input type="password" name="j_password"></td>
            </tr>
            <tr>
                <td></td>
                <td align="right"><input type="submit" value="Log In"></td>
            </tr>
        </table>
    </form>
  </div>
</div>

<p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; 2011 Brockmann Consult GmbH &nbsp;-&nbsp;<a href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

</body>
</html>
