<%@ page import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<html>
<head>
    <title>Calvalus Login</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
</head>
<body>

<table style="width: 820px; border: 0;" align="center">
    <tr>
        <td>
            <hr/>
            <table class="headerPanel">
                <tr>
                    <td width="200">
                        <a href="http://feeder.calfin.fmi.fi/"><img src="/calfin/images/fmi-logo.png" width="200" alt="FMI logo"/><br/><img src="/calfin/images/syke-logo.png" height="60" alt="SYKE logo"/></a>
                    </td>
                    <td>
                        <h1 class="title">Calvalus</h1>

                        <h2 class="subTitle">Finnish portal for on-demand processing</h2>
                    </td>
                    <td class="userName">
                        <a href="about.jsp">About</a>
                    </td>
                </tr>
            </table>
            <hr/>

            <div class="loginPanel">
                Invalid user name and/or password,</br>
                please try <a href='<%= response.encodeURL("calvalus.jsp") %>'>again</a>.
            </div>

            <p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; <%= BackendServiceImpl.COPYRIGHT_YEAR %> Brockmann Consult GmbH &nbsp;-&nbsp;<a
                    href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

        </td>
    </tr>
</table>


</body>
</html>
