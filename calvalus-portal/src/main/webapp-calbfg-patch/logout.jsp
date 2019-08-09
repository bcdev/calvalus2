<%@ page import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<html>
<head>
    <title>Calvalus Logout</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
</head>
<body>

<table style="width: 99%; border: 0;" align="center">
    <tr>
        <td>
            <hr/>
            <table style="width: 100%" class="headerPanel">
                <tr>
                    <td>
                        <a href="http://www.bafg.de/DE/Home/homepage_node.html/"><img src="images/bfg_logoFL1_A4_rgb.jpg" height="65" alt="BfG logo"></a>
                    </td>
                    <td>
                        <h1 class="title">WasMon-CT</h1>

                        <h2 class="subTitle">Calvalus portal for on-demand processing</h2>
                    </td>
                    <td>
                        <a href="http://www.brockmann-consult.de/"><img src="images/BC-Logo_300dpi.png" height="48" alt="BC logo"></a>
                    </td>
                    <td class="userName">
                        <a href="about.jsp">About</a>
                    </td>
                </tr>
            </table>
            <hr/>

            <%@ page session="true" %>

            <div class="loginPanel">

                User <strong><%=request.getRemoteUser()%></strong> has been logged out.

                <% session.invalidate(); %>

                <br/><br/>
                <a href='<%= response.encodeURL("calvalus.jsp") %>'>Click here to login again.</a>
                <br/><br/>

            </div>

            <p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; <%= BackendServiceImpl.COPYRIGHT_YEAR %> Brockmann Consult GmbH &nbsp;-&nbsp;<a
                    href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

        </td>
    </tr>
</table>


</body>
</html>
