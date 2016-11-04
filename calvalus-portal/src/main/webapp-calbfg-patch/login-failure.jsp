<%@ page import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<html>
<head>
    <title>Calvalus Login</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
</head>
<body>

<table style="width: 99%; border: 0;" align="center">
    <tr>
        <td>
            <hr/>
            <table  style="width: 100%;"class="headerPanel">
                <tr>
                    <td>
                        <a href="http://www.brockmann-consult.de/"><img src="/calbfg/images/bfg_logoFL1_A4_rgb.jpg" width="200" alt="BfG logo"/></a>
                    </td>
                    <td>
                        <h1 class="title">WasMon-CT</h1>

                        <h2 class="subTitle">Calvalus portal for on-demand processing</h2>
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

            <p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; 2016 Brockmann Consult GmbH &nbsp;-&nbsp;<a
                    href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

        </td>
    </tr>
</table>


</body>
</html>
