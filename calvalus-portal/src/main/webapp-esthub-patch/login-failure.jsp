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
                    <td>
                        <img src="images/0_maaamet_vapp_eng_78px_resize.png" alt="Maamet logo"/>
                    </td>
                    <td>
                        <h1 class="title">ESTHub Processing Platform</h1>

                        <h2 class="subTitle">Portal for Earth observation data processing</h2>
                    </td>
                </tr>
            </table>
            <hr/>

            <div class="loginPanel">
                Invalid user name and/or password,</br>
                please try <a href='<%= response.encodeURL("calvalus.jsp") %>'>again</a>.
            </div>

            <p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; <%= BackendServiceImpl.COPYRIGHT_YEAR %> Brockmann Consult GmbH &nbsp;-&nbsp;<a
                    href="https://web.brockmann-consult.de/imprint/" target="_blank">Imprint</a></p>
        </td>
    </tr>
</table>


</body>
</html>
