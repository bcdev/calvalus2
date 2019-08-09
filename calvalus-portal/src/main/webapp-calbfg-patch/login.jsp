<?xml version="1.0" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<%@ page import="com.bc.calvalus.portal.server.BackendServiceImpl" %>

<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <title>Calvalus Login</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
</head>
<body onload='document.login.submitBtn.focus()'>

<table style="width: 820px; border: 0;" align="center">
    <tr>
        <td>
            <hr/>
            <table class="headerPanel">
                <tr>
                    <td>
                        <a href="http://www.bafg.de/DE/Home/homepage_node.html/"><img src="images/bfg_logoFL1_A4_rgb.jpg" height="65" alt="BfG logo"></a>
                    </td>
                    <td>
                        <h1 class="title">WasMon-CT</h1>

                        <h2 class="subTitle">Portal for Calvalus on-demand processing</h2>
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

            <div class="loginPanelWrapper">
                <div class="loginPanel">
                    <form name="login" method="POST" action='<%= response.encodeURL("j_security_check") %>'>
                        <table border="0" cellspacing="5">
                            <tr>
                                <th align="right"><label for="j_username">Username:</label></th>
                                <td align="left">
                                    <input type="text" id="j_username" name="j_username">
                                </td>
                            </tr>
                            <tr>
                                <th align="right"><label for="j_password">Password:</label></th>
                                <td align="left"><input type="password" id="j_password" name="j_password"></td>
                            </tr>
                            <tr>
                                <td></td>
                                <td align="right"><input name="submitBtn" type="submit" value="Log In"></td>
                            </tr>
                        </table>
                    </form>
                </div>
            </div>

            <p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; <%= BackendServiceImpl.COPYRIGHT_YEAR %> Brockmann Consult GmbH &nbsp;-&nbsp;<a
                    href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

        </td>
    </tr>
</table>



</body>
</html>
