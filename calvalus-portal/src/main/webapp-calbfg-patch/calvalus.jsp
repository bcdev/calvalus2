<!doctype html>

<%@page language="java" import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<%@ page import="java.security.Principal" %>

<html>
<head>
    <title>WasMon-CT Calvalus</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <script type="text/javascript" language="javascript" src="calvalus/calvalus.nocache.js"></script>

    <%-- Reanimate following line for OpenLayers support --%>
    <%--<script type="text/javascript" language="javascript" src="http://openlayers.org/api/2.9/OpenLayers.js"></script>--%>
</head>

<body>

<iframe src="javascript:''" id="__gwt_historyFrame" tabIndex='-1'
        style="position:absolute;width:0;height:0;border:0"></iframe>

<noscript>
    <div style="width: 22em; position: absolute; left: 50%; margin-left: -11em; color: red; background-color: white; border: 1px solid red; padding: 4px; font-family: sans-serif">
        Your web browser must have JavaScript enabled
        in order for <b>Calvalus</b> to display correctly.
    </div>
</noscript>

<table style="width: 99%; border: 0;" align="center">
    <tr>
        <td>
            <hr/>
            <table style="width: 100%;">
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
                        <% final Principal userPrincipal = request.getUserPrincipal(); %>
                        <% if (userPrincipal != null) { %>
                        User <b><%=userPrincipal.getName()%>
                    </b>
                        <br/>
                        <a href='<%= response.encodeURL("logout.jsp") %>'>Log Out</a>
                        <% } else { %>
                        Not logged in.
                        <% } %>
                        <br/>
                        <a href="http://www.brockmann-consult.de/beam-wiki/x/W4C8Aw" target="_CVHelp" onclick="window.open('http://www.brockmann-consult.de/beam-wiki/x/W4C8Aw','_CVHelp','width=800,height=600'); return false;">Help</a>
                        <br/>
                        <a href="about.jsp" target="_blank">About</a>
                    </td>
                </tr>
            </table>
            <hr/>
        </td>
    </tr>
</table>

<div id="mainPanel"></div>

<div id="splashScreen">
    Loading Calvalus portal, please wait...<br/><br/>
    <img src="images/progress-bar.gif"/>
</div>

<table style="width: 99%; border: 0;" align="center">
    <tr>
        <td>
            <p class="copyrightApp"><%= BackendServiceImpl.VERSION %>, &#169; <%= BackendServiceImpl.COPYRIGHT_YEAR %> Brockmann Consult GmbH
                &nbsp;-&nbsp;<a href="http://www.brockmann-consult.de/bc-web/impressum.html"
                                target="_blank">Impressum</a></p>
        </td>
    </tr>
</table>

</body>
</html>
