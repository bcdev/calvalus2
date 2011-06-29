<%@ taglib prefix="bean" uri="http://java.sun.com/jsp/jstl/fmt" %>
<!doctype html>

<%@page language="java" import="java.security.Principal" %>

<html>
<head>
    <title>Calvalus Portal</title>
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
            <% final Principal userPrincipal = request.getUserPrincipal(); %>
            <% if (userPrincipal != null) { %>
            User <b><%=userPrincipal.getName()%></b>
            <% } else { %>
            Not logged in.
            <% } %>
            <br/>
            <a href="about.jsp" target="_blank">About Calvalus</a>
        </td>
    </tr>
</table>
<hr/>

<div id="mainPanel"></div>

<div id="splashScreen">
    Loading Calvalus portal, please wait...<br/><br/>
    <img src="images/progress-bar.gif"/>
</div>

<p class="copyright">Calvalus - Version 0.3, &#169; 2011 Brockmann Consult GmbH &nbsp;-&nbsp;<a href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

</body>
</html>
