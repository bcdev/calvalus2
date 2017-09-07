<!doctype html>

<%@page import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<%@ page import="java.security.Principal" %>

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
<div id="container">
    <div class="header">
        <div class="header-logo">
            <img src="images/esa-logo-white.png" alt="ESA logo"/>
        </div>
        <div class="header-title">
            <h1 class="title">Calvalus</h1>
            <h2 class="subTitle">Portal for Earth Observation Cal/Val and User Services</h2>
        </div>
        <div class="header-info">
            <div class="header-info-help">
                <a href="http://www.brockmann-consult.de/beam-wiki/x/W4C8Aw" target="_CVHelp"
                   style="text-decoration:none;">
                    <span class="header-info-text">HELP</span>
                </a>
            </div>
            <div class="header-info-about">
                <a href="about.jsp" target="_blank" class="header-info-about" style="text-decoration:none;">
                    <span class="header-info-text">ABOUT</span>
                </a>
            </div>
            <div class="header-info-user">
                <% final Principal userPrincipal = request.getUserPrincipal(); %>
                <% if (userPrincipal != null) { %>
                USER
                <b>
                    <%=userPrincipal.getName()%>
                </b>
                <br/>
                <a href='<%= response.encodeURL("logout.jsp") %>' style="text-decoration: none">
                    <span class="header-user-logout">LOG OUT</span>
                </a>
                <% } else { %>
                Not logged in.
                <% } %>
            </div>
        </div>
    </div>

    <div id="mainPanel"></div>

    <div class="footer">
        <div class="footer-copyright">
            <div class="footer-calvalus-version">
                <%= BackendServiceImpl.VERSION %>
            </div>
            <div class="footer-copyright-text">
                &#169; <%= BackendServiceImpl.COPYRIGHT_YEAR %> Brockmann Consult GmbH
            </div>
        </div>
        <div class="footer-info">
            <div class="footer-info-impressum">
                <a href="http://www.brockmann-consult.de/bc-web/impressum.html"
                   target="_blank" style="text-decoration:none;">
                    <span class="footer-info-text">Impressum</span>
                </a>
            </div>
            <div class="footer-info-help">
                <a href="http://www.brockmann-consult.de/beam-wiki/x/W4C8Aw" target="_CVHelp"
                   style="text-decoration:none;">
                    <span class="footer-info-text">Help</span>
                </a>
            </div>
            <div class="footer-info-about">
                <a href="about.jsp" target="_blank" class="header-info-about" style="text-decoration:none;">
                    <span class="footer-info-text">About</span>
                </a>
            </div>
        </div>
    </div>
</div>
<div id="splashScreen">
    Loading Calvalus portal, please wait...<br/><br/>
    <img src="images/progress-bar.gif"/>
</div>
</body>
</html>
