<!doctype html>

<%@page import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<%@ page import="java.security.Principal" %>

<html>
<head>
    <title>ESTHub Processing Platform<</title>
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
<div id="container" style="height: 100vh">
    <div class="header-panel">
        <div class="header-logo">
            <a href="https://www.maaamet.ee/">
                <img src="images/0_maaamet_vapp_eng_78px_resize.png" alt="Maamet logo"/>
            </a>
        </div>
        <div class="header-title">
            Processing Service
        </div>
        <div class="header-info">
            <div class="header-info-user">
                <% final Principal userPrincipal = request.getUserPrincipal(); %>
                <% if (userPrincipal != null) { %>
                <span class="header-username-text"><%=userPrincipal.getName()%></span>
                <% } %>
            </div>
            <div class="header-info-help">
                <a href="https://esthub.maaamet.ee/help" target="_CVHelp"
                   style="text-decoration:none;">
                    <span class="header-info-text">HELP</span>
                </a>
            </div>
            <div class="header-user-logout">
                <% if (userPrincipal != null) { %>
                <a href='<%= response.encodeURL("logout.jsp") %>' style="text-decoration: none">
                    <span class="header-info-text">LOG OUT</span>
                </a>
                <% } else { %>
                <span class="header-info-text">LOG IN</span>
                <% } %>
            </div>
        </div>
    </div>

    <div id="mainPanel" class="main-panel"></div>

    <div class="footer">
        <div class="footer-left">
            <div class="footer-calvalus-version">
                <%= BackendServiceImpl.VERSION %>
            </div>
            <div class="footer-copyright-legal-privacy">
                <div class="footer-copyright">
                    <span class="copyright-text">Copyright &#169; 2018 ESTHub</span>
                </div>
                <div class="footer-legal">
                    <a href="https://esthub.maaamet.ee/en/legal_notice" target="_blank" style="text-decoration:none;">
                        <span class="legal-privacy-text">Legal Notice</span>
                    </a>
                </div>
                <div class="footer-privacy">
                    <a href="https://esthub.maaamet.ee/en/data_protection" target="_blank" style="text-decoration:none;">
                        <span class="legal-privacy-text">Privacy Statement</span>
                    </a>
                </div>
                <div class="footer-empty"></div>
            </div>
        </div>
        <div class="footer-info">
            <div class="footer-info-about">
                <a href="https://esthub.maaamet.ee"
                   target="_blank" style="text-decoration:none;">
                    <span class="footer-info-text">About</span>
                </a>
            </div>
            <div class="footer-info-help">
                <a href="https://esthub.maaamet.ee" target="_CVHelp"
                   style="text-decoration:none;">
                    <span class="footer-info-text">Help</span>
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
