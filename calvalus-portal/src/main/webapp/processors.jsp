<%--
  ~ Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de) 
  ~
  ~ This program is free software; you can redistribute it and/or modify it 
  ~ under the terms of the GNU General Public License as published by the Free
  ~ Software Foundation; either version 3 of the License, or (at your option)
  ~ any later version.
  ~ This program is distributed in the hope that it will be useful, but WITHOUT
  ~ ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
  ~ FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for 
  ~ more details.
  ~
  ~ You should have received a copy of the GNU General Public License along 
  ~ with this program; if not, see http://www.gnu.org/licenses/
  --%>

<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.bc.calvalus.production.ServiceContainer" %>
<%@ page import="com.bc.calvalus.processing.BundleDescriptor" %>
<%@ page import="com.bc.calvalus.commons.shared.BundleFilter" %>
<%@ page import="com.bc.calvalus.production.ProductionException" %>
<%@ page import="java.io.PrintWriter" %>
<%@ page import="com.bc.calvalus.processing.ProcessorDescriptor" %>
<%@ page import="java.security.Principal" %>
<%@ page import="java.util.Map" %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="java.util.Comparator" %>
<%@ page import="com.bc.calvalus.portal.server.BackendServiceImpl" %>


<html>
<head>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <title>Available processors</title>
</head>
<body>
<h1>Available system processors</h1>
<table border="1" cellpadding="5">
    <thead>
    <tr>
        <th>Bundle</th>
        <th>Processor</th>
        <th>Input</th>
        <th>Output</th>
        <th>Job Properties</th>
    </tr>
    </thead>
    <tbody>
    <%
        ServletContext sc = request.getServletContext();
        ServiceContainer serviceContainer = (ServiceContainer) sc.getAttribute("serviceContainer");
        final BundleFilter systemFilter = new BundleFilter();
        systemFilter.withProvider(BundleFilter.PROVIDER_SYSTEM);
        final BundleDescriptor[] bundleDescriptors;
        try {
            final Principal userPrincipal = request.getUserPrincipal();
            if (userPrincipal != null && serviceContainer != null) {
                String userName = userPrincipal.getName();
                bundleDescriptors = serviceContainer.getProductionService().getBundles(userName, systemFilter);
                for (BundleDescriptor bundle : bundleDescriptors) {
                    ProcessorDescriptor[] processorDescriptors = bundle.getProcessorDescriptors();
                    if (processorDescriptors != null) {

                        String bundleName = bundle.getBundleName();
                        String bundleVersion = bundle.getBundleVersion();
                        String bundleText = bundleName + " - " + bundleVersion;
                        String bundleLocation = bundle.getBundleLocation();
                        if (!bundleLocation.endsWith("/" + bundle.getBundleName() + "-" + bundle.getBundleVersion())) {
                            String bundleLocShort = bundleLocation.substring(bundleLocation.lastIndexOf('/') + 1); 
                            bundleText += "<br/><b>WARNING: bundle names and version don't match directory:</b><br/>" +
                                    bundleLocShort;
                        }
                        for (int i = 0; i < processorDescriptors.length; i++) {
                            ProcessorDescriptor processor = processorDescriptors[i];
                            String pExeName = processor.getExecutableName();
                            String pVersion = processor.getProcessorVersion();
                            String pDesc = processor.getProcessorName().replaceAll(" ", "&nbsp;");
                            String processorText = pExeName + " (v " +pVersion+")<br/>"+pDesc;
                            Map<String, String> jobConfiguration = processor.getJobConfiguration();
                            ArrayList<String> keys = new ArrayList<>(jobConfiguration.keySet());
                            keys.sort(new Comparator<String>() {
                                @Override
                                public int compare(String s, String anotherString) {
                                    return s.compareTo(anotherString);
                                }
                            });
                            String input = "";
                            if (processor.getInputProductTypes() != null && processor.getInputProductTypes().length > 0) {
                                input = String.join("<br/>", processor.getInputProductTypes());
                            }
                            String output = "";
                            if (processor.getOutputProductType() != null) {
                                output = processor.getOutputProductType();
                            }
                            StringBuilder jobProperties = new StringBuilder();
                            if (keys.size() > 0) {
                                jobProperties.append("<table>");
                                for (String key : keys) {
                                    String value = jobConfiguration.get(key).replaceAll("<", "&lt;").replaceAll(">", "&gt;").trim();
                                    if (value.startsWith("&lt;")) {
                                        value = "<pre>"+value+"</pre";
                                    }
                                    jobProperties.append("<tr>");
                                    jobProperties.append("<td>").append(key).append("</td>");
                                    jobProperties.append("<td>&nbsp;=&nbsp;</td>");
                                    jobProperties.append("<td>").append(value).append("</td>");
                                    jobProperties.append("</tr>");
                                }
                                jobProperties.append("</table>");
                            }
                            
    %>
    <tr>
        <%
            if (i == 0) {
        %>
        <td rowspan="<%=processorDescriptors.length %>"><%=bundleText %></td>
        <%
            }
        %>
        <td><%=processorText %></td>
        <td><%=input %></td>
        <td><%=output %></td>
        <td><%=jobProperties.toString() %></td>
    </tr>
    <%
                        }
                    }
                }
            }
        } catch (ProductionException e) {
            e.printStackTrace(new PrintWriter(out));
        }
    %>
    </tbody>
</table>

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
