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
<%@ page import="java.util.List" %>
<%!
    static class BundleView {
        final String title;
        final List<ProcessorView> processorViews;

        BundleView(String title) {
            this.title = title;
            this.processorViews = new ArrayList<>();
        }
        
        int numRows() {
            int numRows = 0;
            for (ProcessorView processorView : processorViews) {
                numRows += processorView.numRows();
            }
            return numRows;
        }

        static BundleView create(BundleDescriptor bundle) {
            String bundleName = bundle.getBundleName();
            String bundleVersion = bundle.getBundleVersion();
            String bundleText = bundleName + " - " + bundleVersion;
            String bundleLocation = bundle.getBundleLocation();
            if (!bundleLocation.endsWith("/" + bundle.getBundleName() + "-" + bundle.getBundleVersion())) {
                String bundleLocShort = bundleLocation.substring(bundleLocation.lastIndexOf('/') + 1);
                bundleText += "<br/><b>WARNING: bundle names and version don't match directory:</b><br/>" +
                        bundleLocShort;
            }
            return new BundleView(bundleText);
        }
    }

    static class ProcessorView {
        final String title;
        final List<PropertyView> propertyViews;

        ProcessorView(String title, List<PropertyView> propertyViews) {
            this.title = title;
            this.propertyViews = propertyViews;
        }
        
        int numRows() {
            return propertyViews.size();
        }
        
        static ProcessorView create(ProcessorDescriptor processor) {
            String pExeName = processor.getExecutableName();
            String pVersion = processor.getProcessorVersion();
            String pDesc = processor.getProcessorName().replaceAll(" ", "&nbsp;");
            String processorText = pExeName + " (v " + pVersion + ")<br/>" + pDesc;
            return new ProcessorView(processorText, createPropertyViews(processor));
        }

        static List<PropertyView> createPropertyViews(ProcessorDescriptor processor) {
            List<PropertyView> propertyViews = new ArrayList<>();

            if (processor.getInputProductTypes() != null && processor.getInputProductTypes().length > 0) {
                String input = String.join(", ", processor.getInputProductTypes());
                propertyViews.add(new PropertyView("Input", input, true));
            }
            if (processor.getOutputProductType() != null) {
                String output = processor.getOutputProductType();
                propertyViews.add(new PropertyView("Output", output, true));
            }
            if (!processor.getOutputRegex().isEmpty()) {
                propertyViews.add(new PropertyView("Output RegEx", processor.getOutputRegex(), true));
            }
            if (processor.getFormatting() != null) {
                propertyViews.add(new PropertyView("Formatting", processor.getFormatting().toString(), true));
            }
            if (processor.getOutputVariables() != null) {
                ProcessorDescriptor.Variable[] outputVariables = processor.getOutputVariables();
                String[] bandNames = new String[outputVariables.length];
                for (int b = 0; b < outputVariables.length; b++) {
                    bandNames[b] = outputVariables[b].getName();
                }
                propertyViews.add(new PropertyView("Variables", String.join(", ", bandNames), true));
            }
            Map<String, String> jobConfiguration = processor.getJobConfiguration();
            ArrayList<String> keys = new ArrayList<>(jobConfiguration.keySet());
            keys.sort(new Comparator<String>() {
                @Override
                public int compare(String s, String anotherString) {
                    return s.compareTo(anotherString);
                }
            });
            for (String key : keys) {
                propertyViews.add(new PropertyView(key, jobConfiguration.get(key), false));
            }
            return propertyViews;
        }
    }

    static class PropertyView {
        final String name;
        final String value;

        PropertyView(String name, String value, boolean isBold) {
            if (isBold) {
                this.name = "<b>"+name+"</b>";
            } else {
                this.name = name;
            }
            value = value.replaceAll("<", "&lt;").replaceAll(">", "&gt;").trim();
            if (value.startsWith("&lt;")) {
                value = "<pre>" + value + "</pre";
            }
            this.value = value;
        }
    }
%>

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
        <th colspan="2">Job Properties</th>
    </tr>
    </thead>
    <tbody>
    <%
        ServletContext sc = request.getServletContext();
        ServiceContainer serviceContainer = (ServiceContainer) sc.getAttribute("serviceContainer");
        final BundleFilter systemFilter = new BundleFilter();
        systemFilter.withProvider(BundleFilter.PROVIDER_SYSTEM);
        List<BundleView> bundleViews = new ArrayList<>();
        try {
            final Principal userPrincipal = request.getUserPrincipal();
            if (userPrincipal != null && serviceContainer != null) {
                String userName = userPrincipal.getName();
                BundleDescriptor[] bundleDescriptors = serviceContainer.getProductionService().getBundles(userName, systemFilter);
                for (BundleDescriptor bundle : bundleDescriptors) {
                    ProcessorDescriptor[] processorDescriptors = bundle.getProcessorDescriptors();
                    if (processorDescriptors != null) {
                        BundleView bundleView = BundleView.create(bundle);
                        for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                            bundleView.processorViews.add(ProcessorView.create(processorDescriptor));
                        }
                        bundleViews.add(bundleView);
                    }
                }
            }
        } catch (ProductionException e) {
            e.printStackTrace(new PrintWriter(out));
        }
        for (BundleView bundleView : bundleViews) {
            List<ProcessorView> processorViews = bundleView.processorViews;
            for (int p = 0; p < processorViews.size(); p++) {
                ProcessorView processorView = processorViews.get(p);
                List<PropertyView> propertyViews = processorView.propertyViews;
                for (int i = 0; i < propertyViews.size(); i++) {
                    PropertyView propertyView = propertyViews.get(i);
    %>
    <tr>
        <%
            if (p == 0 && i == 0) {
        %>
        <td rowspan="<%=bundleView.numRows() %>"><%=bundleView.title %>
        </td>
        <%
            }
        %>
        <%
            if (i == 0) {
        %>
        <td rowspan="<%=processorView.numRows() %>"><%=processorView.title %>
        </td>
        <%
            }
        %>
        <td><%=propertyView.name %>
        </td>
        <td><%=propertyView.value %>
        </td>
    </tr>
    <%
                }
            }
        }

    %>
    </tbody>
</table>

<table style="width: 99%; border: 0;" align="center">
    <tr>
        <td>
            <p class="copyrightApp"><%= BackendServiceImpl.VERSION %>, &#169; <%= BackendServiceImpl.COPYRIGHT_YEAR %>
                Brockmann Consult GmbH
                &nbsp;-&nbsp;<a href="http://www.brockmann-consult.de/bc-web/impressum.html"
                                target="_blank">Impressum</a></p>
        </td>
    </tr>
</table>
</body>
</html>
