<%@ taglib prefix="bean" uri="http://java.sun.com/jsp/jstl/fmt" %>
<!doctype html>

<%@page language="java" import="java.security.Principal" %>

<html>
<head>
    <title>About Calvalus</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
</head>

<body>

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
            User <b><%=userPrincipal.getName()%>
        </b>
            <% } else { %>
            Not logged in.
            <% } %>
        </td>
    </tr>
</table>
<hr/>

<h1>About Calvalus</h1>

<p>Calvalus is an ESA <i>Leading Edge Technology for Small and Medium Enterprises</i> (LET-SME) project that
    aims to demonstrate that
    <a href="http://hadoop.apache.org/">Hadoop</a>
    technology can be efficiently applied to EO data by implementing a demonstration system
    for Cal/Val and User Services.
</p>

LocalAddr = <%=request.getLocalAddr()%> <br/>
RemoteAddr = <%=request.getRemoteAddr()%> <br/>

<% if (request.getLocalAddr().startsWith("")) { %>
<h1>Calvalus Cluster</h1>

<p>
    <a href="http://cvmaster00:50070/dfshealth.jsp" target="fileSystem">Hadoop File System</a>
</p>

<p>
    <a href="http://cvmaster00:50030/jobtracker.jsp" target="jobTracker">Hadoop Job Tracker</a>
</p>
<% } %>


<h1>Calvalus Portal Overview</h1>

<h2>L2 Processing</h2>

<p>Lets you bulk-process a Level-1 input file set to corresponding Level-2 output file set.</p>

<h2>L3 Processing</h2>

<p>Lets you bulk-process a Level-1 input file set to a corresponding, intermediate Level-2 output file set and generate
    one or more Level-3 products.</p>

<h2>Match-up Analysis</h2>

<h2>Trend Analysis</h2>

<h2>Production Management</h2>

<h2>Region Management</h2>


<p>Calvalus - Version 0.2, &#169; 2011 Brockmann Consult GmbH</p>

</body>
</html>
