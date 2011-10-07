<%@ taglib prefix="bean" uri="http://java.sun.com/jsp/jstl/fmt" %>
<!doctype html>

<%@page language="java" import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<%@ page import="java.security.Principal" %>

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

<p>ESA’s Earth Observation (EO) missions provide a unique dataset of observational data of our environment. Calibration
    of the measured signal and validation of the derived products is an extremely important task for efficient
    exploitation of EO data and the basis for reliable scientific conclusions. In spite of this importance, the cal/val
    work is often hindered by insufficient means to access data, time consuming work to identify suitable in-situ data
    matching the EO data, incompatible software and no possibility for rapid prototyping and testing of ideas. In view
    of the future fleet of satellites and the fast-growing amount of data produced, a very efficient technological
    backbone is required to maintain the ability of ensuring data quality and algorithm performance.
</p>

<p>The project EO Cal/Val and User Services is an ESA LET-SME technology study investigating into an existing leading
    edge technology (LET) for their applicability in the EO domain. In this sense, Brockmann Consult GmbH proposed to
    develop a demonstration processing system based the MapReduce programming model (MR) combined with a Distributed
    File System (DSF). The basic approach was first published in 2004 by the two Google computer scientists J. Dean and
    S. Ghemawat. The technology has been designed for processing of very large amounts of data and is based on
    massive parallelisation of tasks combined with a distributed file system, both running on large, extendible clusters
    solely comprising commodity hardware. All nodes in the cluster are equally configured and provide both disk storage
    and CPU power. Well known online services provided by Google, Yahoo, Amazon and Facebook rely on this technology.
    Its spin-in application to space born, spatial data is eligible and feasible and the results of this study
    demonstrates the efficient processing of very large amounts of EO data using MR and a DSF.
</p>

<p>The demonstration system, Calvalus, basically comprises a cluster of 20 commodity computers with a total disk
    capacity of 112 TB. The processing system software is based on Apache Hadoop – an open-source implementation of MR
    and DSF in Java.
</p>

<p>Calvalus gains its performance from massive parallelisation of tasks and the data-local execution of code. Usual
    processing clusters or grids first copy input data from storage nodes to compute nodes, thereby introducing I/O
    overheads and network transfer bottlenecks. In Calvalus, processing code is executed on cluster nodes where the
    input data is stored. Executable code can be easily deployed; the code distribution and installation on all cluster
    nodes is done automatically. Multiple versions of processing code can be used in parallel. All these properties of
    the Calvalus system allow users to efficiently perform cal/val and EO data processing functions on whole mission
    datasets, thus allowing an agile product development and fast improvement cycles.
</p>

<p>The different production scenarios and analyses implemented in Calvalus are inspired by the needs of current ESA
    projects such as CoastColour and the ESA Climate Change Initiative (CCI) programme on Land Cover and Ocean Colour,
    both of strong interest to an international user community.
    <ol>
        <li>L2-Production: Processing of Level-1b radiance products to Level-2 ocean reflectances and inherent optical
    property (IOP) products.</li>
        <li>L3- Production: Processing of Level-2 products to spatially and temporally aggregated Level-3 products.</li>
        <li>Match-up analysis: Generation of match-ups of Level-2 products with in-situ data.</li>
        <li>Trend analysis: Generation of time-series of Level-3 products generated from Level-2 data.</li>
    </ol>
</p>

<p>The Level-2 products in production scenarios 2 to 3 are generated on-the-fly from Level-1b using a selected Level-2
    processor. The Calvalus demonstration system currently holds the full mission Envisat MERIS Level-1b RR data as well
    as all MERIS Level-1b FRS product subsets required by the CoastColour project. The productions scenarios comprise
    the production of Level-2 and Level 3 products, and the generation of match-up and trend analysis reports.
</p>

<p>Calvalus has a web frontend that allows users to order and monitor productions according the to four production
    scenarios, and to finally download the results. It also offers a Java production API, allowing developers to write
    new production scenarios.
</p>

<p>Calvalus has been developed in the time from September 2009 to October 2011.
</p>


<p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; 2011 Brockmann Consult GmbH &nbsp;-&nbsp;<a href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

</body>
</html>
