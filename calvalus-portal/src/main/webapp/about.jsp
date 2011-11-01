<%@ taglib prefix="bean" uri="http://java.sun.com/jsp/jstl/fmt" %>
<!doctype html>

<%@page language="java" import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<%@ page import="java.security.Principal" %>

<html>
<head>
    <title>About Calvalus</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <style>
        h1, h2, h3 {
            font-family: Calibri, Verdana, Arial, sans-serif;
            color: #7b8f88;
            font-weight: bold;
        }

        p, li, td {
            font-family: Georgia, Times, serif;
            font-size: 10pt;
        }

    </style>
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
            <br/>
            <a href='<%= response.encodeURL("logout.jsp") %>'>Log Out</a>
        </b>
            <% } else { %>
            <a href='<%= response.encodeURL("calvalus.jsp") %>'>Enter</a>
            <% } %>
        </td>
    </tr>
</table>
<hr/>

<h1>The ESA Calvalus Study - Executive Summary</h1>


<h2>Objective</h2>

<p><a href="http://www.esa.int/esaEO/">ESA's Earth Observation (EO)</a> missions provide a unique dataset
    of observational data of our environment. Calibration of
    the measured signal and validation of the derived products is an extremely important task for efficient exploitation
    of EO data and the basis for reliable scientific conclusions. In spite of this importance, the cal/val work is often
    hindered by insufficient means to access data, time consuming work to identify suitable in-situ data matching the EO
    data, incompatible software and limited possibilities for rapid prototyping and testing of ideas. In view of the
    future fleet of satellites and the fast-growing amount of data produced, a very efficient technological backbone is
    required to maintain the ability of ensuring data quality and algorithm performance.</p>

<p>The announcement of opportunities EO Cal/Val and User Services is a technology study of the ESA LET-SME 2009 call,
    investigating into an existing leading edge technology (LET) for their applicability in the EO domain. Specifically,
    LET-SME is a spin-in instrument encouraging the participation of SMEs to ESA technology. The LET-SME focuses on
    early stage development of <b>Leading Edge Technologies</b>, i.e. the ones likely to become the reference
    technologies
    for the near future, and have good chances of being infused into ESA projects and missions. </p>

<p>In accordance with the study's statement of work, Calvalus is a system that has been proposed to fully support the
    idea of LET-SME, thus
    with a strong focus on a selected LET which is described in this report.</p>

<h2>Approach</h2>

<p><a href="http://www.brockmann-consult.de">Brockmann Consult GmbH</a> proposed to develop a <b>demonstration
    processing system</b> based the <b>MapReduce programming model</b>
    (MR) combined with a <b>Distributed File System (DSF)</b>. The basic approach was first published in 2004 by the two
    Google
    computer scientists J. Dean and S. Ghemawat
    (<i><a href="http://labs.google.com/papers/mapreduce.html">MapReduce: Simplified Data Processing on Large
        Clusters</a></i>).
    The technology has been designed for processing of ultra large amounts of data and is based on massive
    parallelisation of tasks combined with a distributed file system, both running on
    large, extendible clusters solely comprising commodity hardware. All nodes in the cluster are equally configured and
    provide both disk storage and CPU power. Well known online services provided by Google, Yahoo, Amazon and Facebook
    rely on this technology. Its spin-in application to space born, spatial data is feasible and pertinent. The results
    of this
    study demonstrate that the processing of large amounts of EO data using MR and a DSF is efficient and advantageous.
    The demonstration system, Calvalus, basically comprises a <b>cluster of 20 commodity computers</b> with a total disk
    capacity
    of <b>112 TB</b> at a total cost of <b>30 k&euro;</b>. The processing system software is based on
    <a href="http://hadoop.apache.org/">Apache Hadoop</a>, an open-source
    implementation of MR and DSF in Java and <a href="http://www.brockmann-consult.de/beam/">BEAM</a>,
    an ESA Earth Observation Toolbox and Development Platform.</p>

<p>Calvalus gains its performance from <b>massive parallelisation of tasks</b> and the <b>data-local execution</b> of
    code. Usual
    processing clusters or grids first copy input data from storage nodes to compute nodes, thereby introducing I/O
    overheads and network transfer bottlenecks. In Calvalus, processing code is executed on cluster nodes where the
    input data are stored. Executable code can be easily deployed; the code distribution and installation on all cluster
    nodes is done automatically. Multiple versions of processing code can be used in parallel. All these properties of
    the Calvalus system allow users to efficiently perform cal/val and EO data processing functions on whole
    mission datasets, thus allowing an <b>agile EO data product development</b> and <b>fast improvement cycles</b>.</p>

<p>The different production scenarios and analyses implemented in Calvalus are inspired by the needs of the current ESA
    projects, such as <a href="http://www.coastcolour.org/">CoastColour</a> and
    <a href="http://www.esa-cci.org/">Climate Change Initiative (CCI)</a> for Land Cover and Ocean Colour, both of
    strong interest to an international user community. The implementation is focused on ocean colour: </p>
<ol>
    <li>L2-Production: Processing of Level-1b radiance products to Level-2 ocean reflectances and inherent optical
        property (IOP) products.
    </li>
    <li>L3- Production: Processing of Level-2 products to spatially and temporally aggregated Level-3 products.</li>
    <li>Match-up analysis: Generation of match-ups of Level-2 products with in-situ data.</li>
    <li>Trend analysis: Generation of time-series of Level-3 products generated from Level-2 data.</li>
</ol>

<p>The Level-2 products in production scenarios 2 to 3 are generated on-the-fly from Level-1b using selected Level-2
    processors and their required versions, processing parameters and LUTs. The Calvalus demonstration system currently
    holds the full mission Envisat MERIS Level-1b RR data as well as all MERIS Level-1b FR product subsets required by
    the CoastColour project.</p>

<p>Calvalus provides <a href="calvalus.jsp">web portal</a> that allows users to order and monitor productions according
    the to four production scenarios, and to finally download the results. It also offers a Java production API,
    allowing developers to write new production scenarios.
</p>

<h2>Team</h2>

<p>The Calvalus team is</p>
<ul>
    <li>Dr Martin B&ouml;ttcher, Brockmann Consult GmbH - Developer</li>
    <li>Olga Faber, Brockmann Consult GmbH - Tester</li>
    <li>Norman Fomferra, Brockmann Consult GmbH - Project manager / Developer</li>
    <li>Dr Ewa Kwiatkowska, ESA - Project initiator / Technical ESA representative</li>
    <li>Marco Z&uuml;hlke, Brockmann Consult GmbH - Developer</li>
</ul>

<h2>Contact</h2>

<p>
    <b>Brockmann Consult GmbH</b> <br/>
    Max-Planck-Str 2, 21502 Geesthacht, Germany<br/>
    <a href="http://www.brockmann-consult.de">www.brockmann-consult.de</a><br/>
    info (a) brockmann-consult (d) de<br/>
    Tel +49 4152 889300<br/>
    Fax +49 4152 889333<br/>
</p>

<p>Calvalus has been developed in the time from September 2009 to October 2011.
    <!--
    More information about the study and its
    results can be found in the <a href="pub/docs/Calvalus-Final_Report-Public-1.0-20111031.pdf">Final Report</a>.
    -->
</p>

<p>
    <br/>
</p>


<p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; 2011 Brockmann Consult GmbH &nbsp;-&nbsp;<a
        href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

</body>
</html>
