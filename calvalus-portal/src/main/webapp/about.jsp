<?xml version="1.0" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<%@page language="java" import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<%@ page import="java.security.Principal" %>

<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <title>About Calvalus</title>
    <link type="text/css" rel="stylesheet" href="calvalus.css">
    <meta http-equiv="content-type" content="text/html; charset=ISO-8859-1">
    <style type="text/css">
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
<table style="width: 820px; border: 0;" align="center">
    <tr>
        <td>
            <hr/>

            <table class="headerPanel">
                <tbody>
                <tr>
                    <td>
                        <img src="images/esa-logo.jpg" alt="ESA logo">
                    </td>
                    <td>
                        <h1 class="title">Calvalus</h1>

                        <h2 class="subTitle">Portal for Earth Observation Cal/Val and User Services</h2>
                    </td>
                    <td class="userName">
                        <% final Principal userPrincipal = request.getUserPrincipal(); %>
                        <% if (userPrincipal != null) { %>
                        User <strong><%=userPrincipal.getName()%>
                        <br/>
                        <a href='<%= response.encodeURL("logout.jsp") %>'>Log Out</a>
                    </strong>
                        <% } else { %>
                        <a href='<%= response.encodeURL("calvalus.jsp") %>'>Enter</a>
                        <% } %>
                    </td>
                </tr>
                </tbody>
            </table>
            <hr/>

            <h1>The ESA Calvalus Study</h1>


            <h2>Objective</h2>

            <p><img src="images/product-development-cycle.png" width="240" height="209" vspace="4" align="right"
                    alt="Product development cycle"><a href="http://www.esa.int/esaEO/">ESA's Earth Observation (EO)</a>
                missions provide a unique dataset
                of observational data of our environment. Calibration of
                the measured signal and validation of the derived products is an extremely important task for efficient
                exploitation
                of EO data and the basis for reliable scientific conclusions. In spite of this importance, the cal/val
                work is often
                hindered by insufficient means to access data, time consuming work to identify suitable in-situ data
                matching the EO
                data, incompatible software and limited possibilities for rapid prototyping and testing of ideas. In
                view of the
                future fleet of satellites and the fast-growing amount of data produced, a very efficient technological
                backbone is
                required to maintain the ability of ensuring data quality and algorithm performance.</p>

            <p>The announcement of opportunities EO Cal/Val and User Services is a technology study of the
                <a href="http://smeprojects.esa.int/TechnologyforSMEs.asp">ESA LET-SME</a> 2009 call,
                investigating into an existing leading edge technology (LET) for their applicability in the EO domain.
                Specifically,
                LET-SME is a spin-in instrument encouraging the participation of SMEs to ESA technology. The LET-SME
                focuses on
                early stage development of <strong>Leading Edge Technologies</strong>, i.e. the ones likely to become
                the reference
                technologies
                for the near future, and have good chances of being infused into ESA projects and missions. </p>

            <p>In accordance with the study's statement of work, Calvalus is a system that has been proposed to fully
                support the
                idea of LET-SME, thus
                with a strong focus on a selected LET which is described in this report.</p>

            <h2>Approach</h2>

            <p><img src="images/cluster-hardware.png" style="float:left; margin-right:12px;"
                    alt="The cluster"><a href="http://www.brockmann-consult.de/">Brockmann Consult GmbH</a> proposed to
                develop a <strong>demonstration
                    processing system</strong> based the <strong>MapReduce programming model</strong>
                (MR) combined with a <strong>Distributed File System (DSF)</strong>. The basic approach was first
                published in 2004 by the two
                Google
                computer scientists J. Dean and S. Ghemawat
                (<em><a href="http://labs.google.com/papers/mapreduce.html">MapReduce: Simplified Data Processing on
                    Large
                    Clusters</a></em>).
                The technology has been designed for processing of ultra large amounts of data and is based on massive
                parallelisation of tasks combined with a distributed file system, both running on
                large, extensible clusters solely comprising commodity hardware. All nodes in the cluster are equally
                configured and
                provide both disk storage and CPU power. Well known online services provided by Google, Yahoo, Amazon
                and Facebook
                rely on this technology. Its spin-in application to space born, spatial data is feasible and pertinent.
                <img src="images/map-reduce.png"
                     style="float:right; margin-right:24px;margin-left:12px;" alt="Map reduce"><br>
                The results
                of this
                study demonstrate that the processing of large amounts of EO data using MR and a DSF is efficient and
                advantageous.
                The demonstration system, Calvalus, basically comprises a <strong>cluster of 20 commodity
                    computers</strong> with a total disk
                capacity
                of <strong>112 TB</strong> at a total cost of <strong>30 k&euro;</strong>. The processing system
                software is based on
                <a href="http://hadoop.apache.org/">Apache Hadoop</a>, an open-source
                implementation of MR and DSF in Java and <a href="http://www.brockmann-consult.de/beam/">BEAM</a>,
                an ESA Earth Observation Toolbox and Development Platform.</p>

            <p>Calvalus gains its performance from <strong>massive parallelisation of tasks</strong> and the <strong>data-local
                execution</strong> of
                code. Usual
                processing clusters or grids first copy input data from storage nodes to compute nodes, thereby
                introducing I/O
                overheads and network transfer bottlenecks. In Calvalus, processing code is executed on cluster nodes
                where the
                input data are stored. Executable code can be easily deployed; the code distribution and installation on
                all cluster
                nodes is done automatically. Multiple versions of processing code can be used in parallel. All these
                properties of
                the Calvalus system allow users to efficiently perform cal/val and EO data processing functions on whole
                mission datasets, thus allowing an <strong>agile EO data product development</strong> and <strong>fast
                    improvement cycles</strong>.</p>

            <p>The different production scenarios and analyses implemented in Calvalus are inspired by the needs of the
                current ESA
                projects, such as <a href="http://www.coastcolour.org/">CoastColour</a> and
                <a href="http://www.esa-cci.org/">Climate Change Initiative (CCI)</a> for Land Cover and Ocean Colour,
                both of
                strong interest to an international user community. The implementation is focused on
                the <a href="http://www.ioccg.org/">Ocean Colour</a> data processing and validation:</p>
            <ol>
                <li><img src="images/processing-results.png" style="float:right;"
                         alt="Processing results">
                    <strong>L2-Production:</strong> Processing of Level-1b radiance products to Level-2 ocean
                    reflectances and inherent optical
                    property (IOP) products.
                </li>
                <li><strong>L3-Production:</strong> Processing of Level-2 products to spatially and temporally
                    aggregated Level-3 products.
                </li>
                <li><strong>Match-up analysis:</strong> Generation of match-ups of Level-2 products with in-situ data.
                </li>
                <li><strong>Trend analysis:</strong> Generation of time-series of Level-3 products generated from
                    Level-2 data.
                </li>
            </ol>

            <p>The Level-2 products in production scenarios 2 to 3 are generated on-the-fly from Level-1b using selected
                Level-2
                processors and their required versions, processing parameters and LUTs. The Calvalus demonstration
                system currently
                holds the <strong>full mission <a href="http://envisat.esa.int/instruments/meris/">Envisat MERIS</a>
                    Level-1b RR data</strong> as well as all MERIS Level-1b FR product subsets
                required by
                the CoastColour project.</p>

            <p>Calvalus provides an <strong>easily operated <a href="calvalus.jsp">web interface</a></strong>
                that allows users to order and monitor productions according
                the to four production scenarios, and to finally download the results. It also offers a Java production
                API, allowing developers to write new production scenarios.
            </p>

            <h2>Project</h2>

            <p>The Calvalus study has been performed in the time from September 2009 to October 2011. The Calvalus
                system is now being actively further developed.</p>

            <p>The Calvalus team is</p>
            <ul>
                <li>Dr Martin B&ouml;ttcher, Brockmann Consult GmbH - Developer</li>
                <li>Olga Faber, Brockmann Consult GmbH - Tester</li>
                <li>Norman Fomferra, Brockmann Consult GmbH - Project manager / Developer</li>
                <li>Dr Ewa Kwiatkowska, ESA - Project initiator / Technical ESA representative</li>
                <li>Marco Z&uuml;hlke, Brockmann Consult GmbH - Developer</li>
            </ul>

            <h2>Information</h2>

            <!-- todo: provide and restrict access to TS (nf) -->
            <p>More information about the study and its results can be found in the
                <a href="pub/docs/Calvalus-Final_Report-Public-1.0-20111031.pdf">Final Report</a>.
                Other public documents you might be interested in are the
                <a href="pub/docs/Calvados-RB-1.2.1-20100716.pdf">Baseline Requirements</a> and the
                <a href="pub/docs/Calvalus-ATP-1.2-20111031.pdf">Acceptance Test Plan</a>. The
                <a href="http://www.google.de/search?q=Technical+Specification">Technical Specification</a> is available for
                registered users only.
            </p>

            <p>
                <strong><img src="images/bc-logo.png" style="float:left; margin-right:12px;" alt="BC-logo">Brockmann
                    Consult GmbH</strong> <br>
                Max-Planck-Str 2, 21502 Geesthacht, Germany<br/>
                <a href="http://www.brockmann-consult.de/">www.brockmann-consult.de</a><br/>
                info (a) brockmann-consult (d) de<br/>
                Tel +49 4152 889300<br/>
                Fax +49 4152 889333<br/>
            </p>

            <p>
                <br/>
            </p>


            <p class="copyright"><%= BackendServiceImpl.VERSION %>, &#169; 2015 Brockmann Consult GmbH &nbsp;-&nbsp;<a
                    href="http://www.brockmann-consult.de/bc-web/impressum.html" target="_blank">Impressum</a></p>

        </td>
    </tr>
</table>

</body>
</html>