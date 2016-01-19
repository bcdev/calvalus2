<?xml version="1.0" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<%@page language="java" import="com.bc.calvalus.portal.server.BackendServiceImpl" %>
<%@ page import="java.security.Principal" %>

<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <title>Calvalus on-demand processing</title>
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
  <hr>
  <table width="500" class="headerPanel">
    <tr>
      <td width="200">
          <a href="http://feeder.calfin.fmi.fi/"><img src="/calfin/images/fmi-logo.png" width="200" alt="FMI logo"/><br/><img src="/calfin/images/syke-logo.png" height="60" alt="SYKE logo"/></a>
      </td>
        <td width="321">
          <h1 class="title">Calvalus</h1>
          <h2 class="subTitle">Finnish portal for on-demand processing</h2>
        </td>
      <td width="50" class="href"><a href='http://feeder.calfin.fmi.fi/products.html'>Back</a></td>
    </tr>
  </table>
  <hr>
  <p>This is the <a href="http://feeder.calfin.fmi.fi/">Calvalus</a> on-demand processing system, developed by <a href="http://www.brockmann-consult.de/" class="href">Brockmann Consult GmbH</a>.</p>
  <p><%= BackendServiceImpl.VERSION %>, &#169; 2016 Brockmann Consult GmbH</p>

</body>
</html>