package com.bc.calvalus.reporting.ws;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * @author hans
 */
@Path("/")
public class ReportingService {

    @GET
    @Path("job/{jobId}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getSingleJobReportTxt(@PathParam("jobId") String jobId) {
        return "Calvalus usage for job ID '" + jobId + "'";
    }

    @GET
    @Path("{user}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getAllJobReportTxt(@PathParam("user") String user) {
        return "Calvalus usage for user " + user;
    }

    @GET
    @Path("{user}/time/{year}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getYearlyJobReportTxt(@PathParam("user") String user,
                                        @PathParam("year") String year) {
        return "Calvalus usage for user " + user + " in year " + year;
    }

    @GET
    @Path("{user}/time/{year}/{month}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getMonthlyJobReportTxt(@PathParam("user") String user,
                                         @PathParam("year") String year,
                                         @PathParam("month") String month) {
        return "Calvalus usage for user " + user + " in month " + month + " year " + year;
    }

    @GET
    @Path("{user}/range")
    @Produces(MediaType.TEXT_PLAIN)
    public String getTimeSpecificJobReportTxt(@PathParam("user") String user) {
        return "Calvalus usage for user " + user + " in period $DATE_START to $DATE_END";
    }

}
