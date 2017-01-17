package com.bc.calvalus.reporting.ws;

import com.bc.calvalus.reporting.io.JSONExtractor;
import com.bc.wps.utilities.PropertiesWrapper;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * @author hans
 */
@Path("/")
public class ReportingService {

    private ReportGenerator reportGenerator;
    private JSONExtractor jsonExtractor;

    public ReportingService() throws IOException {
        PropertiesWrapper.loadConfigFile("calvalus-reporting.properties");
        jsonExtractor = new JSONExtractor();
        reportGenerator = new ReportGenerator();
    }

    @GET
    @Path("job/{jobId}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getSingleJobReportTxt(@PathParam("jobId") String jobId) {
        UsageStatistic singleStatistic = null;
        try {
            singleStatistic = jsonExtractor.getSingleStatistic(jobId);
            return reportGenerator.generateTextSingleJob(singleStatistic);
        } catch (IOException exception) {
            return exception.getLocalizedMessage();
        }
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
