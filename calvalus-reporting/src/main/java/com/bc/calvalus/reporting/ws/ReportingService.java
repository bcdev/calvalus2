package com.bc.calvalus.reporting.ws;

import com.bc.calvalus.reporting.exceptions.JobNotFoundException;
import com.bc.calvalus.reporting.io.JSONExtractor;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    @Produces(MediaType.APPLICATION_JSON)
    public String getSingleJobReportTxt(@PathParam("jobId") String jobId) {
        try {
            UsageStatistic singleStatistic = jsonExtractor.getSingleStatistic(jobId);
            return reportGenerator.generateJsonSingleJob(singleStatistic);
        } catch (IOException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("{user}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getAllJobReportTxt(@PathParam("user") String user) {
        try {
            List<UsageStatistic> singleUserStatistics = jsonExtractor.getSingleUserStatistic(user);
            if (singleUserStatistics.size() < 1) {
                throw new JobNotFoundException("Jobs not found for user '" + user + "'");
            }
            return reportGenerator.generateJsonUserSingleJob(singleUserStatistics);
        } catch (IOException | JobNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("/alluser")
    @Produces(MediaType.APPLICATION_JSON)
    public String getAllUserSummary() {
        try {
            Map<String, List<UsageStatistic>> allUserStatistics = jsonExtractor.getAllUserStatistic();
            if (allUserStatistics.size() < 1) {
                throw new JobNotFoundException("No job found for any user ");
            }
            return reportGenerator.generateJsonAllUserJobSummary(allUserStatistics);
        } catch (IOException | JobNotFoundException exception) {
            return getErrorResponse(exception);
        }
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

    private String getErrorResponse(Exception exception) {
        Map<String, String> responseMap = new HashMap<>();
        responseMap.put("Status", "Failed");
        responseMap.put("Message", exception.getMessage());
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(responseMap);
    }

}
