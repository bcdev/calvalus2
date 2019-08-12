package com.bc.calvalus.reporting.restservice.ws;

import com.bc.calvalus.reporting.common.UsageStatistic;
import com.bc.calvalus.reporting.restservice.exceptions.DatabaseFileNotFoundException;
import com.bc.calvalus.reporting.restservice.exceptions.JobNotFoundException;
import com.bc.calvalus.reporting.restservice.io.JSONExtractor;
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
 * @author hans , muhammad
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
    @Path("job/{jobId}/{date}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getSingleJobReport(@PathParam("jobId") String jobId, @PathParam("date") String date) {
        try {
            UsageStatistic singleStatistic = jsonExtractor.getSingleStatistic(jobId, date);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(singleStatistic);
        } catch (IOException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("{user}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getAllJobReportsForUser(@PathParam("user") String user) {
        try {
            // TODO(hans-permana, 20180111): get user usage statistics should not require a hard-coded date
            List<UsageStatistic> singleUserStatistics = jsonExtractor.getSingleUserStatistic(user, "2017-01-20");
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(singleUserStatistics);
        } catch (IOException | DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("/all/users")
    @Produces(MediaType.APPLICATION_JSON)
    public String getUserAggregatedJobReports() {
        try {
            // TODO(hans-permana, 20180111): get user-aggregated usage statistics should not require a hard-coded date
            Map<String, List<UsageStatistic>> allUserStatistics = jsonExtractor.getAllUserUsageStatistic("2017-01-01");
            if (allUserStatistics.size() < 1) {
                throw new JobNotFoundException("No job found for any users");
            }
            return reportGenerator.generateJsonAllUserJobSummary(allUserStatistics);
        } catch (IOException | JobNotFoundException |DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("{user}/time/{year}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getYearlyJobReportTxt(@PathParam("user") String user, @PathParam("year") String year) {
        try {
            List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageInYear(user, year);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(usageStatisticList);
        } catch (IOException | DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("{user}/time/{year}/{month}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getMonthlyJobReportTxt(@PathParam("user") String user,
                                         @PathParam("year") String year,
                                         @PathParam("month") String month) {
        try {
            List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageInYearMonth(user, year, month);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(usageStatisticList);
        } catch (IOException | DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("{user}/time/{year}/{month}/{day}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getMonthlyJobReportTxt(@PathParam("user") String user,
                                         @PathParam("year") String year,
                                         @PathParam("month") String month,
                                         @PathParam("day") String day) {
        try {
            List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageYearMonthDay(user, year, month,
                                                                                                   day);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(usageStatisticList);
        } catch (IOException | DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("{user}/range/{date_start}/{date_end}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getTimeSpecificJobReportTxt(@PathParam("user") String user,
                                              @PathParam("date_start") String start,
                                              @PathParam("date_end") String end) {
        try {
            List<UsageStatistic> usageStatisticList = jsonExtractor.getSingleUserUsageBetween(user, start, end);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(usageStatisticList);
        } catch (IOException | DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("/range-user/{date_start}/{date_end}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getRangeUserBetween(@PathParam("date_start") String start, @PathParam("date_end") String end) {
        try {
            Map<String, List<UsageStatistic>> allUsersStartEndDateStatistic = jsonExtractor.getAllUserUsageBetween(
                        start, end);
            if (allUsersStartEndDateStatistic.size() < 1) {
                throw new JobNotFoundException("No job found for any user ");
            }
            return reportGenerator.generateJsonAllUserJobSummary(allUsersStartEndDateStatistic);
        } catch (IOException | JobNotFoundException | DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("/range-date/{date_start}/{date_end}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getRangeDateBetween(@PathParam("date_start") String start, @PathParam("date_end") String end) {
        try {
            Map<String, List<UsageStatistic>> allUsersStartEndDateStatistic = jsonExtractor.getAllDateUsageBetween(
                        start, end);
            if (allUsersStartEndDateStatistic.size() <= 0) {
                throw new JobNotFoundException("No job found for any user ");
            }
            return reportGenerator.generateJsonUsageBetween(allUsersStartEndDateStatistic, "jobsInDate");
        } catch (IOException | JobNotFoundException | DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("/range-queue/{date_start}/{date_end}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getRangeQueueBetween(@PathParam("date_start") String start, @PathParam("date_end") String end) {
        try {
            Map<String, List<UsageStatistic>> usageBetween = jsonExtractor.getAllQueueUsageBetween(start, end);
            if (usageBetween.size() <= 0) {
                throw new JobNotFoundException("No job found for any user ");
            }
            return reportGenerator.generateJsonUsageBetween(usageBetween, "jobsInQueue");
        } catch (IOException | JobNotFoundException | DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    @GET
    @Path("/date/{date_start}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getFromDate(@PathParam("date_start") String startDate) {
        try {
            List<UsageStatistic> usageStatisticsSince = jsonExtractor.getUsageStatisticsSince(startDate);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(usageStatisticsSince);
        } catch (IOException | DatabaseFileNotFoundException exception) {
            return getErrorResponse(exception);
        }
    }

    private String getErrorResponse(Exception exception) {
        Map<String, String> responseMap = new HashMap<>();
        responseMap.put("Status", "Failed");
        responseMap.put("Message", exception.getMessage());
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(responseMap);
    }

}
