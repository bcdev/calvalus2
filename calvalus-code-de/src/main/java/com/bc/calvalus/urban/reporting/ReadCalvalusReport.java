package com.bc.calvalus.urban.reporting;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.urban.LoadProperties;
import com.google.gson.Gson;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * @author muhammad.bc.
 */
public class ReadCalvalusReport {
    private static final int HTTP_SUCCESSFUL_CODE_START = 200;
    private static final int HTTP_SUCCESSFUL_CODE_END = 300;
    private static final String STATUS_FAILED = "\"Status\": \"Failed\"";
    private final String calvalusReportUrl = LoadProperties.getInstance().getCalvalusReportUrl();
    private final Logger logger = CalvalusLogger.getLogger();


    public Optional<CalvalusReport> getReport(String jobID, String strDate) {
        logger.log(Level.INFO, String.format("Extracting %s jobID from reporting server", jobID));
        String jsonUsageReport = clientRequest(String.format("%s/job/%s/%s", calvalusReportUrl, jobID, strDate));
        return toCalvalusReport(jsonUsageReport);
    }

    private String clientRequest(String uri) {
        Invocation.Builder builder = ClientBuilder.newClient().target(uri).request();
        Response response = builder.accept(MediaType.APPLICATION_JSON_TYPE).get();
        int status = response.getStatus();
        if (status >= HTTP_SUCCESSFUL_CODE_START && status < HTTP_SUCCESSFUL_CODE_END) {
            return builder.get(String.class);
        }
        return null;
    }

    private Optional<CalvalusReport> toCalvalusReport(String jsonUser) {
        if (jsonUser == null || jsonUser.contains(STATUS_FAILED)) {
            return Optional.empty();
        }
        Gson gson = new Gson();
        CalvalusReport usageStatistics = gson.fromJson(jsonUser, CalvalusReport.class);
        return Optional.ofNullable(usageStatistics);
    }
}
