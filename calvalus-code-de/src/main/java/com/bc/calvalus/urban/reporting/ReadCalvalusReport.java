package com.bc.calvalus.urban.reporting;

import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import java.util.Optional;
import javax.ws.rs.client.Client;
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
    private static Client client = ClientBuilder.newClient();

    public Optional<CalvalusReport> getReport(String jobID, String date) {
        String url = PropertiesWrapper.get("reporting.wps.url");
        String jsonUser = clientRequest(String.format("%s/%s/%s", url, jobID, date));
        Optional<CalvalusReport> jobSummaries = toJobDetails(jsonUser);
        return jobSummaries;
    }

    private String clientRequest(String uri) {
        String jsonAsPlainText = null;
        Invocation.Builder builder = client.target(uri).request();
        Response response = builder.accept(MediaType.TEXT_PLAIN_TYPE).get();
        int status = response.getStatus();
        if (status >= HTTP_SUCCESSFUL_CODE_START && status < HTTP_SUCCESSFUL_CODE_END) {
            jsonAsPlainText = builder.get(String.class);
//            response.close();
            return jsonAsPlainText;
        }
        return null;
    }

    private Optional<CalvalusReport> toJobDetails(String jsonUser) {
        if (jsonUser == null || jsonUser.contains(STATUS_FAILED)) {
            return Optional.empty();
        }
        Gson gson = new Gson();
        CalvalusReport usageStatistics = gson.fromJson(jsonUser, CalvalusReport.class);
        return Optional.ofNullable(usageStatistics);
    }
}
