package com.bc.calvalus.reporting.restservice.code;

import com.bc.calvalus.reporting.restservice.io.JSONExtractor;
import com.bc.calvalus.reporting.restservice.ws.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * @author muhammad.bc.
 */
@Path("/")
public class CodeDeService {
    private JSONExtractor jsonExtractor;

    public CodeDeService() throws IOException {
        PropertiesWrapper.loadConfigFile("calvalus-reporting.properties");
        jsonExtractor = new JSONExtractor();
    }


    @GET
    @Path("date/{startDateTime}/{stopDateTime}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getSingleJobReportTxt(@PathParam("startDateTime") String startDateTime, @PathParam("stopDateTime") String stopDateTime) {
        try {
            List<UsageStatistic> allJobsBetween = jsonExtractor.getAllJobsBetween(startDateTime, stopDateTime);
            return generatorJobDetails(allJobsBetween);
        } catch (IOException exception) {
            return getErrorResponse(exception);
        }
    }

    private String generatorJobDetails(List<UsageStatistic> allJobsBetween) {
        Gson gson = new Gson();
        return gson.toJson(allJobsBetween);

    }

    private String getErrorResponse(Exception exception) {
        Map<String, String> responseMap = new HashMap<>();
        responseMap.put("Status", "Failed");
        responseMap.put("Message", exception.getMessage());
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(responseMap);
    }
}
