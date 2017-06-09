package com.bc.calvalus.reporting.code.reader;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.urban.CursorPosition;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


/**
 * @author muhammad.bc.
 */
public class ReadJobDetail {
    private static final String CODE_DE_URL = "code.de.url";
    private static final int HTTP_SUCCESSFUL_CODE_START = 200;
    private static final int HTTP_SUCCESSFUL_CODE_END = 300;
    private static final String STATUS_FAILED = "\"Status\": \"Failed\"";
    private static final int MAX_LENGTH = 2;
    private static final Logger logger = CalvalusLogger.getLogger();
    private String jobDetailJson;
    private LocalDateTime cursorPosition;
    private LocalDateTime endDateTime;

    public ReadJobDetail() {
        CursorPosition cursorPosition = new CursorPosition();
        this.cursorPosition = cursorPosition.readLastCursorPosition();
        endDateTime = LocalDateTime.now();
        String url = createURL(this.cursorPosition, endDateTime);
        jobDetailJson = getJobDetailFromWS(url);
        if (jobDetailJson.length() > MAX_LENGTH && !jobDetailJson.isEmpty()) {
            cursorPosition.writeLastCursorPosition(endDateTime);
        }
    }

    public List<JobDetail> getJobDetail() {
        if (jobDetailJson == null || jobDetailJson.contains(STATUS_FAILED)) {
            return null;
        }
        Gson gson = new Gson();
        Type mapType = new TypeToken<List<JobDetail>>() {
        }.getType();
        return gson.fromJson(jobDetailJson, mapType);
    }

    public LocalDateTime startDateTime() {
        return cursorPosition;
    }

    public LocalDateTime endDateTime() {
        return endDateTime;
    }

    String createURL(LocalDateTime lastCursorPosition, LocalDateTime now) {
        String codeDeUrl = System.getProperty(CODE_DE_URL);
        return String.format(codeDeUrl + "%s/%s", lastCursorPosition.toString(), now.toString());
    }

    private String getJobDetailFromWS(String url) {
        try {
            Client client = ClientBuilder.newClient();
            WebTarget target = client.target(url);
            Invocation.Builder builder = target.request();
            Response response = builder.accept(MediaType.TEXT_PLAIN).get();
            int status = response.getStatus();
            if (status >= HTTP_SUCCESSFUL_CODE_START && status < HTTP_SUCCESSFUL_CODE_END) {
                return builder.get(String.class);
            } else {
                String msg = String.format("Error %s, %s status code %d ",
                                           response.getStatusInfo().getFamily(),
                                           response.getStatusInfo().getReasonPhrase(),
                                           status);
            }
        } catch (ProcessingException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
        return "";
    }
}
