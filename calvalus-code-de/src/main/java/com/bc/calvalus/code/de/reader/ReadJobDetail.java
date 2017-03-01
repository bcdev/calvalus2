package com.bc.calvalus.code.de.reader;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
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
    private static final int HTTP_SUCCESSFUL_CODE_START = 200;
    private static final int HTTP_SUCCESSFUL_CODE_END = 300;
    private static final String STATUS_FAILED = "\"Status\": \"Failed\"";
    private static final int MAX_LENGTH = 2;
    private String jobDetailJson;
    private static Logger logger = CalvalusLogger.getLogger();
    private LocalDateTime cursorPosition;
    private LocalDateTime endDateTime;

    public ReadJobDetail() {
        try {
            CursorPosition cursorPosition = new CursorPosition();
            this.cursorPosition = cursorPosition.readLastCursorPosition();
            endDateTime = LocalDateTime.now();
            String url = createURL(this.cursorPosition, endDateTime);
            jobDetailJson = getJobDetailFromWS(url);
            if (jobDetailJson.length() > MAX_LENGTH && !jobDetailJson.isEmpty()) {
                cursorPosition.writeLastCursorPosition(endDateTime);
            }
        } catch (CodeDeException e) {
            e.printStackTrace();
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

    String createURL(LocalDateTime lastCursorPosition, LocalDateTime now) {
        String codeDeUrl = PropertiesWrapper.get("report.ws.url");
        return String.format(codeDeUrl + "%s/%s", lastCursorPosition.toString(), now.toString());
    }

    private String getJobDetailFromWS(String url) throws CodeDeException {
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
                throw new CodeDeException(msg);
            }
        } catch (CodeDeException | ProcessingException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
        return "";
    }

    public LocalDateTime startDateTime() {
        return cursorPosition;
    }

    public LocalDateTime endDateTime() {
        return endDateTime;
    }

    static class CursorPosition implements Serializable {
        private LocalDateTime cursorPosition;

        LocalDateTime readLastCursorPosition() {
            try {
                File file = new File("cursor.ser");
                if (!file.exists()) {
                    LocalDateTime startDateTime = getStartDateTime();
                    if (startDateTime != null) {
                        return startDateTime;
                    }
                    return LocalDateTime.now().minusMinutes(5);
                }
                FileInputStream fileInputStream = new FileInputStream("cursor.ser");
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                CursorPosition cursorPosition = (CursorPosition) objectInputStream.readObject();
                return cursorPosition.cursorPosition;
            } catch (IOException | ClassNotFoundException e) {
                logger.log(Level.SEVERE, e.getMessage());
            } catch (CodeDeException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
            return LocalDateTime.MIN;
        }

        private LocalDateTime getStartDateTime() throws CodeDeException {
            LocalDateTime localDateTime = null;
            try {
                String startDateTime = PropertiesWrapper.get("start.date.time");
                localDateTime = LocalDateTime.parse(startDateTime);
            } catch (DateTimeParseException | NullPointerException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
            return localDateTime;
        }

        void writeLastCursorPosition(LocalDateTime localDateTime) {
            try {
                this.cursorPosition = localDateTime;
                FileOutputStream fileOutputStream = new FileOutputStream("cursor.ser");
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
                objectOutputStream.writeObject(this);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
