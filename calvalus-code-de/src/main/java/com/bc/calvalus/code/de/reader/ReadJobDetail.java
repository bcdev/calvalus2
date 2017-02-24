package com.bc.calvalus.code.de.reader;

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
import java.util.List;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientConfig;


/**
 * @author muhammad.bc.
 */
public class ReadJobDetail {
    private static final int HTTP_SUCCESSFUL_CODE_START = 200;
    private static final int HTTP_SUCCESSFUL_CODE_END = 300;
    private static final String STATUS_FAILED = "\"Status\": \"Failed\"";
    private final String jobDetailJson;

    public ReadJobDetail() {
        EntryCursor entryCursor = new EntryCursor();
        LocalDateTime lastCursorPosition = entryCursor.readLastCursorPosition();
//        String url = createURL(lastCursorPosition);
        String url = "http://localhost:8040/calvalus-reporting/code-de/date/2017-02-20T00:00:00.019/2017-02-20T00:40:00.019";
        jobDetailJson = queryWebService(url);
    }


    public List<JobDetail> getJobDetail() {
        if (jobDetailJson == null || jobDetailJson.contains(STATUS_FAILED)) {
            return null;
        }
        Gson gson = new Gson();
        Type mapType = new TypeToken<List<JobDetail>>() {
        }.getType();
        List<JobDetail> jobDetails = gson.fromJson(jobDetailJson, mapType);
        return jobDetails;
    }

    String createURL(LocalDateTime lastCursorPosition) {
        String codeDeUrl = PropertiesWrapper.get("code.de.url");
        return String.format(codeDeUrl + "%s/%s", lastCursorPosition.toString(), LocalDateTime.now());
    }

    String queryWebService(String url) {
        ClientConfig clientConfig = new ClientConfig();
        Client client = ClientBuilder.newClient(clientConfig);
        Invocation.Builder builder = client.target(url).request();
        Response response = builder.accept(MediaType.APPLICATION_JSON).get();
        int status = response.getStatus();
        if (status >= HTTP_SUCCESSFUL_CODE_START && status < HTTP_SUCCESSFUL_CODE_END) {
            return builder.get(String.class);
        }
        System.out.println("Whattttttttttt");
        return null;
    }

    static class EntryCursor implements Serializable {
        private LocalDateTime cursorPosition;

        LocalDateTime readLastCursorPosition() {
            try {
                File file = new File("cursor.ser");
                if (!file.exists()) {
                    return LocalDateTime.now();
                }
                FileInputStream fileInputStream = new FileInputStream("cursor.ser");
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                EntryCursor entryCursor = (EntryCursor) objectInputStream.readObject();
                return entryCursor.cursorPosition;
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            //todo mba**** what is the right thing to do
            return LocalDateTime.MIN;
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
