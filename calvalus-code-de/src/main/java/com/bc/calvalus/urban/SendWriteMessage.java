package com.bc.calvalus.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.urban.account.Account;
import com.bc.calvalus.urban.account.Compound;
import com.bc.calvalus.urban.account.Message;
import com.bc.calvalus.urban.reporting.CalvalusReport;
import com.bc.calvalus.urban.reporting.ReadCalvalusReport;
import com.bc.calvalus.urban.ws.CopyWpsRemoteFile;
import com.bc.calvalus.urban.ws.WpsReport;
import com.google.gson.Gson;
import com.jcraft.jsch.JSchException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;


/**
 * @author muhammad.bc.
 */
public class SendWriteMessage {


    private final static Gson gson = new Gson();
    private final CopyWpsRemoteFile scpCommand;
    private final Logger logger = CalvalusLogger.getLogger();
    private final ReadCalvalusReport calvalusReport;
    private final CursorPosition cursorPosition;
    private final String logAccountMessagePath = LoadProperties.getInstance().getLogAccountMessagePath();
    private final String hostName = LoadProperties.getInstance().getHostName();
    private Client clientConnection;


    public SendWriteMessage() {
        cursorPosition = new CursorPosition();
        calvalusReport = new ReadCalvalusReport();
        scpCommand = new CopyWpsRemoteFile();
    }

    public void start() {
        try {
            logger.log(Level.INFO, "Start....");
            LocalDateTime lastCursorPosition = cursorPosition.readLastCursorPosition();
            String formatDateToStr = formatDate(lastCursorPosition.toString(), "yyyy-MM");
            BufferedReader bufferedReader = scpCommand.readRemoteFile(formatDateToStr);
            startSendWriteMessage(bufferedReader, lastCursorPosition);
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage());
        } catch (JSchException e) {
            logger.log(Level.WARNING, String.format("Read remote file error %s ", e.getMessage()));
        }
    }


    private void startSendWriteMessage(BufferedReader bufferedReader, LocalDateTime lastCursorPosition) throws IOException {
        String readLine;
        LocalDateTime lastDateTime;
        while ((readLine = bufferedReader.readLine()) != null) {
            String[] split = readLine.split("\t");
            WpsReport wpsReport = new WpsReport(
                    split[1], // jobID
                    split[3], // ref
                    split[4], // Compounf ID
                    split[5], // Status
                    split[4], // Host name
                    split[6], // URI
                    split[2]  // FinishDateTime
            );
            Instant parse = Instant.parse(split[2]);
            lastDateTime = parse.atZone(ZoneId.systemDefault()).toLocalDateTime();
            if (!lastCursorPosition.isAfter(lastDateTime)) {
                handleNewWpsReport(wpsReport);
            }
            cursorPosition.writeLastCursorPosition(lastDateTime);
        }
    }

    private void writeMessage(String jobId, Message message) {
        logger.log(Level.INFO, String.format("Writing message with %s jobID to file", jobId));
        File file = new File(logAccountMessagePath, String.format("account-message-%s.json", jobId));
        try (FileWriter fileWriter = new FileWriter(file)) {
            fileWriter.append(gson.toJson(message)).append('\n');
        } catch (IOException e) {
            CalvalusLogger.getLogger().log(Level.WARNING, e.getMessage());
        }
    }

    private void handleNewWpsReport(WpsReport wpsReport) {
        String filePatternName = formatDate(wpsReport.getFinishDateTime(), "yyyy-MM-dd");
        Optional<CalvalusReport> reportOptional = this.calvalusReport.getReport(wpsReport.getJobID(), filePatternName);
        if (reportOptional.isPresent()) {
            Message message = createMessage(reportOptional.get(), wpsReport);
            writeMessage(message.getJobID(), message);
            sendMessage(message);
        }

    }


    private void sendMessage(Message message) {
        logger.log(Level.INFO, "Send messages");
        LoadProperties loadProperties = LoadProperties.getInstance();
        String accountServerUrl = loadProperties.getAccountServerUrl();
        if (clientConnection == null) {
            clientConnection = getClientConnection(loadProperties);
        }
        String toJson = gson.toJson(message);
        Response response = clientConnection.target(accountServerUrl).request(MediaType.TEXT_PLAIN_TYPE).post(Entity.json(toJson));
        String reasonPhrase = response.getStatusInfo().getReasonPhrase();
        logger.log(Level.INFO, "Server response " + reasonPhrase);
    }

    private Client getClientConnection(LoadProperties instance) {
        String userName = instance.getUserName();
        String password = instance.getPassword();
        HttpAuthenticationFeature feature = HttpAuthenticationFeature.basicBuilder()
                .nonPreemptive().credentials(userName, password).build();
        Client client = ClientBuilder.newClient();
        client.register(feature);
        return client;
    }

    private String formatDate(String dateString, String pattern) {
        String reformattedStr = null;
        try {
            SimpleDateFormat myFormat = new SimpleDateFormat(pattern);
            reformattedStr = myFormat.format(myFormat.parse(dateString));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return reformattedStr;
    }


    private Message createMessage(CalvalusReport jobSummary, WpsReport wps) {
        Objects.requireNonNull(jobSummary, "Calvalus report does not exist.");

        Account account = new Account("", jobSummary.getUser(), wps.getAccRef());
        Instant timestamp = new Date(jobSummary.getFinishTime()).toInstant();

        Compound compound = new Compound(wps.getCompID(),
                                         jobSummary.getJobName(),
                                         jobSummary.getProcessType(),
                                         wps.getUri(),
                                         timestamp.toString());

        Map<String, Long> quantity = new HashMap<>();
        quantity.put("CPU_MILLISECONDS", jobSummary.getCpuMilliseconds());
        quantity.put("PHYSICAL_MEMORY_BYTES", jobSummary.getCpuMilliseconds());
        quantity.put("BYTE_WRITTEN", jobSummary.getFileBytesWritten());
        quantity.put("PROC_INSTANCE", (long) jobSummary.getMapsCompleted());
        quantity.put("NUM_REQ", (long) jobSummary.getReducesCompleted());

        return new Message(jobSummary.getJobId(),
                           account,
                           compound,
                           quantity,
                           hostName,
                           Instant.now().toString(),
                           jobSummary.getState());
    }

}
