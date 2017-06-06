package com.bc.calvalus.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.urban.account.Account;
import com.bc.calvalus.urban.account.Any;
import com.bc.calvalus.urban.account.Compound;
import com.bc.calvalus.urban.account.Message;
import com.bc.calvalus.urban.account.Quantity;
import com.bc.calvalus.urban.reporting.CalvalusReport;
import com.bc.calvalus.urban.reporting.ReadCalvalusReport;
import com.bc.calvalus.urban.ws.CopyWpsRemoteFile;
import com.bc.calvalus.urban.ws.WpsReport;
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
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;


/**
 * @author muhammad.bc.
 */
public class SendWriteMessage {

    public static final String REMOVE_TEP_PREFIX = "tep_";
    public static final String COM_BC_CALVALUS_URBAN_SEND = "com.bc.calvalus.urban.send";
    private final CopyWpsRemoteFile scpCommand;
    private final Logger logger = CalvalusLogger.getLogger();
    private final ReadCalvalusReport calvalusReport;
    private final CursorPosition cursorPosition;
    private final String logAccountMessagePath = LoadProperties.getInstance().getLogAccountMessagePath();
    private final String hostName = LoadProperties.getInstance().getHostName();
    private final String platform = LoadProperties.getInstance().getPlatForm();
    private Client clientConnection;


    public SendWriteMessage() {
        calvalusReport = new ReadCalvalusReport();
        scpCommand = new CopyWpsRemoteFile();
        cursorPosition = new CursorPosition();
    }

    public void start() {
        try {
            logger.log(Level.INFO, "Start....");
            LocalDateTime lastCursorPosition = cursorPosition.readLastCursorPosition();
            BufferedReader bufferedReader = scpCommand.readRemoteFile();
            startSendWriteMessage(bufferedReader, lastCursorPosition);
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage());
        } catch (JSchException e) {
            logger.log(Level.WARNING, String.format("Read remote file error %s ", e.getMessage()));
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, e.getMessage());
            e.printStackTrace();
        }
    }


    private void startSendWriteMessage(BufferedReader bufferedReader, LocalDateTime lastCursorPosition) throws IOException, DateTimeParseException {
        String readLine;
        LocalDateTime lastDateTime;
        while ((readLine = bufferedReader.readLine()) != null) {
            String[] split = readLine.split("\t");
            String jobID = split[1];
            String accRef = split[3];
            String compID = split[4];
            String status = split[5];
            String uri = split[6];
            String finishDateTime = split[2];
            WpsReport wpsReport = new WpsReport(
                    jobID, // jobID
                    accRef, // ref
                    compID, // Compounf ID
                    status, // Status
                    compID, // Host name
                    uri, // URI
                    finishDateTime  // FinishDateTime
            );
            lastDateTime = parseLocalDateTime(finishDateTime, jobID);
            if (Objects.nonNull(lastDateTime) && !lastCursorPosition.isAfter(lastDateTime)) {
                logger.log(Level.INFO, String.format("JobID %s processing ..."));
                handleNewWpsReport(wpsReport);
            } else {
                logger.log(Level.INFO, String.format("JobID %s before %s skipped.", wpsReport.getJobID(), String.valueOf(lastCursorPosition)));
            }
        }
    }

    private LocalDateTime parseLocalDateTime(String strDate, String jobID) {
        try {
            return Instant.parse(strDate).atZone(ZoneId.systemDefault()).toLocalDateTime();
        } catch (DateTimeParseException e) {
            String msg = String.format("Date is not properly formatted, Check the remote file, where jobID" +
                                                             " %s,%s", jobID, e.getMessage());
            logger.log(Level.WARNING, msg);
        }
        return null;
    }

    private void writeMessage(String jobId, String message) {
        logger.log(Level.INFO, String.format("Writing message with %s jobID to file", jobId));
        File file = new File(logAccountMessagePath, String.format("account-message-%s.json", jobId));
        try (FileWriter fileWriter = new FileWriter(file)) {
            fileWriter.append(message).append('\n');
        } catch (IOException e) {
            CalvalusLogger.getLogger().log(Level.WARNING, e.getMessage());
        }
    }

    private void handleNewWpsReport(WpsReport wpsReport) {
        String finishDateTime = wpsReport.getFinishDateTime();
        String filePatternName = formatDate(finishDateTime, "yyyy-MM-dd");
        Optional<CalvalusReport> reportOptional = this.calvalusReport.getReport(wpsReport.getJobID(), filePatternName);
        if (reportOptional.isPresent()) {
            Message message = createMessage(reportOptional.get(), wpsReport);
            writeMessage(message.getId(), message.toJson());
            boolean isDeveloper = Boolean.getBoolean(COM_BC_CALVALUS_URBAN_SEND);
            if (!isDeveloper) {
                sendMessage(message.toJson());
            } else {
                CalvalusLogger.getLogger().warning("skip sending message " + wpsReport.getJobID());
            }
            cursorPosition.writeLastCursorPosition(Instant.parse(finishDateTime).atZone(ZoneId.systemDefault()).toLocalDateTime());
        } else {
            logger.log(Level.INFO, String.format("JobID %s is not present in the calvalus reporting.", wpsReport.getJobID()));
        }
    }


    private void sendMessage(String message) {
        logger.log(Level.INFO, "Send messages");
        LoadProperties loadProperties = LoadProperties.getInstance();
        String accountServerUrl = loadProperties.getAccountServerUrl();
        if (clientConnection == null) {
            clientConnection = getClientConnection(loadProperties);
        }
        WebTarget target = clientConnection.target(accountServerUrl);
        Response response = target.request(MediaType.TEXT_PLAIN_TYPE).post(Entity.json(message));
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

    private List<Quantity> createQuantities(CalvalusReport jobSummary) {
        return Arrays.asList(
                new Quantity("CPU_MILLISECONDS", jobSummary.getCpuMilliseconds()),
                new Quantity("PHYSICAL_MEMORY_BYTES", jobSummary.getCpuMilliseconds() == 0 ? (jobSummary.getMbMillisMapTotal() + jobSummary.getMbMillisReduceTotal()) * 1024 * 1024 : (jobSummary.getMbMillisMapTotal() + jobSummary.getMbMillisReduceTotal()) / jobSummary.getCpuMilliseconds() * 1024 * 1024),
                new Quantity("BYTE_READ", jobSummary.getHdfsBytesRead()),
                new Quantity("BYTE_WRITTEN", jobSummary.getHdfsBytesWritten()),
                new Quantity("PROC_INSTANCE", (long) jobSummary.getMapsCompleted() + jobSummary.getReducesCompleted()),
                new Quantity("NUM_REQ", 1));
    }

    private Message createMessage(CalvalusReport jobSummary, WpsReport wps) {
        Objects.requireNonNull(jobSummary, "Calvalus report does not exist.");

        String userName = jobSummary.getUser().replace(REMOVE_TEP_PREFIX, "");
        Account account = new Account(platform, userName, jobSummary.getRemoteRef());
        Instant timestamp = new Date(jobSummary.getFinishTime()).toInstant();

        Compound compound = new Compound(wps.getCompID(),
                                         jobSummary.getJobName(),
                                         jobSummary.getProcessType(),
                                         new Any(wps.getUri()));
        List<Quantity> quantityList = createQuantities(jobSummary);
        return new Message(jobSummary.getJobId(),
                           account,
                           compound,
                           quantityList,
                           hostName,
                           timestamp.toString(),
                           "SUCCEEDED".equals(jobSummary.getState()) ? "NOMINAL" : "DEGRADED");
    }
}
