package com.bc.calvalus.urban.ws;

import com.bc.calvalus.code.de.CursorPosition;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.urban.account.Account;
import com.bc.calvalus.urban.account.Compound;
import com.bc.calvalus.urban.account.Message;
import com.bc.calvalus.urban.reporting.CalvalusReport;
import com.bc.calvalus.urban.reporting.ReadCalvalusReport;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.jcraft.jsch.JSchException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author muhammad.bc.
 */
public class WpsService {
    private static final String logAccountMessagePath = PropertiesWrapper.get("account.log.send.path");
    private static final String hostName = PropertiesWrapper.get("host.name");
    private final static Gson gson = new Gson();
    private Logger logger = CalvalusLogger.getLogger();
    private ReadCalvalusReport calvalusReport;
    private LocalDateTime lastCursorPosition;
    private CursorPosition cursorPosition;


    public WpsService() {
        try {

            cursorPosition = new CursorPosition();
            lastCursorPosition = cursorPosition.readLastCursorPosition();

            calvalusReport = new ReadCalvalusReport();
            CopyReportFile scpCommand = new CopyReportFile();
            Optional<BufferedReader> bufferedReaderOptional = scpCommand.readFile(lastCursorPosition.toString());
            BufferedReader bufferedReader = bufferedReaderOptional.get();

        } catch (IOException | JSchException e) {
            logger.log(Level.WARNING, String.format("Read remote file error %s ", e.getMessage()));
        }
    }

    public void getReportLog(BufferedReader bufferedReader) throws IOException {
        String readLine = null;
        Gson gson = new Gson();
        while ((readLine = bufferedReader.readLine()) != null) {
            String[] split = readLine.split("\t");

            WpsReport wpsReport = new WpsReport(
                    split[0],
                    split[1],
                    split[2],
                    split[3]);
            if (!lastCursorPosition.isAfter(wpsReport.getFinishDateTime())) {
                handleNewWpsReport(wpsReport);
                cursorPosition.writeLastCursorPosition(wpsReport.getFinishDateTime());
            }
        }
    }

    private void writeMessage(String jobId, Message message) {
        File file = new File(this.logAccountMessagePath, String.format("account-message-%s.json", jobId));
        try (FileWriter fileWriter = new FileWriter(file)) {
            fileWriter.append(gson.toJson(message)).append('\n');
        } catch (IOException e) {
            CalvalusLogger.getLogger().log(Level.WARNING, e.getMessage());
        }
    }

    private void handleNewWpsReport(WpsReport wpsReport) {
        LocalDate finishDateTime = wpsReport.getFinishDateTime().toLocalDate();
        Optional<CalvalusReport> jobSummary = calvalusReport.getReport(wpsReport.getJobID(), finishDateTime.toString());
        Message message = createMessage(jobSummary.get(), wpsReport);
        writeMessage(message.getJobID(), message);
    }

    private Message createMessage(CalvalusReport jobSummary, WpsReport wps) {
        checkNotNull(jobSummary, "Calvalus report does not exist.");
        Account account = new Account("", jobSummary.getUser(), wps.getAccRef());
        Date timestamp = new Date(jobSummary.getFinishTime());
        Compound compound = new Compound(wps.getCompID(), jobSummary.getJobName(), jobSummary.getProcessType(), wps.getUri().toString(), timestamp);

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
