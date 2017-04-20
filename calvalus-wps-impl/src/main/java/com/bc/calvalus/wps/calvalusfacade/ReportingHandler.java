package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionService;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * TODO add API doc
 *
 * @author Martin
 * @author Muhammad
 */
public class ReportingHandler implements Observer {
    private String reportPathString;

    private ReportingHandler(ProductionService productionService, Map<String, String> config) {
        reportPathString = config.get("wps.reporting.db.path");
        CalvalusLogger.getLogger().info("reporting handler created to log into " + reportPathString);
    }

    public static ReportingHandler createReportHandler(ProductionService productionService, Map<String, String> config) {
        return SingletonReportingHandler.reportingHandler;
    }

    @Override
    public void update(Observable o, Object arg) {
        Production production = (Production) arg;
        logToFile(production, reportPathString);
        CalvalusLogger.getLogger().info("request " + production.getName() + " reported " + production.getProcessingStatus() + " should be logged in " + reportPathString);
    }

    private void logToFile(Production production, String reportPathString) {
        Date stopDateTime = production.getWorkflow().getStopTime();
        try {
            Path reportPath = Paths.get(reportPathString);
            if (Files.exists(reportPath)) {
                throw new FileNotFoundException(String.format("The file reportPath %s doest not exist", reportPath));
            }
            String filePathToLog = getFilePathToLog(reportPathString, stopDateTime);
            writeLog(filePathToLog);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getFilePathToLog(String reportPathString, Date stopDateTime) throws IOException {
        Predicate predicate = getPredictFileNameByDate(stopDateTime);
        File reportFile = new File(reportPathString);
        if (!reportFile.isDirectory()) {
            throw new IOException("The file dirstory does not exist ");
        }
        Stream<String> fileNameStream = Arrays.stream(reportFile.list());
        Optional<String> fileNameOptional = fileNameStream.filter(predicate).findFirst();
        return fileNameOptional.orElseGet(() -> createFile(stopDateTime, reportPathString).getPath());
    }

    private void writeLog(String fileToLogIn) {

    }

    private Predicate<String> getPredictFileNameByDate(Date stopDateTime) {
        return fileName -> {
//            2017-04-20T08:05:27.959Z
            Matcher matcher = groupMatchers(fileName);
            if (!matcher.find()) {
                return false;
            }
            String startDateFrmFileName = matcher.group(1);
            String endDateFrmFileName = matcher.group(3);

            LocalDate end = LocalDate.parse(endDateFrmFileName);
            LocalDate start = LocalDate.parse(startDateFrmFileName);

            LocalDate instant = LocalDate.parse(stopDateTime.toInstant().toString());
            return (start.isBefore(instant) || start.equals(instant)) && (end.isAfter(instant) || end.equals(instant));
        };
    }

    private File createFile(Date dateTime, String reportPathString) {
        String fileNameFormat = fileNameFormat(dateTime);
        File file = Paths.get(reportPathString).resolve(fileNameFormat).toFile();
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
        }
        return file;
    }

    private String fileNameFormat(Date dateAsFileName) {
        LocalDate localDate = LocalDate.from(Instant.ofEpochMilli(dateAsFileName.getTime()));
        String fisrtDayDate = localDate.withDayOfMonth(0).toString();
        String lastDayDate = localDate.plusMonths(1).withDayOfMonth(1).minusDays(1).toString();
        return String.format("calvalus-wps-%s-to-%s.json", fisrtDayDate, lastDayDate);
    }


    private Matcher groupMatchers(String aLong) {
        Pattern compile = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})[_-]('*\\w*)[_-](\\d{4}-\\d{2}-\\d{2})");
        return compile.matcher(aLong);
    }


    private static class SingletonReportingHandler {
        static ReportingHandler reportingHandler;

        SingletonReportingHandler(ProductionService productionService, Map<String, String> config) {
            reportingHandler = new ReportingHandler(productionService, config);
        }
    }
}
