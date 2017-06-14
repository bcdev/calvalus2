package com.bc.calvalus.reporting.code.reader;

import com.bc.calvalus.commons.CalvalusLogger;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CursorPosition {
    private static final String cursorFilePath = LoadProperties.getInstance().getCursorFileLocation();
    private static final Logger logger = CalvalusLogger.getLogger();

    private static LocalDateTime getStartDateTime() {
        LocalDateTime localDateTime = null;
        try {
            String startDateTime = LoadProperties.getInstance().getInitStartTime();
            localDateTime = LocalDateTime.parse(startDateTime);
        } catch (DateTimeParseException e) {
            logger.log(Level.WARNING, e.getMessage());
        } catch (NullPointerException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
        return localDateTime;
    }

    public synchronized LocalDateTime readLastCursorPosition() {
        File file = new File(cursorFilePath);
        if (!file.exists()) {
            return getStartDateTime();
        } else {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(cursorFilePath))) {
                return LocalDateTime.parse(bufferedReader.readLine());
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
            return getStartDateTime();
        }
    }

    public synchronized void writeLastCursorPosition(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            throw new NullPointerException("The last date time most not be null");
        }
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(cursorFilePath))) {
            bufferedWriter.write(localDateTime.toString());
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

}