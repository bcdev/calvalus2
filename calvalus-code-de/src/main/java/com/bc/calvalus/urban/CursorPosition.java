package com.bc.calvalus.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CursorPosition {
    public static final String CURSOR_FILE = "cursor.txt";
    private static Logger logger = CalvalusLogger.getLogger();
    private static LocalDateTime cursorPosition;

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
        File file = new File(CURSOR_FILE);
        LocalDateTime readLastDate = null;
        if (!file.exists()) {
            readLastDate = getStartDateTime();
            if (readLastDate != null) {
                return readLastDate;
            }
            return LocalDateTime.now().minusMinutes(5);
        } else {
            CursorPosition cursorPosition = null;
            try (FileInputStream fileInputStream = new FileInputStream(CURSOR_FILE);
                 BufferedReader bufferedReader = new BufferedReader(new FileReader(CURSOR_FILE))) {

                readLastDate = LocalDateTime.parse(bufferedReader.readLine());
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
            return readLastDate;
        }
    }

    public synchronized void writeLastCursorPosition(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            throw new NullPointerException("The last date time most not be null");
        }
        try (FileOutputStream fileOutputStream = new FileOutputStream(CURSOR_FILE);
             BufferedWriter bufferedReader = new BufferedWriter(new FileWriter(CURSOR_FILE))) {

            bufferedReader.write(localDateTime.toString());
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }
}