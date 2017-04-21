package com.bc.calvalus.code.de;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.wps.utilities.PropertiesWrapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CursorPosition {
    private static Logger logger = CalvalusLogger.getLogger();
    private static LocalDateTime cursorPosition;

    private static LocalDateTime getStartDateTime() {
        LocalDateTime localDateTime = null;
        try {
            String startDateTime = PropertiesWrapper.get("start.date.time");
            localDateTime = LocalDateTime.parse(startDateTime);
        } catch (DateTimeParseException e) {
            logger.log(Level.WARNING, e.getMessage());
        } catch (NullPointerException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
        return localDateTime;
    }

    public synchronized LocalDateTime readLastCursorPosition() {
        File file = new File("cursor.ser");
        if (!file.exists()) {
            LocalDateTime startDateTime = getStartDateTime();
            if (startDateTime != null) {
                return startDateTime;
            }
            return LocalDateTime.now().minusMinutes(5);
        } else {
            CursorPosition cursorPosition = null;
            try (FileInputStream fileInputStream = new FileInputStream("cursor.txt");
                 ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
                cursorPosition = (CursorPosition) objectInputStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
            return cursorPosition.cursorPosition;
        }
    }

    //todo String format
    public synchronized void writeLastCursorPosition(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            throw new NullPointerException("The last date time most not be null");
        }
        try (FileOutputStream fileOutputStream = new FileOutputStream("cursor.txt");
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeChars(localDateTime.toString());
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }
}