/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.commons;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;

/**
 * The logger for calvalus
 */
public class CalvalusLogger {

    private static final Formatter calvalusFormatter = new LogFormatter();
    private static final Handler calvalusHandler = new ConsoleHandler();
    private static final Logger calvalusLogger = createLogger();

    public static Logger getLogger() {
        return calvalusLogger;
    }

    private static Logger createLogger() {
        Logger logger = Logger.getLogger("com.bc.calvalus");
        logger.setUseParentHandlers(false);
        calvalusHandler.setFormatter(calvalusFormatter);
        logger.addHandler(calvalusHandler);
        logger.setLevel(Level.INFO);

        Logger snapLogger = Logger.getLogger("org.esa.snap");
        snapLogger.setUseParentHandlers(false);
        snapLogger.addHandler(calvalusHandler);
        snapLogger.setLevel(Level.INFO);

        Logger hadoopLogger = Logger.getLogger("org.apache.hadoop");
        hadoopLogger.setUseParentHandlers(false);
        hadoopLogger.addHandler(calvalusHandler);
        hadoopLogger.setLevel(Level.SEVERE);

        Logger rootLogger = Logger.getLogger("");
        rootLogger.setUseParentHandlers(false);
        rootLogger.addHandler(calvalusHandler);
        rootLogger.setLevel(Level.SEVERE);
        return logger;
    }

    /**
     * The start of the SNAP Engine replace our formatter with another one
     * @see org.esa.snap.runtime.EngineConfig#replaceConsoleLoggerFormatter(java.util.logging.Logger)
     */
    public static void restoreCalvalusLogFormatter() {
        calvalusHandler.setFormatter(calvalusFormatter); 
    }

    /**
     * Log format with pattern "yyyy-MM-dd HH:mm:ss,SSS level context message".
     *
     * @author Martin Boettcher
     */
    private static class LogFormatter extends Formatter {

        private static final SimpleDateFormat LOG_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss,SSS");

        @Override
        public String format(LogRecord logRecord) {
            StringBuilder sb = new StringBuilder(MessageFormat.format("{0} {1} {2}: {3}\n",
                                                                      LOG_TIMESTAMP_FORMAT.format(new Date(logRecord.getMillis())),
                                                                      logRecord.getLevel(),
                                                                      logRecord.getLoggerName(),
                                                                      logRecord.getMessage()));
            Throwable throwable = logRecord.getThrown();
            if (throwable != null) {
                StringWriter writer = new StringWriter();
                throwable.printStackTrace(new PrintWriter(writer));
                sb.append(writer.toString());
                sb.append("\n");
            }
            return sb.toString();
        }
    }
}
