package com.bc.calvalus.experiments.processing;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Log format with pattern "yyyy-MM-dd HH:mm:ss,SSS level context message".
 *
 * @author Martin Boettcher
 */
public class LogFormatter extends Formatter {

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
