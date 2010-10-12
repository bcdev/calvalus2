package com.bc.calvalus.experiments.processing;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class LogFormatter extends Formatter {
    
    @Override
    public String format(LogRecord logRecord) {
        StringBuilder sb = new StringBuilder(MessageFormat.format("{0}: {1} - {2}\n",
                                                                  logRecord.getLevel(),
                                                                  new Date(logRecord.getMillis()),
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
