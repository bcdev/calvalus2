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

package com.bc.calvalus.portal.server;

import javax.servlet.ServletContext;
import java.util.logging.ErrorManager;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

/**
 * A log handler that delegates to a servlet context.
 */
public class ServletContextLogHandler extends StreamHandler {

    private final ServletContext servletContext;

    public ServletContextLogHandler(ServletContext servletContext) {
        this.servletContext = servletContext;
    }


    public synchronized void publish(LogRecord record) {
        if (isLoggable(record)) {
            try {
                String msg = getFormatter().format(record);
                servletContext.log(msg, record.getThrown());
            } catch (Exception ex) {
                // We don't want to throw an exception here, but we
                // report the exception to any registered ErrorManager.
                reportError(null, ex, ErrorManager.FORMAT_FAILURE);
            }
        }
    }

}
