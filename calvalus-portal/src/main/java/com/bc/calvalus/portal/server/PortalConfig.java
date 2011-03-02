package com.bc.calvalus.portal.server;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.File;

/**
 * Configuration of the portal.
 *
 * @author Norman Fomferra
 */
public class PortalConfig {

    private final ServletContext servletContext;

    public PortalConfig(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    public String getInitParameter(String name) throws ServletException {
        String value = servletContext.getInitParameter(name);
        if (value == null) {
            throw new ServletException(String.format("Missing context parameter '%s'", name));
        }
        return value;
    }

    public String getInitParameter(String name, String defaultValue) {
        String value = servletContext.getInitParameter(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    public File getLocalUploadDir() throws ServletException {
        return new File(getLocalBaseDir(),
                        getInitParameter("calvalus.portal.upload.dir"));
    }

    public File getLocalDownloadDir() throws ServletException {
        return new File(getLocalBaseDir(),
                        getInitParameter("calvalus.portal.download.dir"));
    }

    public File getLocalBaseDir() {
        File defaultDir = new File(System.getProperty("user.home"), ".calvalus");
        return new File(getInitParameter("calvalus.portal.base.dir",
                                         defaultDir.getPath()));
    }
}
