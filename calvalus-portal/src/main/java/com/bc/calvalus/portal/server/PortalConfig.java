package com.bc.calvalus.portal.server;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.File;

/**
 * Configuration of the portal.
 *
 * @author Norman
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

    public String getStagingPath() throws ServletException {
        return getInitParameter("calvalus.portal.staging.path");
    }

    public String getUploadPath() throws ServletException {
        return getInitParameter("calvalus.portal.upload.path");
    }

    public File getLocalUploadDir() throws ServletException {
        return new File(getLocalContextDir(), getUploadPath());
    }

    public File getLocalStagingDir() throws ServletException {
        return new File(getLocalContextDir(), getStagingPath());
    }

    public File getLocalContextDir() {
        return new File(servletContext.getRealPath("."));
    }
}
