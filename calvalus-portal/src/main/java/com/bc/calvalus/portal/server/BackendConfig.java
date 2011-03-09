package com.bc.calvalus.portal.server;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.File;

/**
 * Configuration of the backend service of the portal.
 *
 * @author Norman
 */
public class BackendConfig {

    private final String stagingPath;
    private final String uploadPath;
    private File localContextDir;

    public BackendConfig(ServletContext servletContext) throws ServletException {
        stagingPath = getInitParameter(servletContext, "calvalus.portal.staging.path");
        uploadPath = getInitParameter(servletContext, "calvalus.portal.upload.path");
        localContextDir = new File(servletContext.getRealPath("."));
    }

    public String getStagingPath() {
        return stagingPath;
    }

    public String getUploadPath() {
        return uploadPath;
    }

    public File getLocalUploadDir() {
        return new File(getLocalContextDir(), getUploadPath());
    }

    public File getLocalStagingDir() {
        return new File(getLocalContextDir(), getStagingPath());
    }

    public File getLocalContextDir() {
        return localContextDir;
    }

    private static String getInitParameter(ServletContext servletContext, String name) throws ServletException {
        String value = servletContext.getInitParameter(name);
        if (value == null) {
            throw new ServletException(String.format("Missing context parameter '%s'", name));
        }
        return value;
    }

}
