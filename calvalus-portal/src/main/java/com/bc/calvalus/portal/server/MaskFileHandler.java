package com.bc.calvalus.portal.server;

import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.production.ServiceContainer;
import org.apache.commons.fileupload.FileItem;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.bc.calvalus.portal.server.BackendServiceImpl.getUserName;

/**
 * @author Marco Peters
 * @author Thomas Storm
 */
class MaskFileHandler implements FileUploadServlet.FileHandler {

    private final ServletContext context;

    MaskFileHandler(ServletContext context) {
        this.context = context;
    }

    @Override
    public void handleFileItem(HttpServletRequest req, HttpServletResponse resp, FileItem item) throws Exception {
        String maskDirPath = getSpecifiedDirectory(req);
        final String userName = getUserName(req);

        ServiceContainer serviceContainer = (ServiceContainer) context.getAttribute("serviceContainer");

        String filePath = maskDirPath + item.getName();
        String userPath = AbstractFileSystemService.getUserPath(userName, filePath);
        OutputStream out = new BufferedOutputStream(serviceContainer.getFileSystemService().addFile(userName, userPath), 64 * 1024);
        copy(item.getInputStream(), out);

        resp.setStatus(HttpServletResponse.SC_ACCEPTED);
        resp.setHeader("content-type", "text/html");
        resp.getWriter().print(item.getName());
        context.log("Copied file " + item + " for user '" + getUserName(req) + "' to inventory");
    }

    private String getSpecifiedDirectory(HttpServletRequest req) {
        String relPath = req.getParameter("dir");
        String maskDirPath = "";
        if (relPath != null) {
            maskDirPath = relPath + "/";
        }
        return maskDirPath;
    }

    private void copy(InputStream in, OutputStream out) throws IOException {
        final byte[] data = new byte[1024];
        try {
            while (true) {
                int numBytes = in.read(data);
                if (numBytes == -1) {
                    break;
                }
                out.write(data, 0, numBytes);
            }
        } finally {
            in.close();
            out.close();
        }
    }

}
