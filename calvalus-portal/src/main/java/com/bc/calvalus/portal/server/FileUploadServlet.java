package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FilenameUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Servlet to handle file upload requests.
 * If the parameter "echo" is set to any value, the servlet
 * echoes the upload file back in its response.
 *
 * @author Norman
 */
public class FileUploadServlet extends HttpServlet {

    public BackendService getBackendService() {
         return (BackendService) getServletContext().getAttribute("calvalusPortal.backendService");
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        super.doGet(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        if (!ServletFileUpload.isMultipartContent(req)) {
            resp.sendError(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE,
                           "Request contents type is not supported by the servlet.");
            return;
        }
        PortalConfig portalConfig = new PortalConfig(getServletContext());
        final FileHandler fileHandler;
        if (req.getParameter("echo") != null) {
            fileHandler = new FileEchoHandler();
        } else {
            fileHandler = new FileStoreHandler(portalConfig.getLocalUploadDir());
        }
        ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
        try {
            List<FileItem> items = upload.parseRequest(req);
            for (FileItem item : items) {
                // process only file upload - discard other form item types
                if (item.isFormField()) {
                    continue;
                }
                fileHandler.handleFileItem(item, resp);
                resp.flushBuffer();
            }
        } catch (Exception e) {
            log("Error uploading a file", e);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public interface FileHandler {
        void handleFileItem(FileItem item, HttpServletResponse resp) throws Exception;
    }

    private class FileStoreHandler implements FileHandler {
        File uploadDir;

        public FileStoreHandler(File uploadDir) {
            this.uploadDir = uploadDir;
        }

        @Override
        public void handleFileItem(FileItem item, HttpServletResponse resp) throws Exception {
            File file = new File(uploadDir, FilenameUtils.getName(item.getName()));
            item.write(file);
            resp.setStatus(HttpServletResponse.SC_CREATED);
            resp.setHeader("content-type", "text/html");
            resp.getWriter().print(file.getPath());
            log("Downloaded file " + item + " to " + file);
        }
    }

    private class FileEchoHandler implements FileHandler {

        @Override
        public void handleFileItem(FileItem item, HttpServletResponse resp) throws Exception {
            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            resp.setHeader("content-type", "text/html");
            resp.getWriter().print(item.getString());
            log("Echoed file " + item);
        }
    }
}
