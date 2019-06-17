package com.bc.calvalus.portal.server;

import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.ServiceContainerFactory;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FilenameUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static com.bc.calvalus.portal.server.BackendServiceImpl.getUserName;

/**
 * Servlet to handle file upload requests.
 * <p/>
 * The action is {@code "/calvalus/upload[?<parameters>]"}, where parameters are
 * <ul>
 * <li>{@code echo=1} The file contents will be echoed in the response.</li>
 * <li>{@code local=1} The file will be stored locally.</li>
 * <li>{@code dir=<rel-path>} The relative path to the directory into which the file(s) will be written.</li>
 * </ul>
 * If neither {@code echo=1} nor {@code local=1}, the file will be copied into the user's inventory space.
 *
 * @author Norman
 */
public class FileUploadServlet extends HttpServlet {

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
                           "The request's content type is not supported.");
            return;
        }
        BackendConfig backendConfig = new BackendConfig(getServletContext());
        final FileHandler fileHandler;
        if ("text".equals(req.getParameter("echo"))) {
            fileHandler = new EchoFileHandler();
        } else if ("xml".equals(req.getParameter("echo"))) {
            fileHandler = new EchoXMLFileHandler();
        } else if ("1".equals(req.getParameter("local"))) {
            fileHandler = new LocalFileHandler(backendConfig.getLocalUploadDir());
        } else if (Boolean.TRUE.equals(Boolean.valueOf(req.getParameter("bundle")))) {
            fileHandler = new BundleFileHandler(getServletContext());
        } else if (Boolean.TRUE.equals(Boolean.valueOf(req.getParameter("mask")))) {
            fileHandler = new MaskFileHandler(getServletContext());
        } else {
            fileHandler = new InventoryFileHandler();
        }
        ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
        if (getServletContext().getAttribute("serviceContainer") == null) {
            try {
                Class<?> productionServiceFactoryClass = Class.forName(
                        backendConfig.getProductionServiceFactoryClassName());
                ServiceContainerFactory serviceContainerFactory = (ServiceContainerFactory) productionServiceFactoryClass.newInstance();
                ServiceContainer serviceContainer = serviceContainerFactory.create(backendConfig.getConfigMap(),
                                                                                   backendConfig.getLocalAppDataDir(),
                                                                                   backendConfig.getLocalStagingDir());
                // Make the production servlet accessible by other servlets:
                getServletContext().setAttribute("serviceContainer", serviceContainer);
            } catch (Exception e) {
                throw new ServletException(e);
            }
        }
        try {
            List<FileItem> items = upload.parseRequest(req);
            for (FileItem item : items) {
                // process only file upload - discard other form item types
                if (!item.isFormField()) {
                    fileHandler.handleFileItem(req, resp, item);
                    resp.flushBuffer();
                }
            }
        } catch (Exception e) {
            log("Error uploading a file", e);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.getWriter().print(e.getMessage());
        }
    }

    public interface FileHandler {

        void handleFileItem(HttpServletRequest req, HttpServletResponse resp, FileItem item) throws Exception;
    }

    private class EchoFileHandler implements FileHandler {

        @Override
        public void handleFileItem(HttpServletRequest req, HttpServletResponse resp, FileItem item) throws Exception {
            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            resp.setHeader("content-type", "text/html");
            resp.getWriter().print(item.getString());
            log("Echoed file " + item);
        }
    }

    private class EchoXMLFileHandler implements FileHandler {

        @Override
        public void handleFileItem(HttpServletRequest req, HttpServletResponse resp, FileItem item) throws Exception {
            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            resp.setHeader("content-type", "text/html");
            resp.getWriter().print(encodeXML(item.getString()));
            log("Echoed file " + item);
        }

    }

    public static String encodeXML(String xml) {
        return xml.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
    }

    private class LocalFileHandler implements FileHandler {

        File uploadDir;

        public LocalFileHandler(File uploadDir) {
            this.uploadDir = uploadDir;
        }

        @Override
        public void handleFileItem(HttpServletRequest req, HttpServletResponse resp, FileItem item) throws Exception {
            File uploadDir = new File(this.uploadDir, getUserName(req));
            String relPath = req.getParameter("dir");
            if (relPath != null) {
                uploadDir = new File(uploadDir, relPath);
            }

            File file = new File(uploadDir, FilenameUtils.getName(item.getName()));
            item.write(file);
            resp.setStatus(HttpServletResponse.SC_CREATED);
            resp.setHeader("content-type", "text/html");
            resp.getWriter().print(file.getPath());
            log("Downloaded file " + item + " to " + file);
        }

    }

    private class InventoryFileHandler implements FileHandler {

        @Override
        public void handleFileItem(HttpServletRequest req, HttpServletResponse resp, FileItem item) throws Exception {
            String filePath = FilenameUtils.getName(item.getName());
            String relPath = req.getParameter("dir");
            if (relPath != null) {
                filePath = relPath + "/" + filePath;
            }


            ServiceContainer serviceContainer = (ServiceContainer) getServletContext().getAttribute("serviceContainer");
            FileSystemService fileSystemService = serviceContainer.getFileSystemService();

            String userName = getUserName(req);
            String userPath = AbstractFileSystemService.getUserPath(userName, filePath);

            InputStream in = new BufferedInputStream(item.getInputStream(), 64 * 1024);
            try {
                OutputStream out = new BufferedOutputStream(fileSystemService.addFile(userName, userPath), 64 * 1024);
                try {
                    copy(in, out);
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            }
            resp.setStatus(HttpServletResponse.SC_CREATED);
            resp.setHeader("content-type", "text/html");
            resp.getWriter().print(item.getName());
            log("Copied file " + item + " for user '" + getUserName(req) + "' to inventory");
        }

        private void copy(InputStream in, OutputStream out) throws IOException {
            // How often have I been writing this method? (nf)
            while (true) {
                int b = in.read();
                if (b == -1) {
                    break;
                }
                out.write(b);
            }
        }

    }

}
