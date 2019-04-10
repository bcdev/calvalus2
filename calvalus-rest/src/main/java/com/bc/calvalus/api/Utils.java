package com.bc.calvalus.api;

import com.bc.calvalus.api.model.BackendConfig;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.inventory.FileSystemService;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Utils {

    public static Logger LOG = CalvalusLogger.getLogger();

    public static String getUserName(HttpServletRequest request, ServletContext context) throws ServletException {
        String userName = request.getUserPrincipal().getName();
        String remoteUser = request.getHeader(BackendConfig.getConfig(context).getConfigMap().get("calvalus.wps.deputy.attribute"));
        if (remoteUser != null) {
            if (userName.equals(BackendConfig.getConfig(context).getConfigMap().get("calvalus.wps.deputy.user"))) {
                return BackendConfig.getConfig(context).getConfigMap().get("calvalus.wps.deputy.prefix") + remoteUser;
            }
        }
        return userName;
    }

    /** Returns first non-form-field file item */
    public static FileItem getFileItem(HttpServletRequest request) throws FileUploadException {
        ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
        List<FileItem> items = upload.parseRequest(request);
        for (FileItem item : items) {
            if (!item.isFormField()) {
                return item;
            }
        }
        return null;
    }

    public static void unzipFromStream(String userName, String targetPath, InputStream stream, FileSystemService fileSystemService) throws IOException {
        try (ZipInputStream in = new ZipInputStream(new BufferedInputStream(stream, 8192))) {
            byte[] buffer = new byte[8192];
            while (true) {
                ZipEntry entry = in.getNextEntry();
                if (entry == null) {
                    break;
                }
                String fileName = entry.getName();
                if (fileName.endsWith("/")) {
                    continue;
                }
                LOG.info(String.format("extracting '%s from zip to '%s'", fileName, targetPath));
                String filePath = targetPath + "/" + fileName;
                try (OutputStream out = new BufferedOutputStream(fileSystemService.addFile(userName, filePath), 8192)) {
                    int len;
                    while ((len = in.read(buffer)) > 0) {
                        out.write(buffer, 0, len);
                    }
                }
            }
        }
    }
}
