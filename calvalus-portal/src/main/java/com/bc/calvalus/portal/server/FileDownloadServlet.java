package com.bc.calvalus.portal.server;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Servlet to handle file download requests.
 *
 * @author Norman
 */
public class FileDownloadServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String filePath = req.getParameter("file");
        if (filePath == null) {
            throw new ServletException("Missing query parameter 'file'");
        }

        File downloadDir = new BackendConfig(getServletContext()).getLocalStagingDir();
        File file = new File(downloadDir, filePath);
        if (file.length() > Integer.MAX_VALUE) {
            throw new ServletException(String.format("File size too big (expected less than 2 GB, but got %d bytes).", file.length()));
        }
        int contentLength = (int) file.length();

        BufferedInputStream input = new BufferedInputStream(new FileInputStream(file), 4 * 1024 * 1024);
        try {
            resp.reset();
            resp.setContentType("application/octet-stream");
            resp.setContentLength(contentLength);
            resp.setHeader("Content-disposition",
                           String.format("attachment; filename=\"%s\"", file.getName()));
            BufferedOutputStream output = new BufferedOutputStream(resp.getOutputStream(), 4 * 1024 * 1024);
            byte[] buffer = new byte[1024 * 1024];
            int bytesRead, bytesWritten = 0;
            while ((bytesRead = input.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
                bytesWritten += bytesRead;
            }
            output.flush();
            log(bytesWritten + " bytes sent");
        } finally {
            input.close();
        }
    }


}
