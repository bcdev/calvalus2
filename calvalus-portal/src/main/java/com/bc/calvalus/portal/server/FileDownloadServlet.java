package com.bc.calvalus.portal.server;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

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

        String productionId = req.getParameter("productionId");
        if (productionId == null) {
            throw new ServletException("Missing parameter 'productionId'");
        }

        File file = getOutputFile(productionId);
        if (file.length() > Integer.MAX_VALUE) {
            throw new ServletException("File size too big (expected < 2 GB, but got " + file.length() + " bytes).");
        }
        int contentLength = (int) file.length();

        BufferedInputStream input = new BufferedInputStream(new FileInputStream(file), 4 * 1024 * 1024);
        try {
            resp.reset();
            resp.setContentType("application/octet-stream");
            resp.setContentLength(contentLength);
            resp.setHeader("Content-disposition", String.format("attachment; filename=\"%s\"", file.getName()));
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

    private File getOutputFile(String productionId) throws IOException {
        // todo - get real file for productionId
        File file = new File(System.getProperty("java.io.tmpdir", "."), "calvalus-" + productionId + ".dat");
        if (!file.exists()) {
            FileOutputStream stream = new FileOutputStream(file);
            byte[] buffer = new byte[1024 * 1024];
            try {
                for (int i = 0; i < 32; i++) {
                    Arrays.fill(buffer, (byte) i);
                    stream.write(buffer);
                }
            } finally {
                stream.close();
            }
        }
        return file;
    }

}
