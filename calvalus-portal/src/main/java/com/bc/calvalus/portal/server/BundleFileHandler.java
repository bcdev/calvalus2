package com.bc.calvalus.portal.server;

import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.ceres.binding.BindingException;
import org.apache.commons.fileupload.FileItem;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


/**
 * @author Marco Peters
 */
class BundleFileHandler implements FileUploadServlet.FileHandler {

    private final ServletContext context;

    BundleFileHandler(ServletContext context) {
        this.context = context;
    }

    @Override
    public void handleFileItem(HttpServletRequest req, HttpServletResponse resp, FileItem item) throws Exception {

        final File bundleTempFile = File.createTempFile("bundle_", ".tmp");
        try {
            writeFile(item, new FileOutputStream(bundleTempFile));

            final ZipFile zipFile = new ZipFile(bundleTempFile);
            try {
                BundleDescriptor bd = readBundleDescriptor(zipFile);
                final String bundleName = bd.getBundleName();
                final String bundleVersion = bd.getBundleVersion();
                if (bundleName == null || bundleName.isEmpty() || bundleVersion == null || bundleVersion.isEmpty()) {
                    String msg = String.format("Bundle name and version must be given in the %s file.",
                                               HadoopProcessingService.BUNDLE_DESCRIPTOR_XML_FILENAME);
                    throw new Exception(msg);
                }

                String bundleDirPath = getSpecifiedDirectory(req) + String.format("%s-%s", bundleName, bundleVersion) + "/";
                final String userName = BackendServiceImpl.getUserName(req);
                String userBundlePath = AbstractFileSystemService.getUserPath(userName, bundleDirPath);

                ServiceContainer serviceContainer = (ServiceContainer) context.getAttribute("serviceContainer");
                FileSystemService fileSystemService = serviceContainer.getFileSystemService();
                fileSystemService.removeDirectory(userName, userBundlePath);

                final Enumeration<? extends ZipEntry> zipEntries = zipFile.entries();
                while (zipEntries.hasMoreElements()) {
                    ZipEntry zipEntry = zipEntries.nextElement();
                    String filePath = bundleDirPath + zipEntry.getName();
                    userBundlePath = AbstractFileSystemService.getUserPath(userName, filePath);
                    OutputStream out = new BufferedOutputStream(fileSystemService.addFile(userName, userBundlePath), 64 * 1024);
                    copy(zipFile.getInputStream(zipEntry), out);
                }

            } finally {
                zipFile.close();
            }

            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            resp.setHeader("content-type", "text/html");
            resp.getWriter().print(item.getName());
            context.log("Copied file " + item + " for user '" + BackendServiceImpl.getUserName(req) + "' to inventory");
        } finally {
            bundleTempFile.delete();
        }
    }

    private String getSpecifiedDirectory(HttpServletRequest req) {
        String relPath = req.getParameter("dir");
        String bundleDirPath = "";
        if (relPath != null) {
            bundleDirPath = relPath + "/";
        }
        return bundleDirPath;
    }

    private static BundleDescriptor readBundleDescriptor(ZipFile zipFile) throws IOException, BindingException {
        final BundleDescriptor bd = new BundleDescriptor();
        final ZipEntry entry = zipFile.getEntry(HadoopProcessingService.BUNDLE_DESCRIPTOR_XML_FILENAME);
        if (entry == null) {
            throw new IOException("Zip file does not contain a " + HadoopProcessingService.BUNDLE_DESCRIPTOR_XML_FILENAME + " file.");
        }
        final String bundleXml = readBundleDescriptorXML(zipFile.getInputStream(entry));
        final ParameterBlockConverter parameterBlockConverter = new ParameterBlockConverter();
        parameterBlockConverter.convertXmlToObject(bundleXml, bd);
        return bd;
    }

    private static String readBundleDescriptorXML(InputStream stream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        StringBuilder stringBuilder = new StringBuilder();

        String line;
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
        }

        return stringBuilder.toString();
    }

    private void writeFile(FileItem item, OutputStream outputStream) throws IOException, ProductionException {
        BufferedInputStream in = new BufferedInputStream(item.getInputStream(), 64 * 1024);
        BufferedOutputStream out = new BufferedOutputStream(outputStream, 64 * 1024);
        copy(in, out);
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
