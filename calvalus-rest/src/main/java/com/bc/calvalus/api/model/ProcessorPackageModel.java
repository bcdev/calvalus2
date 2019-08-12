package com.bc.calvalus.api.model;

import com.bc.calvalus.api.Utils;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.hadoop.HadoopServiceContainerFactory;
import org.apache.hadoop.fs.FileStatus;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class ProcessorPackageModel extends FileBasedModel {

    private static ProcessorPackageModel instance;
    public static ProcessorPackageModel getInstance(ServletContext context) throws ServletException, ProductionException {
        if (instance == null) {
            instance = new ProcessorPackageModel(context);
        }
        return instance;
    }

    private ProcessorPackageModel(ServletContext context) throws ServletException, ProductionException {
        super(context);
    }

    public BundleDescriptor[] getBundleDescriptors(String userName) throws ServletException, ProductionException {
        BundleFilter filter = new BundleFilter().withProvider(BundleFilter.PROVIDER_USER).withTheUser(userName);
        return serviceContainer.getProductionService().getBundles(userName, filter);
    }

    public BundleDescriptor getBundleDescriptor(String userName, String name) throws ServletException, ProductionException {
        for (BundleDescriptor descriptor : getBundleDescriptors(userName)) {
            if ((descriptor.getBundleName() + "-" + descriptor.getBundleVersion()).equals(name)) {
                return descriptor;
            }
        }
        return null;
    }

    public FileStatus[] getFiles(String userName, String bundleName) throws IOException {
        List<String> pathPatterns = Collections.singletonList(AbstractFileSystemService.getUserGlob(userName, "software/" + bundleName));
        return serviceContainer.getFileSystemService().globFiles(userName, pathPatterns);
    }

    public FileStatus getFile(String userName, String userPath) throws IOException {
        List<String> pathPatterns = Collections.singletonList(AbstractFileSystemService.getUserPath(userName, userPath));
        FileStatus[] fileStatuses = serviceContainer.getFileSystemService().globFiles(userName, pathPatterns);
        return fileStatuses.length > 0 ? fileStatuses[0] : null;
    }

    public void invalidateBundleCache() {
        serviceContainer.getProductionService().invalidateBundleCache();
    }

}
