package com.bc.calvalus.production.local;

import com.bc.calvalus.inventory.AbstractInventoryService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Local inventory service - for testing only.
 *
 * @author Norman
 */
public class LocalInventoryService extends AbstractInventoryService {

    public static final File CONTEXT_DIR = new File(System.getProperty("user.home"), ".calvalus");
    public static final File EODATA_DIR = new File(CONTEXT_DIR, "eodata");

    public LocalInventoryService() throws IOException {
        super(LocalFileSystem.getLocal(new Configuration()));
        CONTEXT_DIR.mkdirs();
        EODATA_DIR.mkdir();
    }

    @Override
    protected InputStream openProductSetCsv() throws IOException {
        return new FileInputStream(new File(EODATA_DIR, "product-sets.csv"));
    }

    @Override
    protected String getContextPath() {
        return CONTEXT_DIR.getPath();
    }

}
