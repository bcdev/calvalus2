package com.bc.calvalus.production.local;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.inventory.AbstractInventoryService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;

/**
 * Local inventory service - for testing only.
 *
 * @author Norman
 */
public class LocalInventoryService extends AbstractInventoryService {

    public static final File CONTEXT_DIR = new File(System.getProperty("user.home"), ".calvalus");
    public static final String EODATA = "eodata";
    public static final File EODATA_DIR = new File(CONTEXT_DIR, EODATA);

    public LocalInventoryService() throws IOException {
        super(new JobClientsMap(new JobConf()), EODATA);
        CONTEXT_DIR.mkdirs();
        EODATA_DIR.mkdir();
    }

    @Override
    protected String getContextPath() {
        return CONTEXT_DIR.getPath();
    }

}
