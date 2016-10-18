package com.bc.calvalus.production.local;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.inventory.DefaultInventoryService;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;

/**
 * Local filesystem service - for testing only.
 *
 * @author Norman
 */
public class LocalFileSystemService extends AbstractFileSystemService {

    public static final File CONTEXT_DIR = new File(System.getProperty("user.home"), ".calvalus");
    public static final String EODATA = "eodata";
    public static final File EODATA_DIR = new File(CONTEXT_DIR, EODATA);

    public LocalFileSystemService() throws IOException {
        super(new JobClientsMap(new JobConf()));
        CONTEXT_DIR.mkdirs();
        EODATA_DIR.mkdir();
    }

    @Override
    protected String getContextPath() {
        return CONTEXT_DIR.getPath();
    }

}
