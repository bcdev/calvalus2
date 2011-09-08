package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.inventory.AbstractInventoryService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

public class HdfsInventoryService extends AbstractInventoryService {

    public static final String CONTEXT_PATH = "/calvalus";

    public HdfsInventoryService(FileSystem fileSystem) {
        super(fileSystem);
    }

    @Override
    protected InputStream openProductSetCsv() throws IOException {
        Path databasePath = new Path(getPath("eodata/product-sets.csv"));
        return getFileSystem().open(databasePath);
    }

    @Override
    protected String getContextPath() {
        return CONTEXT_PATH;
    }

}
