package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.inventory.AbstractInventoryService;
import org.apache.hadoop.fs.FileSystem;

/**
 * An inventory service that uses Hadoop's HDFS.
 *
 * @author MarcoZ
 * @author Norman
 */
public class HdfsInventoryService extends AbstractInventoryService {

    public static final String CONTEXT_PATH = "/calvalus";

    public HdfsInventoryService(FileSystem fileSystem, String archiveRootDir) {
        super(fileSystem, archiveRootDir);
    }

    @Override
    protected String getContextPath() {
        return CONTEXT_PATH;
    }

}
