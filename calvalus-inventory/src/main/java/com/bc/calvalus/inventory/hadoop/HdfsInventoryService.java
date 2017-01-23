package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.inventory.AbstractInventoryService;
import org.apache.hadoop.conf.Configuration;

/**
 * An inventory service that uses Hadoop's HDFS.
 *
 * @author MarcoZ
 * @author Norman
 */
public class HdfsInventoryService extends AbstractInventoryService {

    public static final String CONTEXT_PATH = "/calvalus";

    public HdfsInventoryService(Configuration conf, String archiveRootDir) {
        super(conf, archiveRootDir);
    }

    @Override
    protected String getContextPath() {
        return CONTEXT_PATH;
    }

}
