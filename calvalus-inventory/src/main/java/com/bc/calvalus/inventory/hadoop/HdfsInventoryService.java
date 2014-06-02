package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.inventory.AbstractInventoryService;

/**
 * An inventory service that uses Hadoop's HDFS.
 *
 * @author MarcoZ
 * @author Norman
 */
public class HdfsInventoryService extends AbstractInventoryService {

    public static final String CONTEXT_PATH = "/calvalus";

    public HdfsInventoryService(JobClientsMap jobClientsMap, String archiveRootDir) {
        super(jobClientsMap, archiveRootDir);
    }

    @Override
    protected String getContextPath() {
        return CONTEXT_PATH;
    }

}
