package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Creates a hadoop production service.
 */
public class HadoopProductionServiceFactory implements ProductionServiceFactory {

    @Override
    public ProductionService create(Map<String, String> serviceConfiguration, Logger logger,
                                    String relStagingUrl, File localStagingDir) throws ProductionException {
        return new HadoopProductionService(createJobConf(serviceConfiguration), logger, relStagingUrl, localStagingDir);
    }

    private static JobConf createJobConf(Map<String, String> hadoopProp) {
        JobConf jobConf = new JobConf();
        for (Map.Entry<String, String> entry : hadoopProp.entrySet()) {
            String name = entry.getKey();
            if (name.startsWith("calvalus.hadoop.")) {
                String hadoopName = name.substring("calvalus.hadoop.".length());
                jobConf.set(hadoopName, entry.getValue());
                // System.out.println("Using Hadoop configuration: " + hadoopName + " = " + hadoopValue);
            }
        }
        return jobConf;
    }

}
