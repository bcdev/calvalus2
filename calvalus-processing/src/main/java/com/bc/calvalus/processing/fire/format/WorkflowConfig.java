package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import org.apache.hadoop.conf.Configuration;

public class WorkflowConfig {

    public String productionName;
    public HadoopProcessingService processingService;
    public String area;
    public Configuration jobConfig;
    public String year;
    public String month;
    public String userName;
    public String outputDir;
}
