package com.bc.calvalus.code.de.reader;

import lombok.Value;

/**
 * @author muhammad.bc.
 */
@Value
public class JobDetail {
    private String user;
    private String queue;
    private String state;
    private String jobId;
    private String jobName;
    private String startTime;
    private String inputPath;
    private String finishTime;
    private String mapsCompleted;
    private String reducesCompleted;
    private String fileBytesRead;
    private String fileBytesWritten;
    private String hdfsBytesRead;
    private String hdfsBytesWritten;
    private String vCoresMillisTotal;
    private String mbMillisMapTotal;
    private String mbMillisReduceTotal;
    private String cpuMilliseconds;
    private String totalMaps;
    private String remoteUser;
    private String remoteRef;
    private String processType;
    private String wpsJobId;
    private String workflowType;
    private String inProductType;
    private String dataProcessorUsed;

}
