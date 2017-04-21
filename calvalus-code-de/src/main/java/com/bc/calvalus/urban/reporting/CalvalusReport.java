package com.bc.calvalus.urban.reporting;

import lombok.Value;

/**
 * @author muhammad.bc.
 */
@Value
public class CalvalusReport {


    private String jobId;
    private String user;
    private String queue;
    private long startTime;
    private long finishTime;
    private String state;

    private String outputDir;
    private String remoteUser;
    private String jobName;
    private String remoteRef;
    private String processType;
    private String inputPath;

    private int mapsCompleted;
    private int totalMaps;
    private int reducesCompleted;
    private long fileBytesRead;
    private long fileBytesWritten;
    private long hdfsBytesRead;
    private long hdfsBytesWritten;
    private long mbMillisMapTotal;
    private long mbMillisReduceTotal;
    private long vCoresMillisTotal;
    private long cpuMilliseconds;
}
