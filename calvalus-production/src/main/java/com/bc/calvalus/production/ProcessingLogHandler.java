package com.bc.calvalus.production;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.production.store.ProxyWorkflow;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class ProcessingLogHandler {

    private static final int KILO_BYTES = 1024;
    private static final int DEFAULT_RETRY_PERIOD_MILLIS = 1000;
    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final Logger LOGGER = CalvalusLogger.getLogger();

    public static final int LOG_STREAM_EMPTY_ERROR_CODE = 1;
    private static final int LOG_GENERAL_ERROR_CODE = -1;

    private Map<String, String> configMap;
    private boolean withExternalAccessControl;
    private long retryPeriodMillis;
    private long maxRetries;

    public ProcessingLogHandler(Map<String, String> configMap, boolean withExternalAccessControl) {
        this.configMap = configMap;
        this.withExternalAccessControl = withExternalAccessControl;
        this.retryPeriodMillis = resolveRetryPeriod();
        this.maxRetries = resolveMaxRetries();
    }

    public int handleProduction(Production production, OutputStream out, String userName) throws IOException {
        WorkflowItem workflow = production.getWorkflow();
        ProcessState processState = production.getProcessingStatus().getState();
        return handleWorkFlow(workflow, processState, out, userName);
    }

    public int showErrorPage(String message, OutputStream out) {
        PrintWriter writer = new PrintWriter(out);
        writer.println("<html>");
        writer.println("<title>Error while showing processing log</title>");
        writer.println("<body>");
        writer.println("<h1>Error while showing processing log</h1>");
        writer.println("While retrieving the logfile from the Hadoop Cluster an error occurred.</br>" +
                       "In most cases this is related to the fact, that the logs are automatically removed after roughly 24 hours.</br>" +
                       "Depending on the cluster activity this can happen sooner or later.</br></br></br>");
        writer.println("<hr>");
        writer.println("<h5>Internal error message</h5>");
        writer.println(message);
        writer.println("</body>");
        writer.flush();
        writer.close();

        return LOG_GENERAL_ERROR_CODE;
    }

    private int handleWorkFlow(WorkflowItem workflow, ProcessState processState, OutputStream out,
                               String userName) throws IOException {
        WorkflowItem[] items = workflow.getItems();
        if (items.length == 0) {
            // the one and only item must be the failed one
            return showLogFor(workflow, out, userName);
        } else {
            for (WorkflowItem workflowItem : items) {
                if (workflowItem.getStatus().getState() == processState) {
                    return handleWorkFlow(workflowItem, processState, out, userName);
                }
            }
            return showErrorPage("Cannot find logs with the status '" + processState + "'.", out);
        }
    }

    private int showLogFor(WorkflowItem workflowItem, OutputStream out,
                           String userName) throws IOException {
        if (workflowItem instanceof HadoopWorkflowItem) {
            HadoopWorkflowItem hadoopWorkflowItem = (HadoopWorkflowItem) workflowItem;

            HadoopProcessingService processingService = hadoopWorkflowItem.getProcessingService();
            JobClient jobClient = processingService.getJobClient(userName);
            return showLogFor(out, workflowItem, jobClient, userName);
        } else if (workflowItem instanceof ProxyWorkflow) {
            ProxyWorkflow proxyWorkflow = (ProxyWorkflow) workflowItem;

            HadoopProcessingService processingService = (HadoopProcessingService) proxyWorkflow.getProcessingService();
            JobClient jobClient = processingService.getJobClient(userName);
            return showLogFor(out, workflowItem, jobClient, userName);
        } else {
            return showErrorPage("Not able to provide logs.", out);
        }
    }

    private int showLogFor(OutputStream out, WorkflowItem hadoopWorkflowItem, JobClient jobClient, String userName) {
        Object[] jobIds = hadoopWorkflowItem.getJobIds();
        if (jobIds.length != 1) {
            return showErrorPage("found not one jobId, but:" + jobIds.length, out);
        }
        JobID jobId = (JobID) jobIds[0];
        org.apache.hadoop.mapred.JobID downgradeJobId = org.apache.hadoop.mapred.JobID.downgrade(jobId);
        try {
            int runningJobRetries = 0;
            RunningJob runningJob = null;
            while (runningJobRetries < this.maxRetries) {
                runningJob = jobClient.getJob(downgradeJobId);
                if (runningJob != null) {
                    break;
                }
                runningJobRetries++;
                LOGGER.log(Level.INFO, "No running job for jobId '" + downgradeJobId.toString() +
                                       "'. Already tried " + runningJobRetries + " time(s) (maxRetries=" +
                                       this.maxRetries + ", retryPeriodMillis=" + this.retryPeriodMillis + ")");
                Thread.sleep(this.retryPeriodMillis);
            }
            Configuration conf = jobClient.getConf();
            LOGGER.log(Level.INFO, "runningJob = " + runningJob);
            if (runningJob == null) {
                return showErrorPage("No 'RunningJob' found for jobId: " + jobId, out);
            }

            UserGroupInformation remoteUser;
            if (withExternalAccessControl) {
                remoteUser = UserGroupInformation.createRemoteUser("yarn");
            } else {
                remoteUser = UserGroupInformation.createRemoteUser(userName);
            }
            try {
                return remoteUser.doAs(new PrivilegedExceptionAction<Integer>() {
                    @Override
                    public Integer run() throws Exception {
                        StringBuilder appSB = new StringBuilder("application");
                        String appIdStr = jobId.appendTo(appSB).toString();

                        ApplicationId appId;
                        try {
                            appId = ConverterUtils.toApplicationId(appIdStr);
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, "Invalid ApplicationId specified: " + appIdStr);
                            return LOG_GENERAL_ERROR_CODE;
                        }
                        int resultCode = dumpAllContainersLogs(appId, userName, out, conf);
                        if (resultCode == LOG_GENERAL_ERROR_CODE) {
                            return showErrorPage("Failed to open Logfile.", out);
                        }
                        return resultCode;
                    }
                });
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return showErrorPage("I/O error", out);
        }
    }


    private int dumpAllContainersLogs(ApplicationId appId, String appOwner,
                                      OutputStream outputStream, Configuration conf) throws IOException {
        Path remoteRootLogDir = new Path(conf.get(
                    YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
                    YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
        String logDirSuffix = conf.get("yarn.nodemanager.remote-app-log-dir-suffix", "logs");
        // TODO Change this to get a list of files from the LAS.
        Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(
                    remoteRootLogDir, appId, appOwner, logDirSuffix);
        // give a short period to increase the probability that the logs have been successfully aggregated
        try {
            Thread.sleep(this.retryPeriodMillis);
        } catch (InterruptedException ignored) {
        }
        RemoteIterator<FileStatus> nodeFiles = retrieveFiles(conf, remoteAppLogDir);
        if (nodeFiles == null) {
            LOGGER.log(Level.INFO, "Logs not available at " + remoteAppLogDir.toString());
            LOGGER.log(Level.INFO, "Log aggregation has not completed or is not enabled.");
            return LOG_GENERAL_ERROR_CODE;
        }
        try (CountablePrintStream countablePrintStream = new CountablePrintStream(outputStream)) {
            while (nodeFiles.hasNext()) {
                FileStatus thisNodeFile = nodeFiles.next();
                LOGGER.log(Level.INFO, "Reading log file " + thisNodeFile.getPath().getName() + " with size " +
                                       thisNodeFile.getBlockSize());
                AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(
                            conf, new Path(remoteAppLogDir, thisNodeFile.getPath().getName()));
                try {
                    DataInputStream valueStream;
                    AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
                    valueStream = reader.next(key);

                    while (valueStream != null) {
                        String containerString = "\n\nContainer: " + key + " on " + thisNodeFile.getPath().getName();
                        countablePrintStream.println(containerString);
                        countablePrintStream.println(StringUtils.repeat("=", containerString.length()));
                        long logMaxSizeBytes;
                        try {
                            logMaxSizeBytes = Long.parseLong(configMap.get("calvalus.logs.maxSizeKb")) * KILO_BYTES;
                        } catch (NumberFormatException exception) {
                            logMaxSizeBytes = 0;
                        }
                        if (logMaxSizeBytes == 0 || countablePrintStream.getCount() < logMaxSizeBytes) {
                            while (true) {
                                try {
                                    AggregatedLogFormat.LogReader.readAContainerLogsForALogType(valueStream,
                                                                                                countablePrintStream);
                                } catch (EOFException eof) {
                                    LOGGER.log(Level.INFO, "Finished reading a file. " +
                                                           "Accumulated size: " + countablePrintStream.getCount());
                                    break;
                                }
                            }
                        } else {
                            String message = String.format(
                                        "Log file contents have been omitted because it has already " +
                                        "reached the maximum limit of %d Bytes.", logMaxSizeBytes);
                            countablePrintStream.println(StringUtils.repeat("-", message.length()));
                            countablePrintStream.println(message);
                            countablePrintStream.println(StringUtils.repeat("-", message.length()));
                        }

                        // Next container
                        key = new AggregatedLogFormat.LogKey();
                        valueStream = reader.next(key);
                    }
                } finally {
                    reader.close();
                }
            }
            if (countablePrintStream.getCount() == 0) {
                return LOG_STREAM_EMPTY_ERROR_CODE;
            }
        }
        return 0;
    }

    private RemoteIterator<FileStatus> retrieveFiles(Configuration conf, Path remoteAppLogDir) throws IOException {
        int retriesCount = 0;
        while (true) {
            try {
                RemoteIterator<FileStatus> nodeFiles = FileContext.getFileContext(conf).listStatus(remoteAppLogDir);
                if (nodeFiles != null) {
                    return nodeFiles;
                }
            } catch (FileNotFoundException fnf) {
                retriesCount++;
                if (retriesCount >= this.maxRetries) {
                    fnf.printStackTrace();
                    break;
                }
                LOGGER.log(Level.INFO, "Logs not yet available at " + remoteAppLogDir.toString() + ". " +
                                       "'. Already tried " + retriesCount + " time(s) (maxRetries=" +
                                       this.maxRetries + ", retryPeriodMillis=" + this.retryPeriodMillis + ")");
                try {
                    Thread.sleep(this.retryPeriodMillis);
                } catch (InterruptedException ignored) {
                }
            }
        }
        return null;
    }

    private long resolveRetryPeriod() {
        try {
            return Long.parseLong(configMap.get("calvalus.logs.cpt.retryPeriodMillis"));
        } catch (NumberFormatException exception) {
            return DEFAULT_RETRY_PERIOD_MILLIS;
        }
    }

    private long resolveMaxRetries() {
        try {
            return Long.parseLong(configMap.get("calvalus.logs.cpt.maxRetries"));
        } catch (NumberFormatException exception) {
            return DEFAULT_MAX_RETRIES;
        }
    }
}
