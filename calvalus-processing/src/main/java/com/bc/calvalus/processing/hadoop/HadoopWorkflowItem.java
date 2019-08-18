/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.hadoop;

import static com.bc.calvalus.processing.hadoop.HadoopProcessingService.CALVALUS_SOFTWARE_PATH;
import static com.bc.calvalus.processing.hadoop.HadoopProcessingService.DEFAULT_CALVALUS_BUNDLE;
import static com.bc.calvalus.processing.hadoop.HadoopProcessingService.DEFAULT_SNAP_BUNDLE;

import com.bc.calvalus.commons.AbstractWorkflowItem;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * A workflow item that corresponds to a single Hadoop job.
 */
public abstract class HadoopWorkflowItem extends AbstractWorkflowItem {

    protected static final String NO_DEFAULT = "[[NO_DEFAULT]]";
    private final HadoopProcessingService processingService;
    private final String jobName;
    private final String userName;
    private final Configuration jobConfig;
    private JobID jobId;
    private HadoopJobHook jobHook;

    public HadoopWorkflowItem(HadoopProcessingService processingService,
                              String userName,
                              String jobName,
                              Configuration jobConfig) {
        this.processingService = processingService;
        this.userName = userName;
        this.jobName = jobName;
        this.jobConfig = jobConfig;
    }

    public final HadoopProcessingService getProcessingService() {
        return processingService;
    }

    public abstract String getOutputDir();

    public String getUserName() {
        return userName;
    }

    public final String getJobName() {
        return jobName;
    }

    public final Configuration getJobConfig() {
        return jobConfig;
    }

    public final JobID getJobId() {
        return jobId;
    }

    protected final void setJobId(JobID jobId) {
        this.jobId = jobId;
    }

    @Override
    public final Object[] getJobIds() {
        return jobId != null ? new Object[]{getJobId()} : new Object[0];
    }

    @Override
    public void kill() throws WorkflowException {
        try {
            if (jobId != null) {
                CalvalusLogger.getLogger().fine("Killing Job: " + getJobName() + " - " + jobId.getJtIdentifier());
                processingService.killJob(userName, jobId);
            }
        } catch (IOException e) {
            throw new WorkflowException("Failed to kill Hadoop job: " + e.getMessage(), e);
        }
    }

    @Override
    public void updateStatus() {
        if (jobId != null) {
            if (!getStatus().isDone()) {
                ProcessStatus newJobStatus = processingService.getJobStatus(jobId);
                if (newJobStatus.getState().equals(ProcessState.ERROR)) {
                    String failedTaskMessage = getDiagnosticFromFirstFailedTask();
                    if (failedTaskMessage != null) {
                        newJobStatus = new ProcessStatus(ProcessState.ERROR, newJobStatus.getProgress(), failedTaskMessage);
                    }
                }
                setStatus(newJobStatus);
            }
        }
    }

    public void setJobHook(HadoopJobHook jobHook) {
        this.jobHook = jobHook;
    }

    private String getDiagnosticFromFirstFailedTask() {
        org.apache.hadoop.mapred.JobID downgradeJobId = org.apache.hadoop.mapred.JobID.downgrade(jobId);
        try {
            JobClient jobClient = processingService.getJobClient(userName);
            RunningJob runningJob = jobClient.getJob(downgradeJobId);
            if (runningJob == null) {
                return null;
            }
//            for (TaskReport mapTaskReport : jobClient.getMapTaskReports(downgradeJobId)) {
//                System.out.println("mapTaskReport = " + mapTaskReport.getCurrentStatus() + " "+ mapTaskReport.getState());
//                if (mapTaskReport.getCurrentStatus().equals(TIPStatus.FAILED)) {
//                    String[] taskDiagnostics = mapTaskReport.getDiagnostics();
//                    System.out.println("taskDiagnostics = " + Arrays.toString(taskDiagnostics));
//                    if (taskDiagnostics.length > 0) {
//                        return getErrorMessageFromDiagnostics(taskDiagnostics);
//                    }
//                }
//            }
//            for (TaskReport reduceTaskReport : jobClient.getReduceTaskReports(downgradeJobId)) {
//                System.out.println("reduceTaskReport = " + reduceTaskReport.getCurrentStatus() + " "+ reduceTaskReport.getState());
//                if (reduceTaskReport.getCurrentStatus().equals(TIPStatus.FAILED)) {
//                    String[] taskDiagnostics = reduceTaskReport.getDiagnostics();
//                    System.out.println("taskDiagnostics = " + Arrays.toString(taskDiagnostics));
//                    if (taskDiagnostics.length > 0) {
//                        return getErrorMessageFromDiagnostics(taskDiagnostics);
//                    }
//                }
//            }

            int eventCounter = 0;
            while (true) {
                TaskCompletionEvent[] taskCompletionEvents = runningJob.getTaskCompletionEvents(eventCounter);
                if (taskCompletionEvents.length == 0) {
                    break;
                }
                eventCounter += taskCompletionEvents.length;
                for (TaskCompletionEvent taskCompletionEvent : taskCompletionEvents) {
                    if (taskCompletionEvent.getTaskStatus().equals(TaskCompletionEvent.Status.FAILED)) {
                        String[] taskDiagnostics = runningJob.getTaskDiagnostics(taskCompletionEvent.getTaskAttemptId());
                        if (taskDiagnostics.length > 0) {
                            return getErrorMessageFromDiagnostics(taskDiagnostics);
                        }
                    }
                }
            }
        } catch (IOException e) {
            return null;
        }
        return null;
    }

    static String getErrorMessageFromDiagnostics(String[] taskDiagnostics) {
        // this is a stack trace and we only want the case
        // so take the first line
        // remove the "*Exception: " prefixes (can be multiple)
        String firstMessage = taskDiagnostics[0];
        String firstLine = firstMessage.split("\\n")[0];
        String[] firstLineSplit = firstLine.split("Exception: ");
        return firstLineSplit[firstLineSplit.length - 1];
    }

    @Override
    public void submit() throws WorkflowException {
        if (processingService.getHadoopLaunchHandler() == null) {
            submitInternal();
        } else {
            processingService.getHadoopLaunchHandler().queueWorkflowItem(this);
        }
    }

    public void submitInternal() throws WorkflowException {
        try {
            UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(userName);
            remoteUser.doAs((PrivilegedExceptionAction<Job>) () -> {
                CalvalusLogger.getLogger().info("Submitting Job: " + getJobName());
                Job job = getProcessingService().createJob(getJobName(), jobConfig);
                configureJob(job);
                FileSystem fileSystem = processingService.getFileSystem(userName);
                ProcessorFactory.installProcessorBundles(userName, job.getConfiguration(), fileSystem);
                validateJob(job);
                // maybe add calvalus.token parameter
                if (jobHook != null) {
                    jobHook.beforeSubmit(job.getConfiguration());
                }
                JobID jobId = submitJob(job);  // the impl of submitJob does a doAs as well, but maybe with the wrong user

                CalvalusLogger.getLogger().info("Submitted Job with Id: " + jobId);
                CalvalusLogger.getLogger().info("-------------------------------");
                CalvalusLogger.getLogger().info("remoteUser=" + remoteUser.getShortUserName()
                                                + " mapreduce.job.user.name=" + job.getConfiguration().get("mapreduce.job.user.name"));
                HashMap<String, String> calvalusConfMap = new HashMap<>();
                for (Map.Entry<String, String> keyValue : job.getConfiguration()) {
                    if (keyValue.getKey().startsWith("calvalus")) {
                        calvalusConfMap.put(keyValue.getKey(), keyValue.getValue());
                    }
                }
                calvalusConfMap.entrySet().
                        stream().
                        sorted(Map.Entry.<String, String>comparingByKey()).
                        forEach(new Consumer<Map.Entry<String, String>>() {
                            @Override
                            public void accept(Map.Entry<String, String> keyValue) {
                                CalvalusLogger.getLogger().info(keyValue.getKey() + " = " + keyValue.getValue());
                            }
                        });
                CalvalusLogger.getLogger().info("-------------------------------");
                
                setJobId(jobId);
                return job;
            });
        } catch (Throwable e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage(), e);
            throw new WorkflowException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }

    protected void validateJob(Job job) throws WorkflowException {
        Configuration jobConfig = job.getConfiguration();
        String[][] configDefaults = getJobConfigDefaults();
        for (String[] configDefault : configDefaults) {
            String propertyName = configDefault[0];
            String propertyDefault = configDefault[1];
            String propertyValue = jobConfig.get(propertyName);
            if (propertyValue == null) {
                if (NO_DEFAULT.equals(propertyDefault)) {
                    throw new WorkflowException("Missing value for job configuration property '" + propertyName + "'");
                } else if (propertyDefault != null) {
                    jobConfig.set(propertyName, propertyDefault);
                }
            }
        }
    }

    protected abstract void configureJob(Job job) throws IOException;

    protected abstract String[][] getJobConfigDefaults();

    protected JobID submitJob(Job job) throws IOException {
        Configuration configuration = job.getConfiguration();
        // Add Calvalus modules to classpath of Hadoop jobs
        final String calvalusBundle = configuration.get(JobConfigNames.CALVALUS_CALVALUS_BUNDLE, DEFAULT_CALVALUS_BUNDLE);
        processingService.addBundleToDistributedCache(new Path(CALVALUS_SOFTWARE_PATH, calvalusBundle), userName, configuration);
        // Add SNAP modules to classpath of Hadoop jobs
        final String snapBundle = configuration.get(JobConfigNames.CALVALUS_SNAP_BUNDLE, DEFAULT_SNAP_BUNDLE);
        processingService.addBundleToDistributedCache(new Path(CALVALUS_SOFTWARE_PATH, snapBundle), userName, configuration);
        // System.out.println("trace mapreduce.job.cache.files=" + configuration.get("mapreduce.job.cache.files"));
        JobConf jobConf;
        if (configuration instanceof JobConf) {
            jobConf = (JobConf) configuration;
        } else {
            jobConf = new JobConf(configuration);
        }
        jobConf.setUseNewMapper(true);
        jobConf.setUseNewReducer(true);

        JobClient jobClient = processingService.getJobClient(userName);
        RunningJob runningJob = jobClient.submitJob(jobConf);
        return runningJob.getID();
    }

    protected Class<? extends InputFormat> getInputFormatClass(Configuration conf) throws IOException {
        if (conf.get(JobConfigNames.CALVALUS_INPUT_TABLE) != null) {
            return TableInputFormat.class;
        } else if (conf.get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY) != null || conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS) != null) {
            return PatternBasedInputFormat.class;
        } else {
            throw new IOException(String.format("Missing job parameter for inputFormat. Neither %s nor %s nor %s had been set.",
                                                JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS,
                                                JobConfigNames.CALVALUS_INPUT_TABLE,
                                                JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY));
        }

    }

}
