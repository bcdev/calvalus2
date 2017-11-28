/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.portal.server;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.store.ProxyWorkflow;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.owasp.esapi.User;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

import static com.bc.calvalus.portal.server.BackendServiceImpl.getUserName;

/**
 * Servlet to handle log file viewing for productions
 *
 * @author MarcoZ
 */
public class HadoopLogServlet extends HttpServlet {

    private boolean withExternalAccessControl;

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String productionId = req.getParameter("productionId");
        if (productionId == null) {
            showErrorPage("Missing query parameter 'productionId'", resp);
            return;
        }
        ServiceContainer serviceContainer = (ServiceContainer) getServletContext().getAttribute("serviceContainer");
        withExternalAccessControl = serviceContainer.getHadoopConfiguration().getBoolean("calvalus.accesscontrol.external", false);
        try {
            Production production = serviceContainer.getProductionService().getProduction(productionId);
            final String userName = getUserName(req).toLowerCase();
            ProcessState processState = production.getProcessingStatus().getState();
            WorkflowItem workflow = production.getWorkflow();
            if (processState == ProcessState.ERROR) {
                handleWorkFlow(workflow, ProcessState.ERROR, resp, TaskCompletionEvent.Status.FAILED, userName);
            } else if (processState == ProcessState.COMPLETED) {
                handleWorkFlow(workflow, ProcessState.COMPLETED, resp, TaskCompletionEvent.Status.SUCCEEDED, userName);
            } else {
                showErrorPage("Logfile are only supported for ERROR or COMPLETED states", resp);
            }
        } catch (ProductionException e) {
            showErrorPage("Failed to get production for id: " + productionId, resp);
        }
    }

    private void handleWorkFlow(WorkflowItem workflow, ProcessState processState, HttpServletResponse resp, TaskCompletionEvent.Status status, String userName) throws IOException {
        WorkflowItem[] items = workflow.getItems();
        if (items.length == 0) {
            // the one and only item must be the failed one
            showLogFor(workflow, resp, status, userName);
        } else {
            for (WorkflowItem workflowItem : items) {
                if (workflowItem.getStatus().getState() == processState) {
                    handleWorkFlow(workflowItem, processState, resp, status, userName);
                    return;
                }
            }
        }
    }

    private void showLogFor(WorkflowItem workflowItem, HttpServletResponse resp, TaskCompletionEvent.Status status, String userName) throws IOException {
        if (workflowItem instanceof HadoopWorkflowItem) {
            HadoopWorkflowItem hadoopWorkflowItem = (HadoopWorkflowItem) workflowItem;

            HadoopProcessingService processingService = hadoopWorkflowItem.getProcessingService();
            JobClient jobClient = processingService.getJobClient(userName);
            showLogFor(resp, workflowItem, jobClient, status, userName);
        } else if (workflowItem instanceof ProxyWorkflow) {
            ProxyWorkflow proxyWorkflow = (ProxyWorkflow) workflowItem;

            HadoopProcessingService processingService = (HadoopProcessingService) proxyWorkflow.getProcessingService();
            JobClient jobClient = processingService.getJobClient(userName);
            showLogFor(resp, workflowItem, jobClient, status, userName);
        } else {
            showErrorPage("Not able to provide logs.", resp);
        }
    }

    private void showLogFor(HttpServletResponse resp, WorkflowItem hadoopWorkflowItem, JobClient jobClient, TaskCompletionEvent.Status status, String userName) throws IOException {
        Object[] jobIds = hadoopWorkflowItem.getJobIds();
        if (jobIds.length != 1) {
            showErrorPage("found not one jobId, but:" + jobIds.length, resp);
            return;
        }
        JobID jobId = (JobID) jobIds[0];
        org.apache.hadoop.mapred.JobID downgradeJobId = org.apache.hadoop.mapred.JobID.downgrade(jobId);
        try {
            RunningJob runningJob = jobClient.getJob(downgradeJobId);
            Configuration conf = jobClient.getConf();
            if (runningJob == null) {
                showErrorPage("No 'RunningJob' found for jobId: " + jobId, resp);
                return;
            }
            int eventCounter = 0;
            while (true) {
                TaskCompletionEvent[] taskCompletionEvents = runningJob.getTaskCompletionEvents(eventCounter);
                if (taskCompletionEvents.length == 0) {
                    break;
                }
                eventCounter += taskCompletionEvents.length;
                for (TaskCompletionEvent event : taskCompletionEvents) {
                    if (event.getTaskStatus().equals(status)) {
                        // TaskID taskID = event.getTaskAttemptId().getTaskID();
                        // TODO in case of status == failed --> check if task has really failed.
//                        displayTaskLogs(event.getTaskAttemptId(), event.getTaskTrackerHttp(), resp);

                        UserGroupInformation remoteUser;
                        if (withExternalAccessControl) {
                            remoteUser = UserGroupInformation.createRemoteUser("yarn");
                        } else {
                            remoteUser = UserGroupInformation.createRemoteUser(userName);
                        }
                        try {
                            remoteUser.doAs(new PrivilegedExceptionAction<Integer>() {
                                @Override
                                public Integer run() throws Exception {
                                    String appOwner = userName;
                                    StringBuilder appSB = new StringBuilder("application");
                                    String appIdStr = jobId.appendTo(appSB).toString();

                                    ApplicationId appId = null;
                                    try {
                                        appId = ConverterUtils.toApplicationId(appIdStr);
                                    } catch (Exception e) {
                                        System.err.println("Invalid ApplicationId specified: " + appIdStr);
                                        return -1;
                                    }
                                    int resultCode = dumpAllContainersLogs(appId, appOwner, resp.getOutputStream(), conf);
                                    if (resultCode != 0) {
                                        showErrorPage("Failed to open Logfile.", resp);
                                        return -1;
                                    }
                                    return 0;
                                }
                            });
                        } catch (InterruptedException e) {
                            throw new IOException(e);
                        }
                        return;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            showErrorPage("I/O error", resp);
        }
    }

    private String getTaskLogURL(TaskAttemptID taskId, String baseUrl) {
        return (baseUrl + "/tasklog?attemptid=" + taskId);
    }

    private void displayTaskLogs(TaskAttemptID taskId, String baseUrl, HttpServletResponse resp) throws IOException {
        // The tasktracker for a 'failed/killed' job might not be around...
        if (baseUrl != null) {
            // Construct the url for the tasklogs
            String taskLogUrl = getTaskLogURL(taskId, baseUrl);

            URL url = new URL(taskLogUrl);
            InputStream inputStream = null;
            try {
                inputStream = url.openStream();
                copyIntoOutput(inputStream, resp.getOutputStream());
            } catch (IOException e) {
                showErrorPage("Failed to open Logfile." + e.getMessage(), resp);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        } else {
            showErrorPage("No 'baseUrl' given.", resp);
        }
    }

    private void copyIntoOutput(InputStream input, OutputStream outputStream) throws IOException {
        BufferedOutputStream output = new BufferedOutputStream(outputStream, 4 * 1024 * 1024);
        try {
            byte[] buffer = new byte[1024 * 1024];
            int bytesRead, bytesWritten = 0;
            while ((bytesRead = input.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
                bytesWritten += bytesRead;
            }
            output.flush();
            log(bytesWritten + " bytes sent");
        } finally {
            output.close();
        }
    }

    private void showErrorPage(String message, HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        PrintWriter writer = new PrintWriter(response.getOutputStream());
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
    }


    public int dumpAllContainersLogs(ApplicationId appId, String appOwner,
                                     OutputStream outputStream, Configuration conf) throws IOException {
        Path remoteRootLogDir = new Path(conf.get(
                YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
                YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
        String user = appOwner;
        String logDirSuffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);
        // TODO Change this to get a list of files from the LAS.
        Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(
                remoteRootLogDir, appId, user, logDirSuffix);
        RemoteIterator<FileStatus> nodeFiles;
        try {
            nodeFiles = FileContext.getFileContext(conf).listStatus(remoteAppLogDir);
        } catch (FileNotFoundException fnf) {
            System.out.println("Logs not available at " + remoteAppLogDir.toString());
            System.out
                    .println("Log aggregation has not completed or is not enabled.");
            return -1;
        }
        while (nodeFiles.hasNext()) {
            FileStatus thisNodeFile = nodeFiles.next();
            AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(
                    conf, new Path(remoteAppLogDir, thisNodeFile.getPath().getName()));
            try {

                DataInputStream valueStream;
                AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
                valueStream = reader.next(key);

                PrintStream out = new PrintStream(outputStream);
                while (valueStream != null) {
                    String containerString = "\n\nContainer: " + key + " on "
                                             + thisNodeFile.getPath().getName();
                    out.println(containerString);
                    out.println(StringUtils.repeat("=", containerString.length()));
                    while (true) {
                        try {
                            AggregatedLogFormat.LogReader.readAContainerLogsForALogType(valueStream, out);
                        } catch (EOFException eof) {
                            break;
                        }
                    }

                    // Next container
                    key = new AggregatedLogFormat.LogKey();
                    valueStream = reader.next(key);
                }
            } finally {
                reader.close();
            }
        }
        return 0;
    }
}
