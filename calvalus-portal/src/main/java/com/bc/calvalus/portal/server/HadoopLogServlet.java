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
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.store.ProxyWorkflow;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.JobID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;

import static com.bc.calvalus.portal.server.BackendServiceImpl.getUserName;

/**
 * Servlet to handle log file viewing for productions
 *
 * @author MarcoZ
 */
public class HadoopLogServlet extends HttpServlet {

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
        ProductionService productionService = (ProductionService) getServletContext().getAttribute("productionService");
        try {
            Production production = productionService.getProduction(productionId);
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
            showLogFor(resp, workflowItem, jobClient, status);
        } else if (workflowItem instanceof ProxyWorkflow) {
            ProxyWorkflow proxyWorkflow = (ProxyWorkflow) workflowItem;

            HadoopProcessingService processingService = (HadoopProcessingService) proxyWorkflow.getProcessingService();
            JobClient jobClient = processingService.getJobClient(userName);
            showLogFor(resp, workflowItem, jobClient, TaskCompletionEvent.Status.FAILED);
        } else {
            showErrorPage("Not able to provide logs.", resp);
        }
    }

    private void showLogFor(HttpServletResponse resp, WorkflowItem hadoopWorkflowItem, JobClient jobClient, TaskCompletionEvent.Status status) throws IOException {
        Object[] jobIds = hadoopWorkflowItem.getJobIds();
        if(jobIds.length != 1) {
            showErrorPage("found not one jobId, but:" + jobIds.length, resp);
            return;
        }
        JobID jobId = (JobID) jobIds[0];
        org.apache.hadoop.mapred.JobID downgradeJobId = org.apache.hadoop.mapred.JobID.downgrade(jobId);
        try {
            RunningJob runningJob = jobClient.getJob(downgradeJobId);
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
                        displayTaskLogs(event.getTaskAttemptId(), event.getTaskTrackerHttp(), resp);
                        return;
                    }
                }
            }
        } catch (IOException e) {
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
}
