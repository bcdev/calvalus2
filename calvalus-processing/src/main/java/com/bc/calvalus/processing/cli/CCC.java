/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.cli;

import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Set;

/**
 * The Calalus Commandline Client.
 *
 * @author MarcoZ
 */
public class CCC {

    private final HadoopProcessingService hps;
    private final WorkflowFactoryRegistry registry;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            showHelp(WorkflowFactoryRegistry.getInstance(), System.out);
            System.exit(1);
        } else {
            CCC ccc = new CCC();
            ccc.execute(args);
        }
    }

    public CCC() throws IOException {
        hps = createProcessingService();
        registry = WorkflowFactoryRegistry.getInstance();
    }

    private static HadoopProcessingService createProcessingService() throws IOException {
        JobConf jobConf = new JobConf();
        //TODO make this configurable
        jobConf.set("fs.default.name", "hdfs://cvmaster00:9000");
        jobConf.set("mapred.job.tracker", "cvmaster00:9001");
        JobClient jobClient = new JobClient(jobConf);
        return new HadoopProcessingService(jobClient);
    }

    private void execute(String[] args) throws InterruptedException, IOException {

        String command = args[0];
        Set<String> commandNames = registry.getNames();
        if (!commandNames.contains(command)) {
            System.err.println("Unkown command: '" + command + "'");
            System.err.println();
            showHelp(registry, System.out);
            System.exit(1);
        }
        String[] remainingArgs = new String[args.length - 1];
        System.arraycopy(args, 1, remainingArgs, 0, remainingArgs.length);
        WorkflowFactory workflowFactory = registry.getWorkflowFactory(command);
        WorkflowItem workflowItem = workflowFactory.create(hps, remainingArgs);
        try {
            workflowItem.submit();
        } catch (WorkflowException e) {
            System.err.println("Failed to submit job: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("===================================");
        System.out.println("Job has been submitted.");
        System.out.println("===================================");
        while (!workflowItem.getStatus().getState().isDone()) {
            Thread.sleep(1000);
            hps.updateStatuses();
            workflowItem.updateStatus();
            System.out.println(workflowItem.getStatus());
        }
    }

    private static void showHelp(WorkflowFactoryRegistry registry, PrintStream out) {
        out.println("Usage: ccc <command> [<arguments>]");
        out.println();
        out.println("Available comands:");
        for (String name : registry.getNames()) {
            WorkflowFactory workflowFactory = registry.getWorkflowFactory(name);
            out.println("  " + workflowFactory.getUsage());
        }
    }
}
