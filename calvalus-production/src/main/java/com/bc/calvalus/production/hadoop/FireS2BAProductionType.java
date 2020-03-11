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

package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.fire.S2BaInputFormat;
import com.bc.calvalus.processing.fire.S2BaPostInputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * The production type used for generating the S2 BA data.
 *
 * @author thomas
 */
@SuppressWarnings("unused")
public class FireS2BAProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new FireS2BAProductionType(fileSystemService, processing, staging);
        }

    }

    private FireS2BAProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                                   StagingService stagingService) {
        super("Fire-S2-BA", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = "Fire S2 BA" + " " + productionRequest.getString("calvalus.year");
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration s2BaPeriodJobConfig = createJobConfig(productionRequest);
        Configuration s2BaPostJobConfig = createJobConfig(productionRequest);
        String outputPath = getOutputPath(productionRequest, productionId, "S2-BA");
        s2BaPeriodJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputPath);
        s2BaPeriodJobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
//        s2BaPeriodJobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, productionRequest.getString("processorName"));

        s2BaPostJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputPath);
        s2BaPostJobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
//        s2BaPostJobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, productionRequest.getString("processorName"));

        setRequestParameters(productionRequest, s2BaPeriodJobConfig);
        setRequestParameters(productionRequest, s2BaPostJobConfig);

        String processorBundle = productionRequest.getParameter("processorBundleName", true) + "-" + productionRequest.getParameter("processorBundleVersion", true);
        s2BaPeriodJobConfig.set(JobConfigNames.CALVALUS_BUNDLES, processorBundle);
        s2BaPostJobConfig.set(JobConfigNames.CALVALUS_BUNDLES, processorBundle);
        Workflow.Sequential s2BAWorkflow = new Workflow.Sequential();
        String userName = productionRequest.getUserName();

        s2BaPeriodJobConfig.set("mapreduce.map.class", productionRequest.getString("calvalus.s2baperiod.map.class"));
        s2BaPostJobConfig.set("mapreduce.map.class", productionRequest.getString("calvalus.s2bapost.map.class"));

        S2BaPeriodWorkflowItem s2BaPeriodWorkflowItem = new S2BaPeriodWorkflowItem(getProcessingService(), userName, productionName, s2BaPeriodJobConfig);
        S2BaPostWorkflowItem s2BaPostWorkflowItem = new S2BaPostWorkflowItem(getProcessingService(), userName, productionName + " - Post processing", s2BaPostJobConfig);

        if (!onlyDoPostProcessing(productionRequest)) {
            s2BAWorkflow.add(s2BaPeriodWorkflowItem);
        } else {
            CalvalusLogger.getLogger().info("Skipping period workflow.");
        }
        if (!onlyDoPreProcessing(productionRequest)) {
            s2BAWorkflow.add(s2BaPostWorkflowItem);
        } else {
            CalvalusLogger.getLogger().info("Skipping merging workflow.");
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        return new Production(productionId,
                productionName,
                null, // no dedicated output directory
                stagingDir,
                false,
                productionRequest,
                s2BAWorkflow);
    }

    private boolean onlyDoPostProcessing(ProductionRequest productionRequest) throws ProductionException {
        return productionRequest.getBoolean("calvalus.s2ba.onlyPostProcessing", false);
    }

    private boolean onlyDoPreProcessing(ProductionRequest productionRequest) throws ProductionException {
        return productionRequest.getBoolean("calvalus.s2ba.onlyPreProcessing", false);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for fire-cci MERIS BA.");
    }

    private static class S2BaPeriodWorkflowItem extends HadoopWorkflowItem {

        S2BaPeriodWorkflowItem(HadoopProcessingService processingService, String userName, String jobName, Configuration jobConfig) {
            super(processingService, userName, jobName, jobConfig);
        }

        @Override
        public String getOutputDir() {
            return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
        }

        @Override
        protected void configureJob(Job job) throws IOException {
            job.setInputFormatClass(S2BaInputFormat.class);
            job.setNumReduceTasks(0);
            job.setOutputFormatClass(SimpleOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
        }

        @Override
        protected String[][] getJobConfigDefaults() {
            return new String[][]{
                /* {JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, NO_DEFAULT}, */
                    {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                    {JobConfigNames.CALVALUS_BUNDLES, NO_DEFAULT},
                    {JobConfigNames.CALVALUS_BUNDLES, NO_DEFAULT},
            };
        }
    }

    private static class S2BaPostWorkflowItem extends HadoopWorkflowItem {

        S2BaPostWorkflowItem(HadoopProcessingService processingService, String userName, String jobName, Configuration jobConfig) {
            super(processingService, userName, jobName, jobConfig);
        }

        @Override
        public String getOutputDir() {
            return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
        }

        @Override
        protected void configureJob(Job job) {
            job.setInputFormatClass(S2BaPostInputFormat.class);
            job.setNumReduceTasks(0);
            job.setOutputFormatClass(SimpleOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
        }

        @Override
        protected String[][] getJobConfigDefaults() {
            return new String[][]{
                /* {JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, NO_DEFAULT}, */
                    {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                    {JobConfigNames.CALVALUS_BUNDLES, NO_DEFAULT},
                    {JobConfigNames.CALVALUS_BUNDLES, NO_DEFAULT},
            };
        }
    }
}
