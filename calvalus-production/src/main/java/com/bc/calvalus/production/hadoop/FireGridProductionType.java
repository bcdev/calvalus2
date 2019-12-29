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
import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.fire.FireFormattingMapper;
import com.bc.calvalus.processing.fire.FirePixelInputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * The production type used for formatting the MERIS BA data to a grid product.
 *
 * @author thomas
 */
public class FireGridProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new FireGridProductionType(fileSystemService, processing, staging);
        }
    }

    FireGridProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                           StagingService stagingService) {
        super("Fire-Formatting", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = String.format("Fire Formatting %s/%s", productionRequest.getString("calvalus.year"), productionRequest.getString("calvalus.month"));
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration jobConfig = createJobConfig(productionRequest);
        String outputPath = getOutputPath(productionRequest, productionId, "Fire-Formatting");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputPath);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
        jobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, productionRequest.getString("processorName"));
        setRequestParameters(productionRequest, jobConfig);

        String processorBundle = productionRequest.getParameter("processorBundleName", true) + "-" + productionRequest.getParameter("processorBundleVersion", true);
        jobConfig.set(JobConfigNames.CALVALUS_BUNDLES, processorBundle);
        Workflow.Sequential merisFormattingWorkflow = new Workflow.Sequential();
        String userName = productionRequest.getUserName();
        MerisFormattingWorkflowItem merisFormattingWorkflowItem = new MerisFormattingWorkflowItem(getProcessingService(), userName, productionName, jobConfig);
        merisFormattingWorkflow.add(merisFormattingWorkflowItem);
        CalvalusLogger.getLogger().info("Submitting workflow.");
        try {
            merisFormattingWorkflow.submit();
        } catch (WorkflowException e) {
            throw new ProductionException(e);
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        return new Production(productionId,
                productionName,
                null, // no dedicated output directory
                stagingDir,
                false,
                productionRequest,
                merisFormattingWorkflow);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging currently not implemented for fire-cci MERIS BA.");
    }

    private static class MerisFormattingWorkflowItem extends HadoopWorkflowItem {

        public MerisFormattingWorkflowItem(HadoopProcessingService processingService, String userName, String jobName, Configuration jobConfig) {
            super(processingService, userName, jobName, jobConfig);
        }

        @Override
        public String getOutputDir() {
            return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
        }

        @Override
        protected void configureJob(Job job) throws IOException {
            CalvalusLogger.getLogger().info("Configuring job.");
            job.setInputFormatClass(FirePixelInputFormat.class);
            job.setMapperClass(FireFormattingMapper.class);
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
