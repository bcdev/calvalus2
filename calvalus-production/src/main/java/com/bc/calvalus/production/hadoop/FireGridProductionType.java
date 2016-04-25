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
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.fire.FireGridInputFormat;
import com.bc.calvalus.processing.fire.FireGridMapper;
import com.bc.calvalus.processing.fire.FireGridReducer;
import com.bc.calvalus.processing.fire.GridCell;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * The production type used for formatting the MERIS BA data to the grid format.
 *
 * @author thomas
 */
public class FireGridProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new FireGridProductionType(inventory, processing, staging);
        }
    }

    FireGridProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                           StagingService stagingService) {
        super("Fire-Grid-Formatting", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = String.format("Fire Grid Formatting %s/%s", productionRequest.getString("calvalus.year"), productionRequest.getString("calvalus.month"));
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration jobConfig = createJobConfig(productionRequest);
        String outputPath = getOutputPath(productionRequest, productionId, "Fire-Grid-Formatting");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputPath);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
        setRequestParameters(productionRequest, jobConfig);

        Workflow.Sequential merisFormattingWorkflow = new Workflow.Sequential();
        String userName = productionRequest.getUserName();
        FireGridFormattingWorkflowItem fireGridFormattingWorkflowItem = new FireGridFormattingWorkflowItem(getProcessingService(), userName, productionName, jobConfig);
        merisFormattingWorkflow.add(fireGridFormattingWorkflowItem);
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
        throw new NotImplementedException("Staging currently not implemented for fire-cci MERIS BA grid formatting.");
    }

    private static class FireGridFormattingWorkflowItem extends HadoopWorkflowItem {

        public FireGridFormattingWorkflowItem(HadoopProcessingService processingService, String userName, String jobName, Configuration jobConfig) {
            super(processingService, userName, jobName, jobConfig);
        }

        @Override
        public String getOutputDir() {
            return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
        }

        @Override
        protected void configureJob(Job job) throws IOException {
            CalvalusLogger.getLogger().info("Configuring job.");
            job.setInputFormatClass(FireGridInputFormat.class);
            job.setMapperClass(FireGridMapper.class);
            job.setReducerClass(FireGridReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(GridCell.class);
            FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
        }

        @Override
        protected String[][] getJobConfigDefaults() {
            return new String[][]{
            };
        }
    }
}
