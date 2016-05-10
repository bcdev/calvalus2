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
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.fire.FirePixelInputFormat;
import com.bc.calvalus.processing.fire.FirePixelMapper;
import com.bc.calvalus.processing.fire.FirePixelProductArea;
import com.bc.calvalus.processing.fire.FirePixelReducer;
import com.bc.calvalus.processing.fire.FirePixelVariableType;
import com.bc.calvalus.processing.fire.PixelCell;
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
 * The production type used for formatting the MERIS BA data to the pixel format.
 *
 * @author thomas
 */
public class FirePixelProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new FirePixelProductionType(inventory, processing, staging);
        }
    }

    FirePixelProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                            StagingService stagingService) {
        super("Fire-Pixel-Formatting", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = String.format("Fire Pixel Formatting %s/%s", productionRequest.getString("calvalus.year"), productionRequest.getString("calvalus.month"));
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration jobConfig = createJobConfig(productionRequest);
        String outputPath = getOutputPath(productionRequest, productionId, "Fire-Pixel-Formatting");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputPath);
        setRequestParameters(productionRequest, jobConfig);

        Workflow merisFormattingWorkflow = new Workflow.Parallel();
        String userName = productionRequest.getUserName();
        for (FirePixelProductArea area : FirePixelProductArea.values()) {
            if (area == FirePixelProductArea.ASIA) {
                for (FirePixelVariableType type : FirePixelVariableType.values()) {
                    CalvalusLogger.getLogger().info(String.format("Creating workflow item for area %s and variable %s.", area, type.name()));
                    FirePixelFormattingWorkflowItem item = new FirePixelFormattingWorkflowItem(getProcessingService(), userName, productionName + "_" + area.name() + "_" + type.name(), area, type, jobConfig);
                    merisFormattingWorkflow.add(item);
                }
            }
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

    private static class FirePixelFormattingWorkflowItem extends HadoopWorkflowItem {

        private final FirePixelProductArea area;
        private final FirePixelVariableType variableType;

        FirePixelFormattingWorkflowItem(HadoopProcessingService processingService, String userName, String jobName, FirePixelProductArea area, FirePixelVariableType variableType, Configuration jobConfig) {
            super(processingService, userName, jobName, jobConfig);
            this.area = area;
            this.variableType = variableType;
        }

        @Override
        public String getOutputDir() {
            return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR) + "/" + area.name();
        }

        @Override
        protected void configureJob(Job job) throws IOException {
            CalvalusLogger.getLogger().info("Configuring job.");
            job.setInputFormatClass(FirePixelInputFormat.class);
            job.setMapperClass(FirePixelMapper.class);
            job.setReducerClass(FirePixelReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PixelCell.class);
            job.getConfiguration().set("area", area.name());
            job.getConfiguration().set("variableType", variableType.name());
            FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
        }

        @Override
        protected String[][] getJobConfigDefaults() {
            return new String[][]{
            };
        }
    }

}
