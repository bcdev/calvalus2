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
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.fire.BATilesInputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.l3.seasonal.SeasonalTilesInputFormat;
import com.bc.calvalus.processing.fire.MerisBAMapper;
import com.bc.calvalus.processing.mosaic.landcover.LcL3SensorConfig;
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
 * The production type used for generating the MERIS BA data.
 *
 * @author thomas
 */
public class FireMERISBAProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new FireMERISBAProductionType(inventory, processing, staging);
        }
    }

    FireMERISBAProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                              StagingService stagingService) {
        super("Fire-MERIS-BA", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = "Fire MERIS BA" + " " + productionRequest.getString("calvalus.year");
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration jobConfig = createJobConfig(productionRequest);
        String outputPath = getOutputPath(productionRequest, productionId, "MERIS-BA");
        final int mosaicTileSize = getMosaicTileSize(productionRequest);
        jobConfig.setIfUnset("calvalus.mosaic.macroTileSize", "10");
        jobConfig.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputPath);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
        jobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, productionRequest.getString("processorName"));
        setRequestParameters(productionRequest, jobConfig);

        String processorBundle = productionRequest.getParameter("processorBundleName", true) + "-" + productionRequest.getParameter("processorBundleVersion", true);
        jobConfig.set(JobConfigNames.CALVALUS_BUNDLES, processorBundle);
        Workflow.Sequential merisBAWorkflow = new Workflow.Sequential();
        String userName = productionRequest.getUserName();
        MerisBAWorkflowItem merisBAWorkflowItem = new MerisBAWorkflowItem(getProcessingService(), userName, productionName, jobConfig);
        merisBAWorkflow.add(merisBAWorkflowItem);
        CalvalusLogger.getLogger().info("Submitting workflow.");
        try {
            merisBAWorkflow.submit();
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
                merisBAWorkflow);
    }

    private static int getMosaicTileSize(ProductionRequest productionRequest) throws ProductionException {
        LcL3SensorConfig sensorConfig = LcL3SensorConfig.create(productionRequest.getString("calvalus.lc.resolution"));
        return sensorConfig.getMosaicTileSize();
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for fire-cci MERIS BA.");
    }

    private static class MerisBAWorkflowItem extends HadoopWorkflowItem {

        public MerisBAWorkflowItem(HadoopProcessingService processingService, String userName, String jobName, Configuration jobConfig) {
            super(processingService, userName, jobName, jobConfig);
        }

        @Override
        public String getOutputDir() {
            return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
        }

        @Override
        protected void configureJob(Job job) throws IOException {
            job.setInputFormatClass(BATilesInputFormat.class);
            job.setMapperClass(MerisBAMapper.class);
            job.setOutputFormatClass(SimpleOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));

            Configuration configuration = job.getConfiguration();
            ProcessorFactory.installProcessorBundles(configuration);
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
