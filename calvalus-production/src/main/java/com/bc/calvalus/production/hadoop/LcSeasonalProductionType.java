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

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.mosaic.MosaicFormattingWorkflowItem;
import com.bc.calvalus.processing.mosaic.MosaicSeasonalWorkflowItem;
import com.bc.calvalus.processing.mosaic.landcover.LCSeasonMosaicAlgorithm;
import com.bc.calvalus.production.DateRange;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.operator.AggregatorConfig;
import org.esa.beam.util.StringUtils;

/**
 * A production type used for generating one or more lc_cci Level-3 products.
 *
 * @author MarcoZ
 */
public class LcSeasonalProductionType extends HadoopProductionType {

    public static final String NAME = "LCL3Seasonal";
    private static final int PERIOD_LENGTH_DEFAULT = 7;

    public LcSeasonalProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = LcL3ProductionType.createLcProductionName("Level 3 LC Seasonal", productionRequest);
        final String productionName = productionRequest.getProdcutionName(defaultProductionName);

        DateRange mainRange = DateRange.createFromMinMax(productionRequest);

        String inputPath = productionRequest.getString("inputPath");
        String regionName = productionRequest.getRegionName();
        String[] mainInputFiles = getInputPaths(getInventoryService(), inputPath, mainRange.getStartDate(), mainRange.getStopDate(), regionName);

        System.out.println("mainInputFiles = " + mainInputFiles.length);
        for (String mainInputFile : mainInputFiles) {
            System.out.println("mainInputFile = " + mainInputFile);
        }

        if (mainInputFiles.length == 0) {
            String date1Str = ProductionRequest.getDateFormat().format(mainRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(mainRange.getStopDate());
            throw new ProductionException(String.format("No input products found for given time range. [%s - %s]", date1Str, date2Str));
        }

        String mainL3ConfigXml = getMainL3Config().toXml();

        String period = LcL3ProductionType.getLcPeriodName(productionRequest);
        String mainOutputDir = getOutputPath(productionRequest, productionId, period + "-sr");
        String ncOutputDir = getOutputPath(productionRequest, productionId, period + "-nc");

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String regionGeometryString = regionGeometry != null ? regionGeometry.toString() : "";

        String resolution = productionRequest.getString("resolution", "FR");
        int mosaicTileSize = 360;
        String groundResultion = "300m";
        if (resolution.equals("RR")) {
            mosaicTileSize = 90;
            groundResultion = "1000m";
        }
        Workflow.Sequential sequence = new Workflow.Sequential();
        if (!successfullyCompleted(mainOutputDir)) {
            Configuration jobConfigSr = createJobConfig(productionRequest);
            jobConfigSr.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(mainInputFiles, ","));
            jobConfigSr.set(JobConfigNames.CALVALUS_OUTPUT_DIR, mainOutputDir);
            jobConfigSr.set(JobConfigNames.CALVALUS_L3_PARAMETERS, mainL3ConfigXml);
            jobConfigSr.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfigSr.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
            jobConfigSr.set("mapred.job.priority", "NORMAL");
            sequence.add(new MosaicSeasonalWorkflowItem(getProcessingService(), productionName + " SR", jobConfigSr));
        }
        if (!successfullyCompleted(ncOutputDir)) {
            String outputPrefix = String.format("CCI-LC-MERIS-SR-L3-%s-v4.0--%s", groundResultion, period);
            Configuration jobConfigFormat = createJobConfig(productionRequest);
            jobConfigFormat.set(JobConfigNames.CALVALUS_INPUT, mainOutputDir);
            jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_DIR, ncOutputDir);
            jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_NAMEFORMAT, outputPrefix+ "-v%02dh%02d");
            jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4");
            jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "");
            jobConfigFormat.set(JobConfigNames.CALVALUS_L3_PARAMETERS, mainL3ConfigXml);
            jobConfigFormat.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
            jobConfigFormat.set("mapred.job.priority", "HIGH");
            sequence.add(new MosaicFormattingWorkflowItem(getProcessingService(), productionName + " Format", jobConfigFormat));
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              null, // no dedicated output directory
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              sequence);
    }

    static L3Config getMainL3Config() throws ProductionException {
        String type = LCSeasonMosaicAlgorithm.class.getName();

        AggregatorConfig aggregatorConfig = new AggregatorConfig(type);
        aggregatorConfig.setVarNames(new String[0]);

        L3Config l3Config = new L3Config();
        l3Config.setMaskExpr("");
        l3Config.setAggregatorConfigs(aggregatorConfig);
        return l3Config;
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for lc_cci Level3.");
    }

}
