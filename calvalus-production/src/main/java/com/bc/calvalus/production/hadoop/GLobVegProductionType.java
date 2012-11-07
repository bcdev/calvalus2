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
import com.bc.calvalus.processing.mosaic.MosaicWorkflowItem;
import com.bc.calvalus.processing.mosaic.globveg.GlobVegMosaicAlgorithm;
import com.bc.calvalus.production.DateRange;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.AggregatorConfig;
import org.esa.beam.util.StringUtils;

/**
 * A production type used for generating one or more GLobVeg Level-3 products.
 *
 * @author MarcoZ
 */
public class GLobVegProductionType extends HadoopProductionType {

    public static final String NAME = "GLobVeg";

    public GLobVegProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("GLobVeg L3 ", productionRequest);
        final String productionName = productionRequest.getProdcutionName(defaultProductionName);

        DateRange dateRange = DateRange.createFromMinMax(productionRequest);

        String inputPath = productionRequest.getString("inputPath");
        String regionName = productionRequest.getRegionName();
        String[] inputFiles = getInputPaths(getInventoryService(), inputPath, dateRange.getStartDate(), dateRange.getStopDate(), regionName);
        if (inputFiles.length == 0) {
            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
            throw new ProductionException(String.format("No input products found for given time range. [%s - %s]", date1Str, date2Str));
        }

        String processorName = productionRequest.getString("processorName", null);
        String processorBundle = productionRequest.getString("processorBundle", null);
        String processorParameters = productionRequest.getString("processorParameters", "<parameters/>");

        String l3ConfigXml = getL3Config().toXml();

        String period = getGlobVegPeriodName(productionRequest);
        String partsOutputDir = getOutputPath(productionRequest, productionId, period + "-parts");
        String ncOutputDir = getOutputPath(productionRequest, productionId, period + "-nc");

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String regionGeometryString = regionGeometry != null ? regionGeometry.toString() : "";
        Workflow.Sequential sequence = new Workflow.Sequential();

        if (!successfullyCompleted(partsOutputDir)) {
            Configuration jobConfig = createJobConfig(productionRequest);
            setRequestParameters(jobConfig, productionRequest);
            jobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, partsOutputDir);

            if (processorName != null) {
                jobConfig.set(JobConfigNames.CALVALUS_L2_BUNDLE, processorBundle);
                jobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, processorName);
                jobConfig.set(JobConfigNames.CALVALUS_L2_PARAMETERS, processorParameters);
            }

            jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
            jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfig.setInt("calvalus.mosaic.macroTileSize", 10);
            jobConfig.setInt("calvalus.mosaic.tileSize", 360);
            jobConfig.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
            jobConfig.set("mapred.job.priority", "NORMAL");
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionName + " L3", jobConfig));
        }
        if (!successfullyCompleted(ncOutputDir)) {
            String outputNameFormat = "meris-globveg-" + period + "-v%02dh%02d-1.0";
            Configuration jobConfig = createJobConfig(productionRequest);
            setRequestParameters(jobConfig, productionRequest);
            jobConfig.set(JobConfigNames.CALVALUS_INPUT, partsOutputDir);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, ncOutputDir);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_NAMEFORMAT, outputNameFormat);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4");
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "");
            jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
            jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);
            jobConfig.setInt("calvalus.mosaic.macroTileSize", 10);
            jobConfig.setInt("calvalus.mosaic.tileSize", 360);
            jobConfig.set("mapred.job.priority", "HIGH");
            sequence.add(new MosaicFormattingWorkflowItem(getProcessingService(), productionName + " Format", jobConfig));
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

    static String createProductionName(String prefix, ProductionRequest productionRequest) throws ProductionException {
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(getGlobVegPeriodName(productionRequest));
        return sb.toString().trim();
    }

    static String getGlobVegPeriodName(ProductionRequest productionRequest) throws ProductionException {
        return productionRequest.getString("minDate").replaceAll("-", "");
    }

    static L3Config getL3Config() throws ProductionException {

        final String[] varNames = new String[]{"valid_fapar", "valid_lai", "obs_time", "fapar", "lai"};

        String type = GlobVegMosaicAlgorithm.class.getName();

        AggregatorConfig aggregatorConfig = new AggregatorConfig(type) {
            @Override
            public String[] getVarNames() {
                return varNames;
            }
        };

        L3Config l3Config = new L3Config();
        l3Config.setMaskExpr("valid_fapar == 1 || valid_lai == 1");
        l3Config.setAggregatorConfigs(aggregatorConfig);
        return l3Config;
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for lc_cci Level3.");
    }

}
