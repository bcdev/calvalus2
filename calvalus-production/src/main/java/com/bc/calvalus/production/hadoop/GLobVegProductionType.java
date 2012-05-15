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
import org.esa.beam.binning.operator.AggregatorConfig;
import org.esa.beam.binning.operator.VariableConfig;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 * A production type used for generating one or more GLobVeg Level-3 products.
 *
 * @author MarcoZ
 */
public class GLobVegProductionType extends HadoopProductionType {

    public static final String NAME = "GLobVeg";
    private static final int PERIOD_LENGTH_DEFAULT = 10;

    public GLobVegProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("GLobVeg L3 ", productionRequest);
        final String productionName = productionRequest.getProdcutionName(defaultProductionName);

        DateRange dateRange = getDateRange(productionRequest);

        String inputPath = productionRequest.getString("inputPath");
        String regionName = productionRequest.getRegionName();
        String[] inputFiles = getInputPaths(getInventoryService(), inputPath, dateRange.getStartDate(), dateRange.getStopDate(), regionName);
        if (inputFiles.length == 0) {
            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
            throw new ProductionException(String.format("No input products found for given time range. [%s - %s]", date1Str, date2Str));
        }

        String l3ConfigXml = getL3Config().toXml();

        String period = getPeriodName(productionRequest);
        String partsOutputDir = getOutputPath(productionRequest, productionId, period + "-parts");
        String ncOutputDir = getOutputPath(productionRequest, productionId, period + "-nc");

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String regionGeometryString = regionGeometry != null ? regionGeometry.toString() : "";
        Workflow.Sequential sequence = new Workflow.Sequential();

        if (!successfullyCompleted(getInventoryService(), partsOutputDir)) {
            Configuration jobConfig = createJobConfig(productionRequest);
            jobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, partsOutputDir);
            jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
            jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfig.setInt("calvalus.mosaic.macroTileSize", 10);
            jobConfig.setInt("calvalus.mosaic.tileSize", 360);
            jobConfig.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
            jobConfig.set("mapred.job.priority", "NORMAL");
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionName + " L3", jobConfig));
        }
        if (!successfullyCompleted(getInventoryService(), ncOutputDir)) {
            String outputPrefix = String.format("GlobVeg-%s", period);
            Configuration jobConfig = createJobConfig(productionRequest);
            jobConfig.set(JobConfigNames.CALVALUS_INPUT, partsOutputDir);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, ncOutputDir);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_PREFIX, outputPrefix);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4");
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "");
            jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
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

    static boolean successfullyCompleted(InventoryService inventoryService, String outputDir) {
        ArrayList<String> globs = new ArrayList<String>();
        globs.add(outputDir + "/_SUCCESS");
        try {
            String[] pathes = inventoryService.globPaths(globs);
            return pathes.length == 1;
        } catch (IOException e) {
            return false;
        }
    }

    static String createProductionName(String prefix, ProductionRequest productionRequest) throws ProductionException {
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(getPeriodName(productionRequest));
        return sb.toString().trim();
    }

    static String getPeriodName(ProductionRequest productionRequest) throws ProductionException {
        String minDate = productionRequest.getString("minDate");
        int periodLength = productionRequest.getInteger("periodLength", PERIOD_LENGTH_DEFAULT); // unit=days
        return String.format("%s-%dd", minDate, periodLength);
    }

    static DateRange getDateRange(ProductionRequest productionRequest) throws ProductionException {
        Date minDate = productionRequest.getDate("minDate");
        int periodLength = productionRequest.getInteger("periodLength", PERIOD_LENGTH_DEFAULT); // unit=days
        Calendar calendar = ProductData.UTC.createCalendar();
        calendar.setTimeInMillis(minDate.getTime());
        calendar.add(Calendar.DAY_OF_MONTH, periodLength - 1);

        return new DateRange(minDate, calendar.getTime());
    }

    static L3Config getL3Config() throws ProductionException {
        // TODO this is only an example to get is workging with MERIS L1b products
        String maskExpr = "!l1_flags.INVALID && !l1_flags.BRIGHT && l1_flags.LAND_OCEAN";

        // TODO once the L2op is ready use these varNames
        //String[] varNames = new String[]{ "time", "fapar", "lai" };
        VariableConfig[] variableConfigs = new VariableConfig[4];
        variableConfigs[0] = new VariableConfig("valid", maskExpr);

        //TODO this is just to create virtual band with some data before the L2 op is ready
        variableConfigs[1] = new VariableConfig("time", "Y");
        variableConfigs[2] = new VariableConfig("fapar", "(radiance_10 - radiance_6) / (radiance_10 + radiance_6)");
        variableConfigs[3] = new VariableConfig("lai", "(radiance_8 - radiance_2) / (radiance_8 + radiance_2)");

        String type = GlobVegMosaicAlgorithm.class.getName();

        AggregatorConfig aggregatorConfig = new AggregatorConfig(type);
//        aggregatorConfig.setVarNames(varNames);

        L3Config l3Config = new L3Config();
        l3Config.setMaskExpr(maskExpr);
        l3Config.setAggregatorConfigs(aggregatorConfig);
        l3Config.setVariableConfigs(variableConfigs);
        return l3Config;
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for lc_cci Level3.");
    }

}