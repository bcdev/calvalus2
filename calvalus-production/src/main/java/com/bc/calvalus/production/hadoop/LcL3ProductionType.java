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
import com.bc.calvalus.processing.mosaic.landcover.LCMosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.landcover.LcSDR8MosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.MosaicFormattingWorkflowItem;
import com.bc.calvalus.processing.mosaic.MosaicWorkflowItem;
import com.bc.calvalus.processing.mosaic.landcover.StatusRemapper;
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
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * A production type used for generating one or more lc_cci Level-3 products.
 *
 * @author MarcoZ
 */
public class LcL3ProductionType extends HadoopProductionType {

    public static final String NAME = "LCL3";
    private static final int PERIOD_LENGTH_DEFAULT = 7;

    public LcL3ProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Level 3 LC ", productionRequest);
        final String productionName = productionRequest.getProdcutionName(defaultProductionName);

        DateRange mainRange = getDateRange(productionRequest);
        DateRange cloudRange = getWingsRange(productionRequest, mainRange);

        String inputPath = productionRequest.getString("inputPath");
        String regionName = productionRequest.getRegionName();
        String[] cloudInputFiles = getInputPaths(getInventoryService(), inputPath, cloudRange.getStartDate(), cloudRange.getStopDate(), regionName);
        String[] mainInputFiles = getInputPaths(getInventoryService(), inputPath, mainRange.getStartDate(), mainRange.getStopDate(), regionName);
        if (mainInputFiles.length == 0) {
            String date1Str = ProductionRequest.getDateFormat().format(mainRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(mainRange.getStopDate());
            throw new ProductionException(String.format("No input products found for given time range. [%s - %s]", date1Str, date2Str));
        }

        String cloudL3ConfigXml = getCloudL3Config(productionRequest).toXml();
        String mainL3ConfigXml = getMainL3Config().toXml();

        String period = getPeriodName(productionRequest);
        String meanOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-cloud");
        String mainOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-sr");
        String ncOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-nc");

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
        if (productionRequest.getBoolean("lcl3.cloud", true) && !successfullyCompleted(getInventoryService(), meanOutputDir)) {
            Configuration jobConfigCloud = createJobConfig(productionRequest);
            jobConfigCloud.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(cloudInputFiles, ","));
            jobConfigCloud.set(JobConfigNames.CALVALUS_OUTPUT_DIR, meanOutputDir);
            jobConfigCloud.set(JobConfigNames.CALVALUS_L3_PARAMETERS, cloudL3ConfigXml);
            jobConfigCloud.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfigCloud.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
            jobConfigCloud.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
            jobConfigCloud.set("mapred.job.priority", "LOW");
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionName + " Cloud", jobConfigCloud));
        }
        if (productionRequest.getBoolean("lcl3.sr", true) && !successfullyCompleted(getInventoryService(), mainOutputDir)) {
            Configuration jobConfigSr = createJobConfig(productionRequest);
            jobConfigSr.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(mainInputFiles, ","));
            jobConfigSr.set(JobConfigNames.CALVALUS_OUTPUT_DIR, mainOutputDir);
            jobConfigSr.set(JobConfigNames.CALVALUS_L3_PARAMETERS, mainL3ConfigXml);
            jobConfigSr.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            if (productionRequest.getBoolean("lcl3.cloud", true)) {
                // if cloud aggregation is disabled, don't set this property
                jobConfigSr.set(LCMosaicAlgorithm.CALVALUS_LC_SDR8_MEAN, meanOutputDir);
            }
            jobConfigSr.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
            jobConfigSr.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
            jobConfigSr.set("mapred.job.priority", "NORMAL");
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionName + " SR", jobConfigSr));
        }
        if (productionRequest.getBoolean("lcl3.nc", true) && !successfullyCompleted(getInventoryService(), ncOutputDir)) {
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
        String resolution = productionRequest.getString("resolution", "FR");
        return String.format("%s-%s-%dd", resolution, minDate, periodLength);
    }

    static DateRange getDateRange(ProductionRequest productionRequest) throws ProductionException {
        Date minDate = productionRequest.getDate("minDate");
        Date maxDate = productionRequest.getDate("maxDate");
        return new DateRange(minDate, maxDate);
    }

    static DateRange getDateRange_OLD(ProductionRequest productionRequest) throws ProductionException {
        Date minDate = productionRequest.getDate("minDate");
        int periodLength = productionRequest.getInteger("periodLength", PERIOD_LENGTH_DEFAULT); // unit=days
        Calendar calendar = ProductData.UTC.createCalendar();
        calendar.setTimeInMillis(minDate.getTime());
        calendar.add(Calendar.DAY_OF_MONTH, periodLength - 1);

        return new DateRange(minDate, calendar.getTime());
    }

    // 7, or 10 days periods, that are adjusted to full month
    static List<DateRange> getDateRanges(ProductionRequest productionRequest, int periodLengthDefault) throws ProductionException {
        List<DateRange> dateRangeList = new ArrayList<DateRange>();

        Date minDate = productionRequest.getDate("minDate");
        Date maxDate = productionRequest.getDate("maxDate");
        int periodLength = productionRequest.getInteger("periodLength", periodLengthDefault); // unit=days

        Calendar calendarMax = ProductData.UTC.createCalendar();
        calendarMax.setTime(maxDate);

        long time = minDate.getTime();
        for (int i = 0; ; i++) {
            Calendar calendar1 = ProductData.UTC.createCalendar();
            Calendar calendar2 = ProductData.UTC.createCalendar();
            calendar1.setTimeInMillis(time);
            calendar2.setTimeInMillis(time);
            calendar2.add(Calendar.DAY_OF_MONTH, periodLength - 1);

            if (calendar2.after(calendarMax)) {
                break;
            }
            // check if next period wraps into the next month
            Calendar calendar3 = ProductData.UTC.createCalendar();
            calendar3.setTimeInMillis(time);
            calendar3.add(Calendar.DAY_OF_MONTH, periodLength + periodLength - 1);
            if (calendar3.get(Calendar.MONTH) != calendar2.get(Calendar.MONTH)) {
                calendar2.set(Calendar.DAY_OF_MONTH, calendar2.getActualMaximum(Calendar.DAY_OF_MONTH));
            }

            dateRangeList.add(new DateRange(calendar1.getTime(), calendar2.getTime()));
            calendar2.add(Calendar.DAY_OF_MONTH, 1);
            time = calendar2.getTimeInMillis();
        }

        return dateRangeList;
    }


    static DateRange getWingsRange(ProductionRequest productionRequest, DateRange mainRange) throws ProductionException {
        int wings = productionRequest.getInteger("wings", 10);

        Calendar calendar = ProductData.UTC.createCalendar();
        calendar.setTime(mainRange.getStartDate());
        calendar.add(Calendar.DAY_OF_MONTH, -wings);
        Date date1 = calendar.getTime();
        calendar.setTime(mainRange.getStopDate());
        calendar.add(Calendar.DAY_OF_MONTH, wings);
        Date date2 = calendar.getTime();
        return new DateRange(date1, date2);
    }

    static L3Config getCloudL3Config(ProductionRequest productionRequest) throws ProductionException {
        String asLandText = productionRequest.getString("calvalus.lc.remapAsLand", null);
        String maskExpr;
        if (asLandText != null) {
            StatusRemapper statusRemapper = StatusRemapper.create(asLandText);
            int[] statusesToLand = statusRemapper.getStatusesToLand();
            StringBuilder sb = new StringBuilder();
            for (int i : statusesToLand) {
                sb.append(" or status == ");
                sb.append(i);
            }
            maskExpr = "(status == 1 "+ sb.toString() + ") and not nan(sdr_8)";
        } else {
            maskExpr = "status == 1 and not nan(sdr_8)";
        }
        String[] varNames = new String[]{"status", "sdr_8"};
        String type = LcSDR8MosaicAlgorithm.class.getName();

        return createL3Config(type, maskExpr, varNames);
    }

    static L3Config getMainL3Config() throws ProductionException {
        // exclude invalid and deep water pixels
        String maskExpr = "(status == 1 or (status == 2 and not nan(sdr_1)) or status == 3 or ((status >= 4) and dem_alt > -100))";

        String[] varNames = new String[]{"status",
                "sdr_1", "sdr_2", "sdr_3", "sdr_4", "sdr_5",
                "sdr_6", "sdr_7", "sdr_8", "sdr_9", "sdr_10",
                "sdr_11", "sdr_12", "sdr_13", "sdr_14", "sdr_15",
                "ndvi",
                "sdr_error_1", "sdr_error_2", "sdr_error_3", "sdr_error_4", "sdr_error_5",
                "sdr_error_6", "sdr_error_7", "sdr_error_8", "sdr_error_9", "sdr_error_10",
                "sdr_error_11", "sdr_error_12", "sdr_error_13", "sdr_error_14", "sdr_error_15",
        };
        String type = LCMosaicAlgorithm.class.getName();

        return createL3Config(type, maskExpr, varNames);
    }

    private static L3Config createL3Config(String type, String maskExpr, String[] varNames) {

        AggregatorConfig aggregatorConfig = new AggregatorConfig(type);
        aggregatorConfig.setVarNames(varNames);

        L3Config l3Config = new L3Config();
        l3Config.setMaskExpr(maskExpr);
        l3Config.setAggregatorConfigs(aggregatorConfig);
        return l3Config;
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for lc_cci Level3.");
    }

}
