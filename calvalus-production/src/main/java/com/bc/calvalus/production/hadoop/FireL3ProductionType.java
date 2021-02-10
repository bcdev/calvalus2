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
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.mosaic.MosaicConfig;
import com.bc.calvalus.processing.mosaic.MosaicFormattingWorkflowItem;
import com.bc.calvalus.processing.mosaic.MosaicWorkflowItem;
import com.bc.calvalus.processing.mosaic.firecci.FireMosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.landcover.AbstractLcMosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.landcover.LcL3SensorConfig;
import com.bc.calvalus.processing.mosaic.landcover.LcSDR8MosaicAlgorithm;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.datamodel.ProductData;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * A production type used for generating one or more fire_cci Level-3 products.
 *
 * @author MarcoZ
 * @author thomas
 */
public class FireL3ProductionType extends HadoopProductionType {

    public static final int DEFAULT_WINGS_RANGE = 10;

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new FireL3ProductionType(fileSystemService, processing, staging);
        }
    }

    FireL3ProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                         StagingService stagingService) {
        super("FireL3", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        // production for a third of each month (9-10 days)

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createLcProductionName("Level 3 Fire ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        DateRange mainRange = productionRequest.createFromMinMax();
        List<DateRange> individualDayList = getIndividualDays(mainRange);
        DateRange cloudRange = getWingsRange(productionRequest, mainRange);

        LcL3SensorConfig sensorConfig = LcL3SensorConfig.create(productionRequest.getString("calvalus.lc.resolution"));
        String temporalCloudBandName = productionRequest.getString("calvalus.lc.temporalCloudBandName", sensorConfig.getTemporalCloudBandName());
        float temporalCloudFilterThreshold = productionRequest.getFloat("calvalus.lc.temporalCloudFilterThreshold", sensorConfig.getTemporalCloudFilterThreshold());
        final int mosaicTileSize = sensorConfig.getMosaicTileSize();

        String cloudMosaicConfigXml = getCloudMosaicConfig(temporalCloudBandName).toXml();
        String mainMosaicConfigXml = getMainMosaicConfig().toXml();

        String period = getFirePeriodName(productionRequest);
        String cloudOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-cloud");

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String regionGeometryString = regionGeometry != null ? regionGeometry.toString() : "";

        Workflow.Sequential sequence = new Workflow.Sequential();
        if (productionRequest.getBoolean("firel3.cloud", true) && !successfullyCompleted(cloudOutputDir)) {
            Configuration jobConfigCloud = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, jobConfigCloud);

            setInputLocationParameters(productionRequest, jobConfigCloud);
            jobConfigCloud.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            jobConfigCloud.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, cloudRange.toString());

            jobConfigCloud.set(JobConfigNames.CALVALUS_OUTPUT_DIR, cloudOutputDir);
            jobConfigCloud.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, cloudMosaicConfigXml);
            jobConfigCloud.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfigCloud.setIfUnset("calvalus.mosaic.macroTileSize", "10");
            jobConfigCloud.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
            jobConfigCloud.setBoolean("calvalus.system.snap.pixelGeoCoding.useTiling", true);
            jobConfigCloud.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
            jobConfigCloud.set("mapred.job.priority", "LOW");
            jobConfigCloud.set("calvalus.lc.temporalCloudFilterThreshold", String.valueOf(temporalCloudFilterThreshold));
            jobConfigCloud.set("calvalus.lc.temporalCloudBandName", temporalCloudBandName);
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                productionName + " Cloud", jobConfigCloud));
        } else {
            CalvalusLogger.getLogger().info("Not computing cloud product.");
        }
        DateFormat dateFormat = ProductionRequest.getDateFormat();
        Workflow.Parallel parallel = new Workflow.Parallel();
        for (DateRange singleDayAsRange : individualDayList) {
            String dateAsString = dateFormat.format(singleDayAsRange.getStartDate());
            String mainOutputDir = getOutputPath(productionRequest, productionId, dateAsString + "-fire-sr");
            String ncOutputDir = getOutputPath(productionRequest, productionId, dateAsString + "-fire-nc");

            Workflow.Sequential singleDaySequence = new Workflow.Sequential();
            if (productionRequest.getBoolean("firel3.sr", true) && !successfullyCompleted(mainOutputDir)) {
                Configuration jobConfigSr = createJobConfig(productionRequest);
                setRequestParameters(productionRequest, jobConfigSr);

                setInputLocationParameters(productionRequest, jobConfigSr);
                jobConfigSr.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
                jobConfigSr.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, singleDayAsRange.toString());

                jobConfigSr.set(JobConfigNames.CALVALUS_OUTPUT_DIR, mainOutputDir);
                jobConfigSr.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, mainMosaicConfigXml);
                jobConfigSr.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
                if (productionRequest.getBoolean("firel3.cloud", true)) {
                    // if cloud aggregation is disabled, don't set this property
                    jobConfigSr.set(AbstractLcMosaicAlgorithm.CALVALUS_LC_SDR8_MEAN, cloudOutputDir);
                }
                jobConfigSr.setIfUnset("calvalus.mosaic.macroTileSize", "10");
                jobConfigSr.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
                jobConfigSr.setBoolean("calvalus.system.snap.pixelGeoCoding.useTiling", true);
                jobConfigSr.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
                jobConfigSr.set("mapred.job.priority", "NORMAL");
                singleDaySequence.add(new MosaicWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                             productionName + " SR", jobConfigSr));
            } else {
                CalvalusLogger.getLogger().info("Not computing SR product.");
            }
            if (productionRequest.getBoolean("firel3.nc", true) && !successfullyCompleted(ncOutputDir)) {
                String outputPrefix = String.format("CCI-Fire-MERIS-SDR-L3-300m-v1.0-%s", dateAsString);
                Configuration jobConfigFormat = createJobConfig(productionRequest);
                setRequestParameters(productionRequest, jobConfigFormat);
                jobConfigFormat.set(JobConfigNames.CALVALUS_INPUT_DIR, mainOutputDir);
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_DIR, ncOutputDir);
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_NAMEFORMAT, outputPrefix + "-v%02dh%02d");
                jobConfigFormat.setIfUnset(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4-Fire");
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "");
                String date1Str = ProductionRequest.getDateFormat().format(mainRange.getStartDate());
                String date2Str = ProductionRequest.getDateFormat().format(mainRange.getStopDate());
                jobConfigFormat.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
                jobConfigFormat.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);
                jobConfigFormat.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, mainMosaicConfigXml);
                jobConfigFormat.setIfUnset("calvalus.mosaic.macroTileSize", "10");
                jobConfigFormat.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
                jobConfigFormat.set("mapred.job.priority", "HIGH");
                singleDaySequence.add(new MosaicFormattingWorkflowItem(getProcessingService(),
                                                                       productionRequest.getUserName(),
                                                                       productionName + " Format",
                                                                       jobConfigFormat));
            } else {
                CalvalusLogger.getLogger().info("Not formatting result.");
            }
            parallel.add(singleDaySequence);
        }
        sequence.add(parallel);

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

    static String createLcProductionName(String prefix, ProductionRequest productionRequest) throws ProductionException {
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(getFirePeriodName(productionRequest));
        return sb.toString().trim();
    }

    static String getFirePeriodName(ProductionRequest productionRequest) throws ProductionException {
        DateRange minMax = productionRequest.createFromMinMax();
        long diffMillis = minMax.getStopDate().getTime() - minMax.getStartDate().getTime() + L3ProductionType.MILLIS_PER_DAY;
        int periodLength = (int) (diffMillis / L3ProductionType.MILLIS_PER_DAY);
        String minDate = productionRequest.getString("minDate");
        String resolution = productionRequest.getString("calvalus.lc.resolution", "FR");
        return String.format("%s-%s-%dd", resolution, minDate, periodLength);
    }

    static DateRange getWingsRange(ProductionRequest productionRequest, DateRange mainRange) throws ProductionException {
        int wings = productionRequest.getInteger("wings", DEFAULT_WINGS_RANGE);

        Calendar calendar = DateUtils.createCalendar();
        calendar.setTime(mainRange.getStartDate());
        calendar.add(Calendar.DAY_OF_MONTH, -wings);
        Date date1 = calendar.getTime();
        calendar.setTime(mainRange.getStopDate());
        calendar.add(Calendar.DAY_OF_MONTH, wings);
        Date date2 = calendar.getTime();
        return new DateRange(date1, date2);
    }

    static MosaicConfig getCloudMosaicConfig(String temporalCloudBandName) throws ProductionException {
        String maskExpr = "status == 1 and not nan(" + temporalCloudBandName + ")";
        String[] varNames = new String[]{"status", temporalCloudBandName, "ndvi"};
        String type = LcSDR8MosaicAlgorithm.class.getName();

        return new MosaicConfig(type, maskExpr, varNames);
    }

    static MosaicConfig getMainMosaicConfig() throws ProductionException {
        String maskExpr = "(status == 1 or status == 2) and not nan(sdr_1)";
        String[] varNames = new String[]{
                "status",
                "sdr_1", "sdr_2", "sdr_3", "sdr_4", "sdr_5",
                "sdr_6", "sdr_7", "sdr_8", "sdr_9", "sdr_10",
                "sdr_12", "sdr_13", "sdr_14",
                "sdr_error_1", "sdr_error_2", "sdr_error_3", "sdr_error_4", "sdr_error_5",
                "sdr_error_6", "sdr_error_7", "sdr_error_8", "sdr_error_9", "sdr_error_10",
                "sdr_error_12", "sdr_error_13", "sdr_error_14",
                "ndvi", "sun_zenith", "sun_azimuth", "view_zenith", "view_azimuth"
        };
        return new MosaicConfig(FireMosaicAlgorithm.class.getName(), maskExpr, varNames);
    }

    static List<DateRange> getIndividualDays(DateRange mainRange) throws ProductionException {
        List<DateRange> dateRangeList = new ArrayList<DateRange>();

        Date minDate = mainRange.getStartDate();

        Calendar testCalendar = DateUtils.createCalendar();
        testCalendar.setTime(minDate);

        long endTime = mainRange.getStopDate().getTime();
        dateRangeList.add(new DateRange(minDate, minDate));
        while (testCalendar.getTimeInMillis() < endTime) {
            testCalendar.add(Calendar.DAY_OF_MONTH, 1);
            Date testCalendarTime = testCalendar.getTime();
            dateRangeList.add(new DateRange(testCalendarTime, testCalendarTime));
        }
        return dateRangeList;
    }


    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging currently not implemented for fire-cci Level3.");
    }

}
