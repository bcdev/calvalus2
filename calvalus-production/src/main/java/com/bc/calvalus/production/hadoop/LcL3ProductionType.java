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

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.mosaic.MosaicFormattingWorkflowItem;
import com.bc.calvalus.processing.mosaic.MosaicWorkflowItem;
import com.bc.calvalus.processing.mosaic.landcover.AbstractLcMosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.landcover.LcL3SensorConfig;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;

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

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new LcL3ProductionType(fileSystemService, processing, staging);
        }
    }

    private static final int PERIOD_LENGTH_DEFAULT = 7;

    LcL3ProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                       StagingService stagingService) {
        super("LCL3", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createLcProductionName("Level 3 LC ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        DateRange mainRange = productionRequest.createFromMinMax();
        DateRange cloudRange = getWingsRange(productionRequest, mainRange);

        String resolution = productionRequest.getString("resolution", "FR");
        String sensor = productionRequest.getString("sensor");
        String spatialResolution = productionRequest.getString("spatialResolution");
        LcL3SensorConfig sensorConfig = LcL3SensorConfig.create(sensor, spatialResolution);
        int mosaicTileSize = sensorConfig.getMosaicTileSize();
        String groundResolution = sensorConfig.getGroundResolution();
        String sensorName = sensorConfig.getSensorName();
        String temporalCloudBandName = productionRequest.getString("calvalus.lc.temporalCloudBandName", sensorConfig.getTemporalCloudBandName());
        float temporalCloudFilterThreshold = productionRequest.getFloat("calvalus.lc.temporalCloudFilterThreshold", sensorConfig.getTemporalCloudFilterThreshold());

        String outputVersion = productionRequest.getString("calvalus.output.version", "1.0");

        int cloudBorderWidth = productionRequest.getInteger("cloudBorderWidth", 0);  // was 150
        int mainBorderWidth = productionRequest.getInteger("mainBorderWidth", 0);  // was 250

        String cloudMosaicConfigXml = sensorConfig.getCloudMosaicConfig(temporalCloudBandName, productionRequest.getString("calvalus.lc.remapAsLand", null), cloudBorderWidth).toXml();
        String mainMosaicConfigXml = sensorConfig.getMainMosaicConfig(productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4"), mainBorderWidth).toXml();

        String period = getLcPeriodName(productionRequest);
        String meanOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-cloud");
        String mainOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-sr");
        String ncOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-nc");

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String regionGeometryString = regionGeometry != null ? regionGeometry.toString() : "";


        Workflow.Sequential sequence = new Workflow.Sequential();
        if (productionRequest.getBoolean("lcl3.cloud", true) && !successfullyCompleted(meanOutputDir)) {
            Configuration jobConfigCloud = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, jobConfigCloud);
            if (productionRequest.getParameters().containsKey("processorName")) {
                ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
                setDefaultProcessorParameters(processorProductionRequest, jobConfigCloud);
                processorProductionRequest.configureProcessor(jobConfigCloud);
            }

            setInputLocationParameters(productionRequest, jobConfigCloud);
            jobConfigCloud.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            jobConfigCloud.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, cloudRange.toString());

            jobConfigCloud.set(JobConfigNames.CALVALUS_OUTPUT_DIR, meanOutputDir);
            jobConfigCloud.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, cloudMosaicConfigXml);
            jobConfigCloud.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfigCloud.set("calvalus.lc.sensor", sensorName);
            jobConfigCloud.set("calvalus.lc.temporalCloudBandName", temporalCloudBandName);
            jobConfigCloud.set("calvalus.lc.temporalCloudFilterThreshold", String.valueOf(temporalCloudFilterThreshold));
            jobConfigCloud.set("calvalus.lc.version", outputVersion);
            jobConfigCloud.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
            jobConfigCloud.setBoolean("calvalus.system.snap.pixelGeoCoding.useTiling", true);
            if ("MSI".equals(sensorName) || "AGRI".equals(sensorName)) {
                jobConfigCloud.setIfUnset("calvalus.mosaic.numTileY", "900");
            }
            if ("VEGETATION".equals(sensorName)|| "PROBAV".equals(sensorName)) {
                jobConfigCloud.setIfUnset("calvalus.mosaic.withIntersectionCheck", "false");
            }
            jobConfigCloud.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
            //jobConfigCloud.set("mapred.job.priority", "LOW");
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                productionName + " Cloud", jobConfigCloud));
        }
        if (productionRequest.getBoolean("lcl3.sr", true) && !successfullyCompleted(mainOutputDir)) {
            Configuration jobConfigSr = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, jobConfigSr);
            if (productionRequest.getParameters().containsKey("processorName")) {
                ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
                setDefaultProcessorParameters(processorProductionRequest, jobConfigSr);
                processorProductionRequest.configureProcessor(jobConfigSr);
            }

            setInputLocationParameters(productionRequest, jobConfigSr);
            jobConfigSr.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            jobConfigSr.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, mainRange.toString());

            jobConfigSr.set(JobConfigNames.CALVALUS_OUTPUT_DIR, mainOutputDir);
            jobConfigSr.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, mainMosaicConfigXml);
            jobConfigSr.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            if (productionRequest.getBoolean("lcl3.cloud", true)) {
                // if cloud aggregation is disabled, don't set this property
                jobConfigSr.set(AbstractLcMosaicAlgorithm.CALVALUS_LC_SDR8_MEAN, meanOutputDir);
                jobConfigSr.set("calvalus.lc.temporalCloudBandName", temporalCloudBandName);
            }
            jobConfigSr.set("calvalus.lc.sensor", sensorName);
            jobConfigSr.set("calvalus.lc.version", outputVersion);
            jobConfigSr.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
            if ("MSI".equals(sensorName) || "AGRI".equals(sensorName)) {
                jobConfigSr.setIfUnset("calvalus.mosaic.numTileY", "900");
            }
            jobConfigSr.setBoolean("calvalus.system.snap.pixelGeoCoding.useTiling", true);
            if ("VEGETATION".equals(sensorName) || "PROBAV".equals(sensorName)) {
                jobConfigSr.setIfUnset("calvalus.mosaic.withIntersectionCheck", "false");
            }
            jobConfigSr.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
            jobConfigSr.set("calvalus.lc.resolution", resolution);
            jobConfigSr.set("spatialResolution", spatialResolution);
            //jobConfigSr.set("mapred.job.priority", "NORMAL");
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                productionName + " SR", jobConfigSr));
        }
        if (productionRequest.getBoolean("lcl3.nc", true) && !successfullyCompleted(ncOutputDir)) {
            String outputPrefix = String.format("CCI-LC-MERIS-SR-L3-%s-v4.0--%s", groundResolution, period);
            Configuration jobConfigFormat = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, jobConfigFormat);
            jobConfigFormat.set(JobConfigNames.CALVALUS_INPUT_DIR, mainOutputDir);
            jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_DIR, ncOutputDir);
            jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_NAMEFORMAT, outputPrefix + "-v%02dh%02d");
            jobConfigFormat.setIfUnset(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4");
            jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "");
            String date1Str = ProductionRequest.getDateFormat().format(mainRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(mainRange.getStopDate());
            jobConfigFormat.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            jobConfigFormat.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);
            jobConfigFormat.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, mainMosaicConfigXml);
            jobConfigFormat.set("calvalus.lc.sensor", sensorName);
            jobConfigFormat.set("calvalus.lc.version", outputVersion);
            jobConfigFormat.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
            if ("MSI".equals(sensorName) || "AGRI".equals(sensorName)) {
                jobConfigFormat.setIfUnset("calvalus.mosaic.numTileY", "900");
            }
            if ("VEGETATION".equals(sensorName) || "PROBAV".equals(sensorName)) {
                jobConfigFormat.setIfUnset("calvalus.mosaic.withIntersectionCheck", "false");
            }
            jobConfigFormat.set("calvalus.lc.resolution", resolution);
            //jobConfigFormat.set("mapred.job.priority", "HIGH");
            sequence.add(new MosaicFormattingWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                          productionName + " Format", jobConfigFormat));
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

    static String createLcProductionName(String prefix, ProductionRequest productionRequest) throws
            ProductionException {
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(getLcPeriodName(productionRequest));
        return sb.toString().trim();
    }

    static String getLcPeriodName(ProductionRequest productionRequest) throws ProductionException {
        DateRange minMax = productionRequest.createFromMinMax();
        long diffMillis = minMax.getStopDate().getTime() - minMax.getStartDate().getTime() + L3ProductionType.MILLIS_PER_DAY;
        int periodLength = (int) (diffMillis / L3ProductionType.MILLIS_PER_DAY);
        String minDate = productionRequest.getString("minDate");
        String resolution = productionRequest.getString("resolution", "FR");
        return String.format("%s-%s-%dd", resolution, minDate, periodLength);
    }

    static DateRange getDateRange_OLD(ProductionRequest productionRequest) throws ProductionException {
        Date minDate = productionRequest.getDate("minDate");
        int periodLength = productionRequest.getInteger("periodLength", PERIOD_LENGTH_DEFAULT); // unit=days
        Calendar calendar = DateUtils.createCalendar();
        calendar.setTimeInMillis(minDate.getTime());
        calendar.add(Calendar.DAY_OF_MONTH, periodLength - 1);

        return new DateRange(minDate, calendar.getTime());
    }

    // 7, or 10 days periods, that are adjusted to full month
    static List<DateRange> getDateRanges(ProductionRequest productionRequest, int periodLengthDefault) throws
            ProductionException {
        List<DateRange> dateRangeList = new ArrayList<DateRange>();

        Date minDate = productionRequest.getDate("minDate");
        Date maxDate = productionRequest.getDate("maxDate");
        int periodLength = productionRequest.getInteger("periodLength", periodLengthDefault); // unit=days

        Calendar calendarMax = DateUtils.createCalendar();
        calendarMax.setTime(maxDate);

        long time = minDate.getTime();
        while (true) {
            Calendar calendar1 = DateUtils.createCalendar();
            Calendar calendar2 = DateUtils.createCalendar();
            calendar1.setTimeInMillis(time);
            calendar2.setTimeInMillis(time);
            calendar2.add(Calendar.DAY_OF_MONTH, periodLength - 1);

            if (calendar2.after(calendarMax)) {
                break;
            }
            // check if next period wraps into the next month
            Calendar calendar3 = DateUtils.createCalendar();
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

    static DateRange getWingsRange(ProductionRequest productionRequest, DateRange mainRange) throws
            ProductionException {
        int wings = productionRequest.getInteger("wings", 10);

        Calendar calendar = DateUtils.createCalendar();
        calendar.setTime(mainRange.getStartDate());
        calendar.add(Calendar.DAY_OF_MONTH, -wings);
        Date date1 = calendar.getTime();
        calendar.setTime(mainRange.getStopDate());
        calendar.add(Calendar.DAY_OF_MONTH, wings);
        Date date2 = calendar.getTime();
        return new DateRange(date1, date2);
    }


    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging currently not implemented for lc_cci Level3.");
    }

}
