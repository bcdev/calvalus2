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
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.mosaic.LCMosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.LcSDR8MosaicAlgorithm;
import com.bc.calvalus.processing.mosaic.MosaicFormattingWorkflowItem;
import com.bc.calvalus.processing.mosaic.MosaicWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.StringUtils;

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
    private static final int PERIOD_LENGTH_DEFAULT = 10;
    static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

    public LcL3ProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createL3ProductionName(productionRequest);
        final String userName = productionRequest.getUserName();

        String inputPath = productionRequest.getString("inputPath");
        // only processing one time for the time
        List<L3ProductionType.DateRange> dateRanges = getDateRanges(productionRequest, PERIOD_LENGTH_DEFAULT);

        Workflow parallel = new Workflow.Parallel();
        for (L3ProductionType.DateRange mainRange : dateRanges) {

            String date1Str = ProductionRequest.getDateFormat().format(mainRange.startDate);
            String date2Str = ProductionRequest.getDateFormat().format(mainRange.stopDate);
            L3ProductionType.DateRange cloudRange = getWingsRange(productionRequest, mainRange);

            String regionName = productionRequest.getRegionName();
            String[] cloudInputFiles = getInputPaths(getInventoryService(), inputPath, cloudRange.startDate, cloudRange.stopDate, regionName);
            String[] mainInputFiles = getInputPaths(getInventoryService(), inputPath, mainRange.startDate, mainRange.stopDate, regionName);
            if (mainInputFiles.length == 0) {
                throw new ProductionException(String.format("No input products found for given time range. [%s - %s]", date1Str, date2Str));
            }
            int periodLength = productionRequest.getInteger("periodLength", PERIOD_LENGTH_DEFAULT); // unit=days

            String cloudL3ConfigXml = getCloudL3Config().toXml();
            String mainL3ConfigXml = getMainL3Config().toXml();

            String period = String.format("%s-%dd", date1Str, periodLength);
            String meanOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-cloud");
            String mainOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-sr");
            String ncOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-nc");

            Geometry regionGeometry = productionRequest.getRegionGeometry(null);
            String regionGeometryString = regionGeometry != null ? regionGeometry.toString() : "";

            Workflow.Sequential sequence = new Workflow.Sequential();

            Configuration jobConfigCloud = createJobConfig(productionRequest);
            jobConfigCloud.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(cloudInputFiles, ","));
            jobConfigCloud.set(JobConfigNames.CALVALUS_OUTPUT_DIR, meanOutputDir);
            jobConfigCloud.set(JobConfigNames.CALVALUS_L3_PARAMETERS, cloudL3ConfigXml);
            jobConfigCloud.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfigCloud.set("mapred.job.priority", "VERY_LOW");
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionId + "_" +period + "_cloud", jobConfigCloud));

            Configuration jobConfigSr = createJobConfig(productionRequest);
            jobConfigSr.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(mainInputFiles, ","));
            jobConfigSr.set(JobConfigNames.CALVALUS_OUTPUT_DIR, mainOutputDir);
            jobConfigSr.set(JobConfigNames.CALVALUS_L3_PARAMETERS, mainL3ConfigXml);
            jobConfigSr.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfigSr.set(LCMosaicAlgorithm.CALVALUS_LC_SDR8_MEAN, meanOutputDir);
            jobConfigSr.set("mapred.job.priority", "LOW");
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionId + "_" +period + "_sr", jobConfigSr));

            String outputPrefix = String.format("CCI-LC-MERIS-SR-L3-300m-v3.0--%s", period);
            Configuration jobConfigFormat = createJobConfig(productionRequest);
            jobConfigFormat.set(JobConfigNames.CALVALUS_INPUT, mainOutputDir);
            jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_DIR, ncOutputDir);
            jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_PREFIX, outputPrefix);
            jobConfigFormat.set(JobConfigNames.CALVALUS_L3_PARAMETERS, mainL3ConfigXml);
            jobConfigFormat.set("mapred.job.priority", "NORMAL");
            // TODO add support for local formatting
//        jobConfigFormat.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            sequence.add(new MosaicFormattingWorkflowItem(getProcessingService(), productionId + "_" +period + "_nc", jobConfigFormat));

            parallel.add(sequence);
        }


        String stagingDir = userName + "/" + productionId;
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              null, // no dedicated output directory
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              parallel);
    }

    static List<L3ProductionType.DateRange> getDateRanges(ProductionRequest productionRequest, int periodLengthDefault) throws ProductionException {
        List<L3ProductionType.DateRange> dateRangeList = new ArrayList<L3ProductionType.DateRange>();

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

            dateRangeList.add(new L3ProductionType.DateRange(calendar1.getTime(), calendar2.getTime()));
            calendar2.add(Calendar.DAY_OF_MONTH, 1);
            time = calendar2.getTimeInMillis();
        }

        return dateRangeList;
    }


    static L3ProductionType.DateRange getWingsRange(ProductionRequest productionRequest, L3ProductionType.DateRange mainRange) throws ProductionException {
        int wings = productionRequest.getInteger("wings", 10);

        Calendar calendar = ProductData.UTC.createCalendar();
        calendar.setTime(mainRange.startDate);
        calendar.add(Calendar.DAY_OF_MONTH, -wings);
        Date date1 = calendar.getTime();
        calendar.setTime(mainRange.stopDate);
        calendar.add(Calendar.DAY_OF_MONTH, wings);
        Date date2 = calendar.getTime();
        return new L3ProductionType.DateRange(date1, date2);
    }

    static L3Config getCloudL3Config() throws ProductionException {
        String maskExpr = "status == 1";
        String[] varNames = new String[]{"status", "sdr_8"};
        String type = LcSDR8MosaicAlgorithm.class.getName();

        return createL3Config(type, maskExpr, varNames);
    }

    static L3Config getMainL3Config() throws ProductionException {
        String maskExpr = "status == 1 or status == 3";
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
        L3Config l3Config = new L3Config();
        l3Config.setMaskExpr(maskExpr);
        L3Config.AggregatorConfiguration conf = new L3Config.AggregatorConfiguration(type);
        conf.setVarNames(varNames);
        l3Config.setAggregators(conf);
        return l3Config;
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for lc_cci Level3.");
    }

    static String createL3ProductionName(ProductionRequest productionRequest) throws ProductionException {
        return String.format("lc_cci Level 3 production using input path '%s'", productionRequest.getString("inputPath"));
    }
}
