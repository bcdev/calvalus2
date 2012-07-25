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
import com.bc.calvalus.processing.mosaic.MosaicFormattingWorkflowItem;
import com.bc.calvalus.processing.mosaic.MosaicWorkflowItem;
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

import java.util.*;

/**
 * A production type used for generating one or more lc_cci Level-3 products
 * from a day by day mixture of RR and FF products. Mostly for quality analysises.
 *
 * @author MarcoZ
 */
public class LcL3FrRrProductionType extends HadoopProductionType {

    public static final String NAME = "LCL3FRRR";
    private static final int PERIOD_LENGTH_DEFAULT = 7;

    public LcL3FrRrProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Level 3 LC ", productionRequest);
        final String productionName = productionRequest.getProdcutionName(defaultProductionName);

        String mainL3ConfigXml = getMainL3Config().toXml();
        String period = getPeriodName(productionRequest);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String regionGeometryString = regionGeometry != null ? regionGeometry.toString() : "";
        int mosaicTileSize = 360;
        String groundResultion = "300m";

        Workflow.Parallel parallel = new Workflow.Parallel();
        for (int rrDays = 0; rrDays <= 7; rrDays++) {

            List<String> allInputs = new ArrayList<String>();
            DateRange mainRangeRR = getDateRangeRR(productionRequest, rrDays);
            if (mainRangeRR != null) {
                String inputPathRR = productionRequest.getString("inputPathRR");
                String[] mainInputFilesRR = getInputPaths(getInventoryService(), inputPathRR, mainRangeRR.getStartDate(), mainRangeRR.getStopDate(), null);
                if (mainInputFilesRR.length == 0) {
                    String date1Str = ProductionRequest.getDateFormat().format(mainRangeRR.getStartDate());
                    String date2Str = ProductionRequest.getDateFormat().format(mainRangeRR.getStopDate());
                    throw new ProductionException(String.format("No input products found for given time range in RR. [%s - %s]", date1Str, date2Str));
                }
                System.out.println("mainInputFilesRR.length = " + mainInputFilesRR.length);
                allInputs.addAll(Arrays.asList(mainInputFilesRR));
            }

            DateRange mainRangeFR = getDateRangeFR(productionRequest, rrDays);
            if (mainRangeFR != null) {
                String inputPathFR = productionRequest.getString("inputPathFR");
                String[] mainInputFilesFR = getInputPaths(getInventoryService(), inputPathFR, mainRangeFR.getStartDate(), mainRangeFR.getStopDate(), null);
                if (mainInputFilesFR.length == 0) {
                    String date1Str = ProductionRequest.getDateFormat().format(mainRangeFR.getStartDate());
                    String date2Str = ProductionRequest.getDateFormat().format(mainRangeFR.getStopDate());
                    throw new ProductionException(String.format("No input products found for given time range in RR. [%s - %s]", date1Str, date2Str));
                }
                System.out.println("mainInputFilesFR.length = " + mainInputFilesFR.length);
                allInputs.addAll(Arrays.asList(mainInputFilesFR));
            }

            String rr = Integer.toString(rrDays);
            String mainOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-sr-" + rr);
            String ncOutputDir = getOutputPath(productionRequest, productionId, period + "-lc-nc-" + rr);

            Workflow.Sequential sequence = new Workflow.Sequential();
            if (productionRequest.getBoolean("lcl3.sr", true) && !successfullyCompleted(mainOutputDir)) {
                Configuration jobConfigSr = createJobConfig(productionRequest);
                jobConfigSr.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(allInputs, ","));
                jobConfigSr.set(JobConfigNames.CALVALUS_OUTPUT_DIR, mainOutputDir);
                jobConfigSr.set(JobConfigNames.CALVALUS_L3_PARAMETERS, mainL3ConfigXml);
                jobConfigSr.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
                jobConfigSr.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
                jobConfigSr.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
                jobConfigSr.set("mapred.job.priority", "NORMAL");
                sequence.add(new MosaicWorkflowItem(getProcessingService(), productionName + " SR " + rr, jobConfigSr));
            }

            if (productionRequest.getBoolean("lcl3.nc", true) && !successfullyCompleted(ncOutputDir)) {
                String outputPrefix = String.format("CCI-LC-MERIS-SR-L3-%s-v4.0--%s--rrdays%s", groundResultion, period, rr);
                Configuration jobConfigFormat = createJobConfig(productionRequest);
                jobConfigFormat.set(JobConfigNames.CALVALUS_INPUT, mainOutputDir);
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_DIR, ncOutputDir);
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_NAMEFORMAT, outputPrefix+ "-v%02dh%02d");
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4");
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "");
                jobConfigFormat.set(JobConfigNames.CALVALUS_L3_PARAMETERS, mainL3ConfigXml);
                jobConfigFormat.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
                jobConfigFormat.set("mapred.job.priority", "HIGH");
                sequence.add(new MosaicFormattingWorkflowItem(getProcessingService(), productionName + " Format " + rr, jobConfigFormat));
            }

            parallel.add(sequence);
        }
        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              null, // no dedicated output directory
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              parallel);

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


    static DateRange getDateRangeRR(ProductionRequest productionRequest, int rrDays) throws ProductionException {
        if (rrDays == 0) {
            return null;
        }
        Date minDate = productionRequest.getDate("minDate");
        int periodLength = productionRequest.getInteger("periodLength", PERIOD_LENGTH_DEFAULT); // unit=days
        Calendar calendar = ProductData.UTC.createCalendar();
        calendar.setTimeInMillis(minDate.getTime());
        calendar.add(Calendar.DAY_OF_MONTH, rrDays - 1);

        return new DateRange(minDate, calendar.getTime());
    }

    static DateRange getDateRangeFR(ProductionRequest productionRequest, int rrDays) throws ProductionException {
        if (rrDays == 7) {
            return null;
        }
        Date minDate = productionRequest.getDate("minDate");
        int periodLength = productionRequest.getInteger("periodLength", PERIOD_LENGTH_DEFAULT); // unit=days
        Calendar calendar = ProductData.UTC.createCalendar();
        calendar.setTimeInMillis(minDate.getTime());
        calendar.add(Calendar.DAY_OF_MONTH, rrDays);

        Calendar calendar2 = ProductData.UTC.createCalendar();
        calendar2.setTimeInMillis(minDate.getTime());
        calendar2.add(Calendar.DAY_OF_MONTH, periodLength - 1);

        return new DateRange(calendar.getTime(), calendar2.getTime());
    }

    static L3Config getMainL3Config() throws ProductionException {
        String maskExpr = "(status == 1 or status == 3) and not nan(sdr_1)";
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
