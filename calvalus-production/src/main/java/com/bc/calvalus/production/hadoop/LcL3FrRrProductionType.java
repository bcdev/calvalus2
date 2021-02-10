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
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.mosaic.MosaicConfig;
import com.bc.calvalus.processing.mosaic.MosaicFormattingWorkflowItem;
import com.bc.calvalus.processing.mosaic.MosaicWorkflowItem;
import com.bc.calvalus.processing.mosaic.landcover.LCMosaicAlgorithm;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * A production type used for generating one or more lc_cci Level-3 products
 * from a day by day mixture of RR and FF products. Mostly for quality analysises.
 *
 * @author MarcoZ
 */
public class LcL3FrRrProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new LcL3FrRrProductionType(fileSystemService, processing, staging);
        }
    }

    private static final int PERIOD_LENGTH_DEFAULT = 7;

    LcL3FrRrProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                                  StagingService stagingService) {
        super("LCL3FRRR", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = LcL3ProductionType.createLcProductionName("Level 3 LC ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        String mosaicConfigXml = getMosaicConfig().toXml();
        String period = LcL3ProductionType.getLcPeriodName(productionRequest);

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
                String[] mainInputFilesRR = getInputPaths(getFileSystemService(), productionRequest.getUserName(),
                                                          inputPathRR,
                                                          mainRangeRR.getStartDate(), mainRangeRR.getStopDate(), null);
                if (mainInputFilesRR.length == 0) {
                    String date1Str = ProductionRequest.getDateFormat().format(mainRangeRR.getStartDate());
                    String date2Str = ProductionRequest.getDateFormat().format(mainRangeRR.getStopDate());
                    throw new ProductionException(
                            String.format("No input products found for given time range in RR. [%s - %s]", date1Str,
                                          date2Str));
                }
                System.out.println("mainInputFilesRR.length = " + mainInputFilesRR.length);
                allInputs.addAll(Arrays.asList(mainInputFilesRR));
            }

            DateRange mainRangeFR = getDateRangeFR(productionRequest, rrDays);
            if (mainRangeFR != null) {
                String inputPathFR = productionRequest.getString("inputPathFR");
                String[] mainInputFilesFR = getInputPaths(getFileSystemService(), productionRequest.getUserName(),
                                                          inputPathFR,
                                                          mainRangeFR.getStartDate(), mainRangeFR.getStopDate(), null);
                if (mainInputFilesFR.length == 0) {
                    String date1Str = ProductionRequest.getDateFormat().format(mainRangeFR.getStartDate());
                    String date2Str = ProductionRequest.getDateFormat().format(mainRangeFR.getStopDate());
                    throw new ProductionException(
                            String.format("No input products found for given time range in RR. [%s - %s]", date1Str,
                                          date2Str));
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
                setRequestParameters(productionRequest, jobConfigSr);
                jobConfigSr.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, StringUtils.join(allInputs, ","));
                jobConfigSr.set(JobConfigNames.CALVALUS_OUTPUT_DIR, mainOutputDir);
                jobConfigSr.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, mosaicConfigXml);
                jobConfigSr.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
                jobConfigSr.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
                jobConfigSr.setBoolean("calvalus.system.snap.pixelGeoCoding.useTiling", true);
                jobConfigSr.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
                jobConfigSr.set("mapred.job.priority", "NORMAL");
                sequence.add(new MosaicWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                    productionName + " SR " + rr, jobConfigSr));
            }

            if (productionRequest.getBoolean("lcl3.nc", true) && !successfullyCompleted(ncOutputDir)) {
                String outputPrefix = String.format("CCI-LC-MERIS-SR-L3-%s-v4.0--%s--rrdays%s", groundResultion, period,
                                                    rr);
                Configuration jobConfigFormat = createJobConfig(productionRequest);
                setRequestParameters(productionRequest, jobConfigFormat);
                jobConfigFormat.set(JobConfigNames.CALVALUS_INPUT_DIR, mainOutputDir);
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_DIR, ncOutputDir);
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_NAMEFORMAT, outputPrefix + "-v%02dh%02d");
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4");
                jobConfigFormat.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "");
                jobConfigFormat.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, mosaicConfigXml);
                jobConfigFormat.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
                jobConfigFormat.set("mapred.job.priority", "HIGH");
                sequence.add(new MosaicFormattingWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                              productionName + " Format " + rr, jobConfigFormat));
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

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging currently not implemented for lc_cci Level3.");
    }

    static DateRange getDateRangeRR(ProductionRequest productionRequest, int rrDays) throws ProductionException {
        if (rrDays == 0) {
            return null;
        }
        Date minDate = productionRequest.getDate("minDate");
        Calendar calendar = DateUtils.createCalendar();
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
        Calendar calendar = DateUtils.createCalendar();
        calendar.setTimeInMillis(minDate.getTime());
        calendar.add(Calendar.DAY_OF_MONTH, rrDays);

        Calendar calendar2 = DateUtils.createCalendar();
        calendar2.setTimeInMillis(minDate.getTime());
        calendar2.add(Calendar.DAY_OF_MONTH, periodLength - 1);

        return new DateRange(calendar.getTime(), calendar2.getTime());
    }

    static MosaicConfig getMosaicConfig() throws ProductionException {
        String type = LCMosaicAlgorithm.class.getName();
        String maskExpr = "(status == 1 or status == 3) and not nan(sdr_1)";
        String[] varNames = new String[]{
                "status",
                "sdr_1", "sdr_2", "sdr_3", "sdr_4", "sdr_5",
                "sdr_6", "sdr_7", "sdr_8", "sdr_9", "sdr_10",
                "sdr_11", "sdr_12", "sdr_13", "sdr_14", "sdr_15",
                "ndvi",
                "sdr_error_1", "sdr_error_2", "sdr_error_3", "sdr_error_4", "sdr_error_5",
                "sdr_error_6", "sdr_error_7", "sdr_error_8", "sdr_error_9", "sdr_error_10",
                "sdr_error_11", "sdr_error_12", "sdr_error_13", "sdr_error_14", "sdr_error_15",
        };
        return new MosaicConfig(type, maskExpr, varNames);
    }

    static String[] getInputPaths(FileSystemService fileSystemService, String username, String inputPathPattern, Date minDate,
                                  Date maxDate, String regionName) throws ProductionException {
        InputPathResolver inputPathResolver = new InputPathResolver();
        inputPathResolver.setMinDate(minDate);
        inputPathResolver.setMaxDate(maxDate);
        inputPathResolver.setRegionName(regionName);
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPattern);
        try {
            return fileSystemService.globPaths(username, inputPatterns);
        } catch (IOException e) {
            throw new ProductionException("Failed to compute input file list.", e);
        }
    }
}
