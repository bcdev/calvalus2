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
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.mosaic.MosaicConfig;
import com.bc.calvalus.processing.mosaic.MosaicFormattingWorkflowItem;
import com.bc.calvalus.processing.mosaic.MosaicWorkflowItem;
import com.bc.calvalus.processing.mosaic.globveg.GlobVegMosaicAlgorithm;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;

/**
 * A production type used for generating one or more GLobVeg Level-3 products.
 *
 * @author MarcoZ
 */
public class GLobVegProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new GLobVegProductionType(fileSystemService, processing, staging);
        }
    }

    GLobVegProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                          StagingService stagingService) {
        super("GLobVeg", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createGlobVegProductionName("GLobVeg L3 ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        DateRange dateRange = productionRequest.createFromMinMax();

        String processorName = productionRequest.getString(ProcessorProductionRequest.PROCESSOR_NAME, null);
        String processorBundle = productionRequest.getString("processorBundle", null);
        String processorBundleLocation = productionRequest.getString(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, null);
        String processorParameters = productionRequest.getString(ProcessorProductionRequest.PROCESSOR_PARAMETERS, "<parameters/>");

        String mosaicConfigXml = getMosaicConfig().toXml();

        String period = getGlobVegPeriodName(productionRequest);
        String partsOutputDir = getOutputPath(productionRequest, productionId, period + "-parts");
        String ncOutputDir = getOutputPath(productionRequest, productionId, period + "-nc");

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String regionGeometryString = regionGeometry != null ? regionGeometry.toString() : "";
        Workflow.Sequential sequence = new Workflow.Sequential();

        if (!successfullyCompleted(partsOutputDir)) {
            Configuration jobConfig = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, jobConfig);

            setInputLocationParameters(productionRequest, jobConfig);
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dateRange.toString());

            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, partsOutputDir);

            if (processorName != null) {
                if (processorBundleLocation != null) {
                    jobConfig.set(JobConfigNames.CALVALUS_BUNDLES, processorBundleLocation);
                } else if (processorBundle != null) {
                    jobConfig.set(JobConfigNames.CALVALUS_BUNDLES, processorBundle);
                }
                jobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, processorName);
                jobConfig.set(JobConfigNames.CALVALUS_L2_PARAMETERS, processorParameters);
            }

            jobConfig.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, mosaicConfigXml);
            jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfig.setInt("calvalus.mosaic.macroTileSize", 10);
            jobConfig.setInt("calvalus.mosaic.tileSize", 360);
            jobConfig.setBoolean("calvalus.system.snap.pixelGeoCoding.useTiling", true);
            jobConfig.setBoolean("calvalus.system.beam.pixelGeoCoding.useTiling", true);
            jobConfig.set("mapred.job.priority", "NORMAL");
            sequence.add(new MosaicWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                productionName + " L3", jobConfig));
        }
        if (!successfullyCompleted(ncOutputDir)) {
            String outputNameFormat = "meris-globveg-" + period + "-v%02dh%02d-1.0";
            Configuration jobConfig = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, jobConfig);
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, partsOutputDir);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, ncOutputDir);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_NAMEFORMAT, outputNameFormat);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4");
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "");
            jobConfig.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, mosaicConfigXml);
            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
            jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);
            jobConfig.setInt("calvalus.mosaic.macroTileSize", 10);
            jobConfig.setInt("calvalus.mosaic.tileSize", 360);
            jobConfig.set("mapred.job.priority", "HIGH");
            sequence.add(
                    new MosaicFormattingWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                     productionName + " Format", jobConfig));
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

    private static String createGlobVegProductionName(String prefix, ProductionRequest productionRequest) throws
                                                                                                          ProductionException {
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(getGlobVegPeriodName(productionRequest));
        return sb.toString().trim();
    }

    private static String getGlobVegPeriodName(ProductionRequest productionRequest) throws ProductionException {
        return productionRequest.getString("minDate").replaceAll("-", "");
    }

    private static MosaicConfig getMosaicConfig() throws ProductionException {
        String type = GlobVegMosaicAlgorithm.class.getName();
        String maskExpr = "valid_fapar == 1 || valid_lai == 1";
        String[] varNames = new String[]{"valid_fapar", "valid_lai", "obs_time", "fapar", "lai", "ndvi_kg"};
        return new MosaicConfig(type, maskExpr, varNames);
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging currently not implemented for old Level3.");
    }

}
