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
import com.bc.calvalus.processing.mosaic.MosaicSeasonalWorkflowItem;
import com.bc.calvalus.processing.mosaic.landcover.LCSeasonMosaicAlgorithm;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;

/**
 * A production type used for generating one or more lc_cci Level-3 products.
 *
 * @author MarcoZ
 */
public class LcSeasonalProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new LcSeasonalProductionType(fileSystemService, processing, staging);
        }
    }

    LcSeasonalProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                                    StagingService stagingService) {
        super("LCL3Seasonal", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = LcL3ProductionType.createLcProductionName("Level 3 LC Seasonal",
                                                                                 productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        DateRange mainRange = productionRequest.createFromMinMax();

        String mosaicConfigXml = getMosaicConfig().toXml();

        String period = LcL3ProductionType.getLcPeriodName(productionRequest);
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
        if (!successfullyCompleted(mainOutputDir)) {
            Configuration jobConfigSr = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, jobConfigSr);

            setInputLocationParameters(productionRequest, jobConfigSr);
            jobConfigSr.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            jobConfigSr.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, mainRange.toString());

            jobConfigSr.set(JobConfigNames.CALVALUS_OUTPUT_DIR, mainOutputDir);
            jobConfigSr.set(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS, mosaicConfigXml);
            jobConfigSr.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometryString);
            jobConfigSr.setIfUnset("calvalus.mosaic.tileSize", Integer.toString(mosaicTileSize));
            jobConfigSr.set("mapred.job.priority", "NORMAL");
            sequence.add(new MosaicSeasonalWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                        productionName + " SR", jobConfigSr));
        }
        if (!successfullyCompleted(ncOutputDir)) {
            String outputPrefix = String.format("CCI-LC-MERIS-SR-L3-%s-v4.0--%s", groundResultion, period);
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
                                                          productionName + " Format",
                                                          jobConfigFormat));
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

    static MosaicConfig getMosaicConfig() throws ProductionException {
        return new MosaicConfig(LCSeasonMosaicAlgorithm.class.getName(), "", new String[0]);
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging currently not implemented for lc_cci Level3.");
    }

}
