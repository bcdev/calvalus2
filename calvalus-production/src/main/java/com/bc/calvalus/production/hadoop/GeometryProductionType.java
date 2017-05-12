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
import com.bc.calvalus.processing.analysis.GeometryWorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * A production type used for creating worldmap-images with product boundaries.
 *
 * @author MarcoZ
 */
public class GeometryProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new GeometryProductionType(fileSystemService, processing, staging);
        }
    }

    GeometryProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                                  StagingService stagingService) {
        super("Geometry", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Geometries ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        List<DateRange> dateRanges = L3ProductionType.getDateRanges(productionRequest, "1");

        Workflow workflow = new Workflow.Parallel();
        for (int i = 0; i < dateRanges.size(); i++) {
            DateRange dateRange = dateRanges.get(i);

            Configuration jobConfig = createJobConfig(productionRequest);
            setRequestParameters(productionRequest, jobConfig);

            setInputLocationParameters(productionRequest, jobConfig);
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dateRange.toString());

            String outputDir = getOutputPath(productionRequest, productionId, "geometries-" + (i + 1));
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
            jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);

            workflow.add(new GeometryWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                  productionName + " " + date1Str, jobConfig));
        }

        // todo - if autoStaging=true, create sequential workflow and add staging job
        String stagingDir = productionRequest.getStagingDirectory(productionId);
        //boolean autoStaging = productionRequest.isAutoStaging(); //TODO
        boolean autoStaging = false;
        return new Production(productionId,
                              productionName,
                              "",
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflow);
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging currently not implemented for quick look generation.");
    }
}
