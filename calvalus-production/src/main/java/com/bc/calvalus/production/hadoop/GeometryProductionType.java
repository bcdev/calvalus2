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
import com.bc.calvalus.processing.analysis.GeometryWorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.DateRange;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

import java.util.List;

/**
 * A production type used for creating worldmap-images with product boundaries.
 *
 * @author MarcoZ
 */
public class GeometryProductionType extends HadoopProductionType {

    static final String NAME = "Geometry";

    public GeometryProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = L2ProductionType.createProductionName("WorldMap ", productionRequest);
        final String productionName = productionRequest.getProdcutionName(defaultProductionName);

        String inputPath = productionRequest.getString("inputPath");
        List<DateRange> dateRanges = L3ProductionType.getDateRanges(productionRequest, 1);

        Workflow workflow = new Workflow.Parallel();
        for (int i = 0; i < dateRanges.size(); i++) {
            DateRange dateRange = dateRanges.get(i);
            String[] inputFiles = getInputPaths(getInventoryService(), inputPath, dateRange.getStartDate(), dateRange.getStopDate(), null);
            if (inputFiles.length > 0) {
                String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
                String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());

                String outputDir = getOutputPath(productionRequest, productionId, "worldmap-" + (i + 1));
                Configuration jobConfig = createJobConfig(productionRequest);
                jobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));
                jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
                jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
                jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);

                workflow.add(new GeometryWorkflowItem(getProcessingService(), productionName + " " + date1Str, jobConfig));
            }
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
        throw new NotImplementedException("Staging currently not implemented for quick look generation.");
    }
}
