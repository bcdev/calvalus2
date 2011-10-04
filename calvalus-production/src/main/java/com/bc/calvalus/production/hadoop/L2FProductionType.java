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

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.l2.L2FormattingWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * A production type used for formatting one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2FProductionType extends HadoopProductionType {

    static final String NAME = "L2F";

    public L2FProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createProductionName(productionRequest);
        final String userName = productionRequest.getUserName();

        HadoopWorkflowItem workflowItem = createWorkflowItem(productionId, productionRequest);

        // todo - if autoStaging=true, create sequential workflow and add staging job
        String stagingDir = userName + "/" + productionId;
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              workflowItem.getOutputDir(),
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

   // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for product validation.");
    }

    static String createProductionName(ProductionRequest productionRequest) throws ProductionException {
        return String.format("Level 2 formatting using input path '%s' and output format'%s'",
                             productionRequest.getString("inputPath"),
                             productionRequest.getString("outputFormat", "NetCDF"));
    }

    HadoopWorkflowItem createWorkflowItem(String productionId,
                                      ProductionRequest productionRequest) throws ProductionException {

        String inputPath = productionRequest.getString("inputPath");
        String regionName = productionRequest.getRegionName();

        Date[] dateList = productionRequest.getDates("dateList", null);
        String[] inputFiles;
        if (dateList != null) {
            List<String> inputFileAccumulator = new ArrayList<String>();
            for (Date date : dateList) {
                inputFileAccumulator.addAll(Arrays.asList(getInputPaths(inputPath, date, date, regionName)));
            }
            inputFiles = inputFileAccumulator.toArray(new String[inputFileAccumulator.size()]);
        } else {
            Date minDate = productionRequest.getDate("minDate", null);
            Date maxDate = productionRequest.getDate("maxDate", null);
            inputFiles = getInputPaths(inputPath, minDate, maxDate, regionName);
        }
        if (inputFiles.length == 0) {
            throw new ProductionException("No input files found for given time range.");
        }
        System.out.println("inputFiles = " + Arrays.toString(inputFiles));

        String outputDir = getOutputPath(productionRequest, productionId, "");
        String outputFormat = productionRequest.getString("outputFormat", "NetCDF");

        Configuration l2JobConfig = createJobConfig(productionRequest);
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));

        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

        return new L2FormattingWorkflowItem(getProcessingService(), productionId, l2JobConfig);
    }

}
