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
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.geodb.GeodbScanWorkflowItem;
import com.bc.calvalus.processing.geodb.GeodbUpdateWorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.util.StringUtils;

import java.util.List;

/**
 * A production type used for creating / updating the geo-inventory.
 *
 * @author MarcoZ
 */
public class GeoDbProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new GeoDbProductionType(fileSystemService, processing, staging);
        }
    }

    GeoDbProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                        StagingService stagingService) {
        super("GeoDB", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String action = productionRequest.getString("action");
        String defaultProductionName = createProductionName("GeoDB " + action, productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration jobConfig = createJobConfig(productionRequest);
        setRequestParameters(productionRequest, jobConfig);

        jobConfig.set(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY, productionRequest.getString("geoInventory"));

        WorkflowItem workflowItem;
        if (action.equalsIgnoreCase("scan")) {
            List<DateRange> dateRanges = productionRequest.getDateRanges();
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

            workflowItem = new GeodbScanWorkflowItem(getProcessingService(),
                                                     productionRequest.getUserName(),
                                                     productionName,
                                                     jobConfig);
        } else if (action.equalsIgnoreCase("update")) {
            workflowItem = new GeodbUpdateWorkflowItem(getProcessingService(),
                                                       productionRequest.getUserName(),
                                                       productionName,
                                                       jobConfig);
        } else {
            throw new ProductionException("unsupported action:" + action);
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = false;
        return new Production(productionId,
                              productionName,
                              "",
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    // no staging required
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging not required for geo DB productions.");
    }
}
