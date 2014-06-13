/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.MAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.util.List;

/**
 * Match-up comparison: A production type used for generating match-ups
 *                      between multiple Level1/Level2 products and in-situ data including comparison of them.
 *
 * @author MarcoZ
 */
public class MACompareProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new MACompareProductionType(inventory, processing, staging);
        }
    }

    MACompareProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                            StagingService stagingService) {
        super("MAC", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Match-up comparison ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);

        String allIdentifierString = productionRequest.getString("allIdentifiers");
        String[] allIdentifiers = allIdentifierString.split(",");
        String outputDir = getOutputPath(productionRequest, productionId, "");

        Workflow workflow = new Workflow.Parallel();
        for (String identifier : allIdentifiers) {
            Configuration maJobConfig = createJobConfig(productionRequest);
            String suffix = "." + identifier;
            ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest, suffix);
            setDefaultProcessorParameters(processorProductionRequest, maJobConfig);
            setRequestParameters(productionRequest, maJobConfig);
            processorProductionRequest.configureProcessor(maJobConfig);

            String maParametersXml = getMAConfigXmlSpecial(productionRequest, suffix);

            List<DateRange> dateRanges = productionRequest.getDateRanges();
            maJobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
            maJobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            maJobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

            maJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir + "/" + identifier);
            maJobConfig.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maParametersXml);
            maJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                            regionGeometry != null ? regionGeometry.toString() : "");
            workflow.add(new MAWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                             productionName, maJobConfig));
        }
        // TODO match-up merging/comparison

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              outputDir,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflow);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        //TODO copy sub-directories, too
        return new CopyStaging(production,
                               getProcessingService().getJobClient(production.getProductionRequest().getUserName()).getConf(),
                               getStagingService().getStagingDir());
    }

    static String getMAConfigXmlSpecial(ProductionRequest productionRequest, String suffix) throws ProductionException {
        String maParametersXml = productionRequest.getString("calvalus.ma.parameters", null);
        if (maParametersXml == null) {
            MAConfig maConfig = MAProductionType.getMAConfig(productionRequest);
            maConfig.setGoodPixelExpression(productionRequest.getString("goodPixelExpression" + suffix, ""));
            maConfig.setGoodRecordExpression(productionRequest.getString("goodRecordExpression" + suffix, ""));
            maParametersXml = maConfig.toXml();
        } else {
            // Check MA XML before sending it to Hadoop
            try {
                MAConfig.fromXml(maParametersXml);
            } catch (BindingException e) {
                throw new ProductionException("Illegal match-up configuration: " + e.getMessage(), e);
            }
        }
        return maParametersXml;
    }
}
