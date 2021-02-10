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
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.MAWorkflowItem;
import com.bc.calvalus.processing.ma.compare.MACompareWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.BindingException;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Match-up comparison: A production type used for generating match-ups
 * between multiple Level1/Level2 products and in-situ data including comparison of them.
 *
 * @author MarcoZ
 */
public class MACompareProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new MACompareProductionType(fileSystemService, processing, staging);
        }
    }

    MACompareProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                            StagingService stagingService) {
        super("MAC", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Match-up comparison ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);

        String allIdentifierString = productionRequest.getString("allIdentifiers");
        String[] allIdentifiers = allIdentifierString.split(",");
        String outputDirMa = getOutputPath(productionRequest, productionId, "-ma");

        MAConfig maConfig = getMAConfig(productionRequest);
        maConfig.setCopyInput(false);

        Workflow maWorkflows = new Workflow.Parallel();
        List<String> macInputs = new ArrayList<String>();
        for (String identifier : allIdentifiers) {
            Configuration maJobConfig = createJobConfig(productionRequest);
            String suffix = "." + identifier;
            ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest, suffix);
            setDefaultProcessorParameters(processorProductionRequest, maJobConfig);
            setRequestParameters(productionRequest, maJobConfig);
            processorProductionRequest.configureProcessor(maJobConfig);

            maConfig.setGoodPixelExpression(productionRequest.getXmlDecodedString("goodPixelExpression" + suffix, ""));
            maConfig.setGoodRecordExpression(productionRequest.getXmlDecodedString("goodRecordExpression" + suffix, ""));

            List<DateRange> dateRanges = productionRequest.getDateRanges();
            setInputLocationParameters(productionRequest, maJobConfig);
            maJobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            maJobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

            String maDir = outputDirMa + "/" + identifier;
            maJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, maDir);
            macInputs.add(maDir + "/part-r-00000");
            maJobConfig.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());
            maJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                            regionGeometry != null ? regionGeometry.toString() : "");
            maWorkflows.add(new MAWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                               productionName + " " + identifier, maJobConfig));
        }
        String outputDir = getOutputPath(productionRequest, productionId, "");
        Configuration macJobConfig = createJobConfig(productionRequest);

        macJobConfig.setStrings(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, macInputs.toArray(new String[macInputs.size()]));
        macJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        macJobConfig.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());
        macJobConfig.set("calvalus.ma.identifiers", allIdentifierString);

        WorkflowItem maCompareWorkflow = new MACompareWorkflowItem(getProcessingService(), productionRequest.getUserName(), productionName + " compare", macJobConfig);
        Workflow.Sequential workflow = new Workflow.Sequential(maWorkflows, maCompareWorkflow);

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

    static MAConfig getMAConfig(ProductionRequest productionRequest) throws ProductionException {
        String maParametersXml = productionRequest.getString("calvalus.ma.parameters", null);
        if (maParametersXml == null) {
            return MAProductionType.getMAConfig(productionRequest);
        } else {
            // Check MA XML before sending it to Hadoop
            try {
                return MAConfig.fromXml(maParametersXml);
            } catch (BindingException e) {
                throw new ProductionException("Illegal match-up configuration: " + e.getMessage(), e);
            }
        }
    }
}
