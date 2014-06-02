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
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.util.List;

/**
 * Vicarious Calibration: A production type used for supporting the computation of vicarious calibration coefficients
 *
 * @author MarcoZ
 */
public class VCProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new VCProductionType(inventory, processing, staging);
        }
    }

    VCProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                     StagingService stagingService) {
        super("VC", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Vicarious Calibration ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        String geometryWKT = regionGeometry != null ? regionGeometry.toString() : "";
        String regionName = productionRequest.getRegionName();
        String dataRanges = StringUtils.join(productionRequest.getDateRanges(), ",");

        String level1Input = productionRequest.getString("inputPath");

        ///////////////////////////////////////////////////////////////////////////////////////////
        Configuration configL1 = createJobConfig(productionRequest);

        configL1.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, level1Input);
        configL1.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, regionName);
        configL1.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dataRanges);

        String outputDirL1 = getOutputPath(productionRequest, productionId, "-L1");
        configL1.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDirL1);
        configL1.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, geometryWKT);

        MAConfig maConfig = MAProductionType.getMAConfig(productionRequest);
        maConfig.setGoodPixelExpression("");
        configL1.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());

        ///////////////////////////////////////////////////////////////////////////////////////////
        Configuration configL1Perturbation = createJobConfig(productionRequest);

        ProcessorProductionRequest pprPerturbation = new ProcessorProductionRequest(productionRequest, "perturbation.");
        setDefaultProcessorParameters(pprPerturbation, configL1Perturbation);
        setRequestParameters(productionRequest, configL1Perturbation);
        pprPerturbation.configureProcessor(configL1Perturbation);

        configL1Perturbation.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, level1Input);
        configL1Perturbation.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, regionName);
        configL1Perturbation.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dataRanges);

        String outputDirL1Perturbation = getOutputPath(productionRequest, productionId, "-L1Perturbation");
        configL1Perturbation.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDirL1Perturbation);
        configL1Perturbation.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, geometryWKT);

        maConfig = MAProductionType.getMAConfig(productionRequest);
        maConfig.setCopyInput(false);
        maConfig.setSaveProcessedProducts(true);
        maConfig.setGoodPixelExpression("");
        configL1Perturbation.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());

        ///////////////////////////////////////////////////////////////////////////////////////////
        Configuration configL2 = createJobConfig(productionRequest);

        ProcessorProductionRequest pprL2 = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(pprL2, configL2);
        setRequestParameters(productionRequest, configL2);
        pprL2.configureProcessor(configL2);

        int lastSlashIndex = level1Input.lastIndexOf("/");
        String level2Input;
        if (lastSlashIndex == -1) {
            level2Input = outputDirL1Perturbation;
        } else {
            level2Input = outputDirL1Perturbation + "/" + level1Input.substring(lastSlashIndex - 1);
        }
        configL2.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, level2Input);

        String outputDirL2 = getOutputPath(productionRequest, productionId, "-L2");
        configL2.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDirL2);
        configL2.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, geometryWKT);

        maConfig = MAProductionType.getMAConfig(productionRequest);
        maConfig.setCopyInput(false);
        maConfig.setGoodPixelExpression("");
        configL2.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());

        ///////////////////////////////////////////////////////////////////////////////////////////
        WorkflowItem workflowL1 = new MAWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                     productionName + " L1", configL1);
        WorkflowItem workflowL1Perturbation = new MAWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                                 productionName + " L1-Perturbation", configL1Perturbation);
        WorkflowItem workflowL2 = new MAWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                     productionName + " L2", configL2);

        WorkflowItem sequential = new Workflow.Sequential(workflowL1Perturbation, workflowL2);
        WorkflowItem workflow = new Workflow.Parallel(workflowL1, sequential);
        /*TODO add CSV merging*/

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              outputDirL2,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflow);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        return new CopyStaging(production,
                               getProcessingService().getJobClient(production.getProductionRequest().getUserName()).getConf(),
                               getStagingService().getStagingDir());
    }
}
