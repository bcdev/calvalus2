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

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.vc.VCWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.util.StringUtils;

import java.io.IOException;

/**
 * Vicarious Calibration: A production type used for supporting the computation of vicarious calibration coefficients
 *
 * @author MarcoZ
 */
public class VCProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new VCProductionType(fileSystemService, processing, staging);
        }
    }

    VCProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                     StagingService stagingService) {
        super("VC", fileSystemService, processingService, stagingService);
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

        ///////////////////////////////////////////////////////////////////////////////////////////
        Configuration jobConfig = createJobConfig(productionRequest);

        setInputLocationParameters(productionRequest, jobConfig);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, regionName);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dataRanges);

        String outputDir = getOutputPath(productionRequest, productionId, "");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, geometryWKT);

        MAConfig maConfig = MAProductionType.getMAConfig(productionRequest);
        jobConfig.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());

        String processorBundleLocation = productionRequest.getString(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, null);
        String processorBundleName;
        String processorBundleVersion;
        StringBuilder processorBundles = new StringBuilder();
        if (processorBundleLocation != null) {
            processorBundles.append(processorBundleLocation);
        } else {
            processorBundleName = productionRequest.getParameter(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, true);
            processorBundleVersion = productionRequest.getParameter(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, true);
            if (processorBundleName != null && processorBundleVersion != null) {
                processorBundles.append(processorBundleName).append("-").append(processorBundleVersion);
            }
        }
        processorBundleLocation = productionRequest.getString(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION + VCWorkflowItem.DIFFERENTIATION_SUFFIX, null);
        if (processorBundleLocation != null) {
            ensureComma(processorBundles);
            processorBundles.append(processorBundleLocation);
        } else {
            processorBundleName = productionRequest.getParameter(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME + VCWorkflowItem.DIFFERENTIATION_SUFFIX, true);
            processorBundleVersion = productionRequest.getParameter(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION + VCWorkflowItem.DIFFERENTIATION_SUFFIX, true);
            if (processorBundleName != null && processorBundleVersion != null) {
                ensureComma(processorBundles);
                processorBundles.append(processorBundleName).append("-").append(processorBundleVersion);
            }
        }

        jobConfig.set(JobConfigNames.CALVALUS_BUNDLES, processorBundles.toString());

        ///////////////////////////////////////////////////////////////////////////////////////////
        ProcessorProductionRequest pprDifferentiation = new ProcessorProductionRequest(productionRequest,
                                                                                       VCWorkflowItem.DIFFERENTIATION_SUFFIX);
        setDefaultProcessorParameters(pprDifferentiation, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        pprDifferentiation.configureProcessor(jobConfig, VCWorkflowItem.DIFFERENTIATION_SUFFIX);

        ProcessorProductionRequest pprL2 = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(pprL2, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        pprL2.configureProcessor(jobConfig);


        WorkflowItem workflow = new VCWorkflowItem(getProcessingService(), productionRequest.getUserName(), productionName, jobConfig);

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

    private static void ensureComma(StringBuilder processorBundles) {
        if (processorBundles.length() > 0) {
            processorBundles.append(",");
        }
    }
}
