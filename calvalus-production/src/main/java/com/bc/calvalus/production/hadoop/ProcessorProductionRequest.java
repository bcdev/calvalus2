/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionRequest;
import org.apache.hadoop.conf.Configuration;

/**
 * The part of a production request dealing with the requested processor.
 */
public class ProcessorProductionRequest {

    private final String processorName;
    private final String processorParameters;
    private final String processorBundleName;
    private final String processorBundleVersion;

    public ProcessorProductionRequest(ProductionRequest productionRequest) {
        this.processorName = productionRequest.getString("processorName", null);
        this.processorParameters = productionRequest.getString("processorParameters", "<parameters/>");
        this.processorBundleName = productionRequest.getString("processorBundleName", null);
        this.processorBundleVersion = productionRequest.getString("processorBundleVersion", null);
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getProcessorBundle() {
        if (processorBundleName != null && processorBundleVersion != null) {
            return String.format("%s-%s", processorBundleName, processorBundleVersion);
        }
        return null;
    }

    public void configureProcessor(Configuration jobConfig) {
        if (processorName != null) {
            jobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, processorName);
            jobConfig.set(JobConfigNames.CALVALUS_L2_PARAMETERS, processorParameters);
        }
        String processorBundle = getProcessorBundle();
        if (processorBundle != null) {
            jobConfig.set(JobConfigNames.CALVALUS_L2_BUNDLE, processorBundle);
        }
    }

    public ProcessorDescriptor getProcessorDescriptor(ProcessingService processingService) {
        try {
            BundleDescriptor[] bundles = processingService.getBundles("");
            for (BundleDescriptor bundle : bundles) {
                if (bundle.getBundleName().equals(processorBundleName) && bundle.getBundleVersion().equals(
                        processorBundleVersion)) {
                    ProcessorDescriptor[] processorDescriptors = bundle.getProcessorDescriptors();
                    for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                        if (processorDescriptor.getExecutableName().equals(processorName)) {
                            return processorDescriptor;
                        }
                    }
                }
            }
        } catch (Exception ignore) {
        }
        return null;
    }

}
