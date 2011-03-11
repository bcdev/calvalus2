/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class L2ProcessingRequestFactory extends ProcessingRequestFactory {

    L2ProcessingRequestFactory(ProcessingService processingService) {
        super(processingService);
    }

    @Override
    public ProcessingRequest[] createProcessingRequests(String productionId, String userName, ProductionRequest productionRequest) throws ProductionException {
        Map<String, String> productionParameters = productionRequest.getProductionParameters();
        productionRequest.ensureProductionParameterSet("l2ProcessorBundleName");
        productionRequest.ensureProductionParameterSet("l2ProcessorBundleVersion");
        productionRequest.ensureProductionParameterSet("l2ProcessorName");
        productionRequest.ensureProductionParameterSet("l2ProcessorParameters");
        productionRequest.ensureProductionParameterSet("dateStart");
        productionRequest.ensureProductionParameterSet("dateStop");

        HashMap<String, Object> processingParameters = new HashMap<String, Object>(productionParameters);
        processingParameters.put("productionId", productionId);
//        commonProcessingParameters.put("bbox", getBBox(productionRequest)); //TODO
        processingParameters.put("autoStaging", isAutoStaging(productionRequest));

        Date startDate = getDate(productionRequest, "dateStart");
        Date stopDate = getDate(productionRequest, "dateStop");
        String inputProductSetId = productionRequest.getProductionParameterSafe("inputProductSetId");
        processingParameters.put("inputFiles", getInputFiles(inputProductSetId, startDate, stopDate));

        String jobDir = getProcessingService().getDataOutputPath() + "/" + userName + "/" + productionId;
        processingParameters.put("outputDir", jobDir);

        return new L3ProcessingRequest[]{new L3ProcessingRequest(processingParameters)};
    }

}
