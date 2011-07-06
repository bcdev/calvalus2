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

import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Abstract base class for production types that require a Hadoop processing system.
 *
 * @author MarcoZ
 * @author Norman
 */
public abstract class HadoopProductionType implements ProductionType {
    private final String name;
    private final HadoopProcessingService processingService;
    private final StagingService stagingService;

    protected HadoopProductionType(String name, HadoopProcessingService processingService, StagingService stagingService) {
        this.name = name;
        this.processingService = processingService;
        this.stagingService = stagingService;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean accepts(ProductionRequest productionRequest) {
        return getName().equalsIgnoreCase(productionRequest.getProductionType());
    }

    @Override
    public Staging createStaging(Production production) throws ProductionException {
        Staging staging = createUnsubmittedStaging(production);
        try {
            getStagingService().submitStaging(staging);
        } catch (IOException e) {
            throw new ProductionException(String.format("Failed to order staging for production '%s': %s",
                                                        production.getId(), e.getMessage()), e);
        }
        return staging;
    }

    protected abstract Staging createUnsubmittedStaging(Production production);

    public HadoopProcessingService getProcessingService() {
        return processingService;
    }

    public StagingService getStagingService() {
        return stagingService;
    }

    public String[] getInputFiles(String inputProductSetId, Date minDate, Date maxDate) throws ProductionException {
        List<String> globs = getPathGlobs(inputProductSetId, minDate, maxDate);
        String dataInputPath = processingService.getDataInputPath();
        try {
            List<String> inputFileList = new ArrayList<String>();
            for (String glob : globs) {
                String[] files = processingService.globFilePaths(dataInputPath + "/" + glob);
                inputFileList.addAll(Arrays.asList(files));
            }
            return inputFileList.toArray(new String[inputFileList.size()]);
        } catch (IOException e) {
            throw new ProductionException("Failed to compute input file list.", e);
        }
    }

    static List<String> getPathGlobs(String productSetId, Date minDate, Date maxDate) {
        if (productSetId.endsWith("/")) {
            productSetId = productSetId.substring(0, productSetId.length() - 1);
        }
        if (!productSetId.contains("/")) {
            productSetId = productSetId + "/*";
        }
        List<String> globs = new ArrayList<String>();
        if (minDate != null && maxDate != null) {
            Calendar startCal = ProductData.UTC.createCalendar();
            Calendar stopCal = ProductData.UTC.createCalendar();
            startCal.setTime(minDate);
            stopCal.setTime(maxDate);
            do {
                globs.add(String.format("%1$s/%2$tY/%2$tm/%2$td/*.N1", productSetId, startCal));
                startCal.add(Calendar.DAY_OF_WEEK, 1);
            } while (!startCal.after(stopCal));
        } else {
            globs.add(String.format("%1$s/*/*/*/*.N1", productSetId));
        }
        return globs;
    }
}
