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

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for production types that require a Hadoop processing system.
 *
 * @author MarcoZ
 * @author Norman
 */
public abstract class HadoopProductionType implements ProductionType {
    private final String name;
    private final InventoryService inventoryService;
    private final HadoopProcessingService processingService;
    private final StagingService stagingService;

    protected HadoopProductionType(String name,
                                   InventoryService inventoryService,
                                   HadoopProcessingService processingService,
                                   StagingService stagingService) {
        this.name = name;
        this.inventoryService = inventoryService;
        this.processingService = processingService;
        this.stagingService = stagingService;
    }

    public InventoryService getInventoryService() {
        return inventoryService;
    }

    public HadoopProcessingService getProcessingService() {
        return processingService;
    }

    public StagingService getStagingService() {
        return stagingService;
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

    protected final Configuration createJobConfig(ProductionRequest productionRequest) {
        Configuration jobConfig = getProcessingService().createJobConfig();
        jobConfig.set(JobConfigNames.CALVALUS_USER, productionRequest.getUserName());
        jobConfig.set(JobConfigNames.CALVALUS_PRODUCTION_TYPE, productionRequest.getProductionType());
        return jobConfig;
    }

    protected final void setDefaultProcessorParameters(Configuration jobConfig, ProcessorProductionRequest processorProductionRequest) {
        ProcessorDescriptor processorDescriptor = processorProductionRequest.getProcessorDescriptor(processingService);
        Map<String, String> map = Collections.emptyMap();
        if (processorDescriptor != null) {
            map = processorDescriptor.getJobConfiguration();
        }
        processorProductionRequest.configure(jobConfig);
        setJobConfig(jobConfig, map);
    }

    protected final void setRequestParameters(Configuration jobConfig, ProductionRequest productionRequest) {
        setJobConfig(jobConfig, productionRequest.getParameters());
    }

    public static String[] getInputPaths(InventoryService inventoryService, String inputPathPattern, Date minDate, Date maxDate, String regionName) throws ProductionException {
        InputPathResolver inputPathResolver = new InputPathResolver();
        inputPathResolver.setMinDate(minDate);
        inputPathResolver.setMaxDate(maxDate);
        inputPathResolver.setRegionName(regionName);
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPattern);
        try {
            return inventoryService.globPaths(inputPatterns);
        } catch (IOException e) {
            throw new ProductionException("Failed to compute input file list.", e);
        }
    }

    /**
     * outputPath :=  if parameter "outputPath" set: "${outputPath}": else "${defaultDir}"<br/>
     * defaultDir := "home/${user}/${relDir}"<br/>
     * relDir := if parameter "outputDir" set: "${outputDir}" else:  "${productionId}"  <br/>
     *
     * @param productionRequest request
     * @param productionId      production ID
     * @param dirSuffix         suffix to make multiple outputs unique
     * @return the fully qualified output path
     */
    protected String getOutputPath(ProductionRequest productionRequest, String productionId, String dirSuffix) {
        String relDir = productionRequest.getString("outputDir", productionId);
        String defaultDir = String.format("home/%s/%s", productionRequest.getUserName(), relDir);
        String outputPath = productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_DIR,
                                                        productionRequest.getString("outputPath", defaultDir));
        return getInventoryService().getQualifiedPath(outputPath + dirSuffix);
    }

    /**
     * Test if Hadoop has placed a "_SUCCESS" file into the output directory,
     * after successfully completing a former job attempt.
     *
     * @param outputDir The output directory
     * @return true, if "_SUCCESS" exists
     */
    protected boolean successfullyCompleted(String outputDir) {
         ArrayList<String> globs = new ArrayList<String>();
         globs.add(outputDir + "/_SUCCESS");
         try {
             String[] pathes = inventoryService.globPaths(globs);
             return pathes.length == 1;
         } catch (IOException e) {
             return false;
         }
     }


    /**
     * Sets {@code jobConfig} values from the given {@code parameters} map.
     * <ol>
     * <li>
     * If a parameter's name is of the form
     * "calvalus.hadoop.&lt;name&gt;" than "&lt;name&gt;" will be set to the parameter value.
     * </li>
     * <li>If a
     * a parameter's name is of the form "calvalus.&lt;name&gt;" this name will be used.</li>
     * <li>If the name if of any other form, the parameter will be ignored.</li>
     * </ol>
     *
     * @param jobConfig  A Hadoop job configuration.
     * @param parameters The parameters.
     */
    public static void setJobConfig(Configuration jobConfig, Map<String, String> parameters) {
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String name = entry.getKey();
            if (name.startsWith("calvalus.hadoop.")) {
                String hadoopName = name.substring("calvalus.hadoop.".length());
                jobConfig.set(hadoopName, entry.getValue());
            } else if (name.startsWith("calvalus.")) {
                jobConfig.set(name, entry.getValue());
            }
        }
    }
}
