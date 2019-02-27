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

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionStaging;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.production.ProductionTypeSpi;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Collections;
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
    private final FileSystemService fileSystemService;
    private final HadoopProcessingService processingService;
    private final StagingService stagingService;

    protected HadoopProductionType(String name,
                                   FileSystemService fileSystemService,
                                   HadoopProcessingService processingService,
                                   StagingService stagingService) {
        this.name = name;
        this.fileSystemService = fileSystemService;
        this.processingService = processingService;
        this.stagingService = stagingService;
    }

    protected static String createProductionName(String prefix, ProductionRequest productionRequest) throws
            ProductionException {
        StringBuilder sb = new StringBuilder(prefix);
        String processorName = productionRequest.getString(ProcessorProductionRequest.PROCESSOR_NAME, null);
        if (processorName != null) {
            sb.append(processorName).append(" ");
        }
        List<DateRange> dateRanges = productionRequest.getDateRanges();
        if (dateRanges.size() > 0 && dateRanges.get(0).getStartDate() != null && dateRanges.get(
                0).getStopDate() != null) {
            DateFormat dateFormat = ProductionRequest.getDateFormat();
            String start = dateFormat.format(dateRanges.get(0).getStartDate());
            String stop = dateFormat.format(dateRanges.get(dateRanges.size() - 1).getStopDate());
            sb.append(start).append(" to ").append(stop).append(" ");
        }
        String regionName = productionRequest.getRegionName();
        if (!regionName.isEmpty()) {
            sb.append("(").append(regionName).append(") ");
        }
        return sb.toString().trim();
    }

    public FileSystemService getFileSystemService() {
        return fileSystemService;
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
        try {
            Staging staging = createUnsubmittedStaging(production);
            ((ProductionStaging) staging).setProductionService((ProductionService)stagingService.getProductionService());
            getStagingService().submitStaging(staging);
            return staging;
        } catch (IOException e) {
            throw new ProductionException(String.format("Failed to order staging for production '%s': %s",
                                                        production.getId(), e.getMessage()), e);
        }
    }

    protected void setInputLocationParameters(ProductionRequest productionRequest, Configuration conf) throws ProductionException {
        Map<String, String> productionRequestParameters = productionRequest.getParameters();
        if(productionRequestParameters.containsKey("productIdentifiers")){
            conf.set(JobConfigNames.CALVALUS_INPUT_PRODUCT_IDENTIFIERS, productionRequest.getString("productIdentifiers"));
        }
        if(productionRequestParameters.containsKey("collectionName")){
            conf.set(JobConfigNames.CALVALUS_INPUT_COLLECTION_NAME, productionRequest.getString("collectionName"));
        }
        if(productionRequestParameters.containsKey("inputProductType")){
            conf.set(JobConfigNames.CALVALUS_INPUT_PRODUCT_TYPE, productionRequest.getString("inputProductType"));
        }
        if (productionRequestParameters.containsKey("inputTable")) {
            conf.set(JobConfigNames.CALVALUS_INPUT_TABLE, productionRequest.getString("inputTable"));
        } else if (productionRequestParameters.containsKey("geoInventory")) {
            conf.set(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY, productionRequest.getString("geoInventory"));
        } else if (productionRequestParameters.containsKey("inputPath")) {
            conf.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
        } else {
            throw new ProductionException("Missing request parameter: neither 'inputPath', 'inputTable' nor 'geoInventory' is given.");
        }
    }

    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        String userName = production.getProductionRequest().getUserName();
        Configuration conf = getProcessingService().getJobClient(userName).getConf();
        return new CopyStaging(production,
                               conf,
                               getProcessingService().getFileSystem(userName, conf, new Path(production.getOutputPath())),
                               getStagingService().getStagingDir());
    }

    protected final Configuration createJobConfig(ProductionRequest productionRequest) throws ProductionException {
        try {
            Configuration jobConfig = getProcessingService().createJobConfig(productionRequest.getUserName());
            // the following values have defaults in mapred-default.xml
            // we instead want to apply our settings in the mapred-site.xml or yarn-site.xml on the RM
            // user provided still values take precedence
            jobConfig.unset("mapreduce.map.memory.mb");
            jobConfig.unset("mapreduce.reduce.memory.mb");
            jobConfig.unset("mapreduce.jobhistory.address");
            jobConfig.unset("mapreduce.jobhistory.webapp.address");
            jobConfig.set(JobConfigNames.CALVALUS_USER, productionRequest.getUserName());
            jobConfig.set(JobConfigNames.CALVALUS_PRODUCTION_TYPE, productionRequest.getProductionType());
            return jobConfig;
        } catch (IOException e) {
            throw new ProductionException(e);
        }
    }

    protected final void setDefaultProcessorParameters(ProcessorProductionRequest processorProductionRequest,
                                                       Configuration jobConfig) {
        ProcessorDescriptor processorDescriptor = processorProductionRequest.getProcessorDescriptor(processingService);
        Map<String, String> map = Collections.emptyMap();
        if (processorDescriptor != null) {
            map = processorDescriptor.getJobConfiguration();
        }
        setJobConfig(map, jobConfig);
    }

    protected final void setRequestParameters(ProductionRequest productionRequest, Configuration jobConfig) {
        setJobConfig(productionRequest.getParameters(), jobConfig);
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
    protected String getOutputPath(ProductionRequest productionRequest, String productionId, String dirSuffix) throws ProductionException {
        String relDir = productionRequest.getString("outputDir", productionId);
        String defaultDir = String.format("home/%s/%s", productionRequest.getUserName(), relDir);
        String outputPath = productionRequest.getString("outputPath", defaultDir);
        String outputDir = productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_DIR, outputPath);
        if (outputDir.endsWith("/")) {
            outputDir = outputDir.substring(0, outputDir.length() - 1);
        }
        try {
            return getFileSystemService().getQualifiedPath(productionRequest.getUserName(), outputDir + dirSuffix);
        } catch (IOException e) {
            throw new ProductionException(e);
        }
    }

    /**
     * Test if Hadoop has placed a "_SUCCESS" file into the output directory,
     * after successfully completing a former job attempt.
     *
     * @param outputDir The output directory
     * @return true, if "_SUCCESS" exists
     */
    protected boolean successfullyCompleted(String outputDir) {
        try {
            return fileSystemService.pathExists(outputDir + "/_SUCCESS", null);
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
     * @param parameters The parameters.
     * @param jobConfig  A Hadoop job configuration.
     */
    public static void setJobConfig(Map<String, String> parameters, Configuration jobConfig) {
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String name = entry.getKey();
            if (name.startsWith("calvalus.hadoop.")) {
                String hadoopName = name.substring("calvalus.hadoop.".length());
                jobConfig.set(hadoopName, entry.getValue());
            } else if (name.startsWith("calvalus.")
                    && !name.startsWith("calvalus.portal")
                    && !name.startsWith("calvalus.queue")
                    && !name.startsWith("calvalus.crypt")) {
                jobConfig.set(name, entry.getValue());
            }
        }
    }

    public static abstract class Spi implements ProductionTypeSpi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, ProcessingService processing, StagingService staging) {
            return create(fileSystemService, (HadoopProcessingService) processing, staging);
        }

        abstract public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging);
    }
}
