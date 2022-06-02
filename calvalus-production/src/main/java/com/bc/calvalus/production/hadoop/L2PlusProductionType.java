/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.beam.S3aCompatibleOutputFormat;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.l2.L2FormattingWorkflowItem;
import com.bc.calvalus.processing.l2.L2Mapper;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.processing.l2.ProcessingMapper;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.StagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.util.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A production type used for generating one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2PlusProductionType extends HadoopProductionType {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing,
                                     StagingService staging) {
            return new L2PlusProductionType(fileSystemService, processing, staging);
        }
    }

    L2PlusProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                         StagingService stagingService) {
        super("L2Plus", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = productionRequest.getProductionName(createProductionName("Level 2 ", productionRequest));
        final String outputFormat = productionRequest.getString("outputFormat", productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null));
        final String outputBandList = productionRequest.getString("outputBandList", "");
        final List<DateRange> dateRanges = productionRequest.getDateRanges();
        final ProcessorProductionRequest processor = new ProcessorProductionRequest(productionRequest);
        final String processorName = processor.getProcessorName();
        final ProcessorDescriptor processorDescriptor = (processorName != null) ? processor.getProcessorDescriptor(getProcessingService()) : null;
        final boolean isXCubeRequested = "XCube".equals(outputFormat);
        final boolean isFormattingImplicit = processorDescriptor != null && processorDescriptor.getFormatting() == ProcessorDescriptor.FormattingType.IMPLICIT;
        final boolean isFormattingRequested = ! isXCubeRequested && ! "SEQ".equals(outputFormat);

        final Workflow.Sequential workflowSequence = new Workflow.Sequential();
        String workflowOutputDir = productionRequest.getString("inputPath", "");
        String formattingInputDir = workflowOutputDir;

        if (isXCubeRequested) {
            // set output url, create step and add it to sequence
            Configuration l2JobConfig = createJobConfig(productionRequest);
            setDefaultProcessorParameters(processor, l2JobConfig);
            String bucketUrl = l2JobConfig.get("calvalus.xcube.bucketurl");
            productionRequest.setParameter("outputPath", String.format("%s/cubes/%s", bucketUrl, productionRequest.getUserName()));
            HadoopWorkflowItem processingItem = createProcessingItem(productionId, productionName, dateRanges, productionRequest, processor,
                                                                     ProcessingMapper.class, bucketUrl.startsWith("s3") ? S3aCompatibleOutputFormat.class : SimpleOutputFormat.class);
            // use ProcessingMapper to include formatting
            processingItem.getJobConfig().set("mapreduce.job.map.class", ProcessingMapper.class.getName());
            processingItem.getJobConfig().set("outputFormat", "xcube-zarr");
            processingItem.getJobConfig().set("outputReplacement", productionName.replaceAll(" ", "_"));
            workflowSequence.add(processingItem);
            workflowOutputDir = processingItem.getOutputDir();
        } else if (! "Formatting".equals(processorName)) {
            // create step and add it to sequence
            HadoopWorkflowItem processingItem = createProcessingItem(productionId, productionName, dateRanges, productionRequest, processor);
            workflowSequence.add(processingItem);
            // memorise intermediate and maybe output
            workflowOutputDir = processingItem.getOutputDir();
            formattingInputDir = workflowOutputDir + "/" + L2ProductionType.getPathPatternForProcessingResult(processorDescriptor);
            // log
            if (processorName != null) {
                LOG.info("choosing pattern " + formattingInputDir + " for processor " + processorName + " descriptor " + processorDescriptor);
                if (processorDescriptor == null) {
                    LOG.info("looking for bundle " + processor.getProcessorBundle() + " location " + processor.getProcessorBundleLocation());
                }
            }
        }

        if (isFormattingRequested && !isFormattingImplicit && ! isXCubeRequested) {
            workflowOutputDir = getOutputPath(productionRequest, productionId, "-output");

            if (! "Multi-GeoTIFF".equals(outputFormat)) {
                // normal formatting of sequence files
                HadoopWorkflowItem formattingItem =
                        createFormattingItem(productionName + " Format", dateRanges,
                                             formattingInputDir, workflowOutputDir, productionRequest,
                                             processor, outputBandList,
                                             outputFormat);
                workflowSequence.add(formattingItem);
            } else {
                // parallel formatting of bands into GeoTIFF files
                if (outputBandList.isEmpty()) {
                    throw new ProductionException("If Multi-GeoTiff is specified as format also tailoring must be enabled and bands must be selected");
                }
                Workflow.Parallel formattingItems = new Workflow.Parallel();
                for (String bandName : StringUtils.csvToArray(outputBandList)) {
                    HadoopWorkflowItem formattingItem =
                            createFormattingItem(productionName + " Format: " + bandName,
                                                 dateRanges,
                                                 formattingInputDir, workflowOutputDir,
                                                 productionRequest,
                                                 processor, bandName, "GeoTIFF");
                    Configuration jobConfig = formattingItem.getJobConfig();
                    if ("FRESHMON".equalsIgnoreCase(jobConfig.get(JobConfigNames.CALVALUS_PROJECT_NAME))) {
                        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_REGEX, "L2_of_MER_..._1.....(........_......).*");
                        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_REPLACEMENT, String.format("%s_%s_BC_$1", bandName, productionRequest.getRegionName()));
                    }
                    formattingItems.add(formattingItem);
                }
                workflowSequence.add(formattingItems);
            }
        }

        if (workflowSequence.getItems().length == 0) {
            throw new ProductionException("Neither Processing nor Formatting selected.");
        }

        // todo - if autoStaging=true, create sequential workflow and add staging job
        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging() && ! isXCubeRequested;
        return new Production(productionId,
                              productionName,
                              workflowOutputDir,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowSequence);
    }


    HadoopWorkflowItem createFormattingItem(String productionName,
                                            List<DateRange> dateRanges,
                                            String formattingInputDir,
                                            String formattingOutputDir,
                                            ProductionRequest productionRequest,
                                            ProcessorProductionRequest processorProductionRequest, String bandList,
                                            String outputFormat) throws
                                                                 ProductionException {

        Configuration formatJobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, formatJobConfig);
        setRequestParameters(productionRequest, formatJobConfig);
        formatJobConfig.unset(JobConfigNames.CALVALUS_INPUT_FORMAT);  // the input format of the processing request

        String processorBundle = processorProductionRequest.getProcessorBundle();
        if (processorBundle != null) {
            String processorBundleLocation = processorProductionRequest.getProcessorBundleLocation();
            if (processorBundleLocation != null) {
                formatJobConfig.set(JobConfigNames.CALVALUS_BUNDLES, processorBundleLocation);
            } else {
                formatJobConfig.set(JobConfigNames.CALVALUS_BUNDLES, processorBundle);
            }
        }

        formatJobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, formattingInputDir);
        formatJobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        formatJobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, formattingOutputDir);
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);


        String outputCRS = productionRequest.getString("outputCRS", "");
        if (!outputCRS.isEmpty()) {
            formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_CRS, outputCRS);
            // only do a subset when reprojecting as well
            Geometry regionGeom = productionRequest.getRegionGeometry(null);
            formatJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                                regionGeom != null ? regionGeom.toString() : "");
        }
        if (productionRequest.getString("replaceNanValue", null) != null) {
            formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_REPLACE_NAN_VALUE,
                                String.valueOf(productionRequest.getDouble("replaceNanValue", 0.0)));
        }
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_BANDLIST, bandList);
        formatJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_QUICKLOOKS,
                            productionRequest.getString("quicklooks", "false"));

        return new L2FormattingWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                            productionName, formatJobConfig);
    }

    private HadoopWorkflowItem createProcessingItem(String productionId, String productionName,
                                                    List<DateRange> dateRanges, ProductionRequest productionRequest,
                                                    ProcessorProductionRequest processorProductionRequest) throws ProductionException {
        return createProcessingItem(productionId, productionName, dateRanges, productionRequest, processorProductionRequest,
                                    L2Mapper.class, SimpleOutputFormat.class);
    }

    private HadoopWorkflowItem createProcessingItem(String productionId, String productionName,
                                                    List<DateRange> dateRanges, ProductionRequest productionRequest,
                                                    ProcessorProductionRequest processorProductionRequest,
                                                    Class mapperClass, Class outputFormatClass) throws ProductionException {

        String outputDir = getOutputPath(productionRequest, productionId, "");

        Configuration l2JobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, l2JobConfig);
        setRequestParameters(productionRequest, l2JobConfig);
        processorProductionRequest.configureProcessor(l2JobConfig);

        setInputLocationParameters(productionRequest, l2JobConfig);
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        Geometry regionGeom = productionRequest.getRegionGeometry(null);
        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        l2JobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeom != null ? regionGeom.toString() : "");

        Date startDate = dateRanges.get(0).getStartDate();
        Date stopDate = dateRanges.get(dateRanges.size() - 1).getStopDate();
        String regionWKT = regionGeom != null ? regionGeom.toString() : null;
        ProcessorDescriptor processorDesc = processorProductionRequest.getProcessorDescriptor(getProcessingService());
        String pathPattern = outputDir + "/" + L2ProductionType.getPathPatternForProcessingResult(processorDesc);
        ProductSet productSet = new ProductSet(L2ProductionType.getResultingProductionType(processorDesc),
                                               productionName, pathPattern, startDate, stopDate,
                                               productionRequest.getRegionName(), regionWKT,
                                               L2ProductionType.getResultingBandNames(processorDesc));

        HadoopWorkflowItem l2Item = new L2WorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                       productionName, l2JobConfig, mapperClass, outputFormatClass);
        l2Item.addWorkflowStatusListener(new ProductSetSaver(l2Item, productSet, outputDir));
        return l2Item;
    }
}
