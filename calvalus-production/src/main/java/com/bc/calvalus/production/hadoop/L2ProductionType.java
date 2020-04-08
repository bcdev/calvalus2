package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
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
import java.util.Date;
import java.util.List;

/**
 * A production type used for generating one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2ProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new L2ProductionType(fileSystemService, processing, staging);
        }
    }

    L2ProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                     StagingService stagingService) {
        super("L2", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = productionRequest.getProductionName(
                createProductionName("Level 2 ", productionRequest));

        L2WorkflowItem workflowItem = createWorkflowItem(productionId, productionName, productionRequest);

        // todo - if autoStaging=true, create sequential workflow and add staging job
        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              workflowItem.getOutputDir(),
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        throw new UnsupportedOperationException("Staging disabled for L2 use L2Plus instead.");
//        return new L2Staging(production,
//                             getProcessingService().getJobClient(production.getProductionRequest().getUsername()).getConf(),
//                             getStagingService().getStagingDir());
    }

    L2WorkflowItem createWorkflowItem(String productionId,
                                      String productionName,
                                      ProductionRequest productionRequest) throws ProductionException {

        List<DateRange> dateRanges = productionRequest.getDateRanges();

        String outputDir = getOutputPath(productionRequest, productionId, "");

        Configuration l2JobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, l2JobConfig);
        setRequestParameters(productionRequest, l2JobConfig);
        processorProductionRequest.configureProcessor(l2JobConfig);

        Geometry geometry = productionRequest.getRegionGeometry(null);
        setInputLocationParameters(productionRequest, l2JobConfig);
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        l2JobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, geometry != null ? geometry.toString() : "");

        Date startDate = dateRanges.get(0).getStartDate();
        Date stopDate = dateRanges.get(dateRanges.size() - 1).getStopDate();
        String regionWKT = geometry != null ? geometry.toString() : null;
        ProcessorDescriptor processorDesc = processorProductionRequest.getProcessorDescriptor(getProcessingService());
        String pathPattern = outputDir + "/" + getPathPatternForProcessingResult(processorDesc);
        String[] bandNames = getResultingBandNames(processorDesc);
        String resultingProductionType = getResultingProductionType(processorDesc);
        ProductSet productSet = new ProductSet(resultingProductionType,
                                               productionName, pathPattern, startDate, stopDate,
                                               productionRequest.getRegionName(), regionWKT, bandNames);

        L2WorkflowItem l2WorkflowItem = new L2WorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                           productionName, l2JobConfig);
        l2WorkflowItem.addWorkflowStatusListener(new ProductSetSaver(l2WorkflowItem, productSet, outputDir));
        return l2WorkflowItem;
    }

    static String getResultingProductionType(ProcessorDescriptor processorDescriptor) {
        String productType = null;
        if (processorDescriptor != null) {
            productType = processorDescriptor.getOutputProductType();
        }
        if (productType == null || productType.isEmpty()) {
            productType = "L2_PRODUCT";
        }
        return productType;
    }

    static String[] getResultingBandNames(ProcessorDescriptor processorDescriptor) {
        if (processorDescriptor == null || processorDescriptor.getOutputVariables() == null) {
            return new String[0];
        }
        ProcessorDescriptor.Variable[] outputVariables = processorDescriptor.getOutputVariables();
        String[] bandNames = new String[outputVariables.length];
        for (int i = 0; i < outputVariables.length; i++) {
            bandNames[i] = outputVariables[i].getName();
        }
        return bandNames;
    }

    static String getPathPatternForProcessingResult(ProcessorDescriptor processorDescriptor) {
        String datePattern = "[^_\\.].*(?:${yyyy}${MM}${dd}|${yyyy}${DDD}).*";
        if (processorDescriptor == null) {
            return datePattern + "\\.seq$";
        }
        ProcessorDescriptor.FormattingType formatting = processorDescriptor.getFormatting();
        String outputRegex = processorDescriptor.getOutputRegex();
        if (! outputRegex.isEmpty()) {
            return outputRegex;
        } else  if (formatting == ProcessorDescriptor.FormattingType.IMPLICIT) {
            // MEGS, l2gen, polymer, fmask (regex from processor, if given)
            return datePattern;
        } else {
            // BEAM processor
            return datePattern + "\\.seq$";
        }
    }

}
