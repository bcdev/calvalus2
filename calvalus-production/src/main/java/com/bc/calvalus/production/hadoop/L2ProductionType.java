package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.WorkflowStatusEvent;
import com.bc.calvalus.commons.WorkflowStatusListener;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.inventory.ProductSetPersistable;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.production.DateRange;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * A production type used for generating one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2ProductionType extends HadoopProductionType {

    static final String NAME = "L2";

    public L2ProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = productionRequest.getProdcutionName(createL2ProductionName(productionRequest));

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
    protected Staging createUnsubmittedStaging(Production production) {
        return new L2Staging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    static String createL2ProductionName(ProductionRequest productionRequest) throws ProductionException {
        return String.format("Level 2 production using input path '%s' and L2 processor '%s'",
                             productionRequest.getString("inputPath"),
                             productionRequest.getString("processorName"));
    }

    L2WorkflowItem createWorkflowItem(String productionId,
                                      String productionName, ProductionRequest productionRequest) throws ProductionException {

        List<DateRange> dateRanges = getDateRanges(productionRequest);
        String inputPath = productionRequest.getString("inputPath");
        String regionName = productionRequest.getRegionName();
        String[] inputFiles = getInputFiles(getInventoryService(), inputPath, regionName, dateRanges);

        String outputDir = getOutputPath(productionRequest, productionId, "");

        String processorName = productionRequest.getString("processorName");
        String processorParameters = productionRequest.getString("processorParameters", "<parameters/>");
        String processorBundleName = productionRequest.getString("processorBundleName");
        String processorBundleVersion = productionRequest.getString("processorBundleVersion");
        String processorBundle = String.format("%s-%s", processorBundleName, processorBundleVersion);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);

        Configuration l2JobConfig = createJobConfig(productionRequest);
        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));
        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        l2JobConfig.set(JobConfigNames.CALVALUS_L2_BUNDLE, processorBundle);
        l2JobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, processorName);
        l2JobConfig.set(JobConfigNames.CALVALUS_L2_PARAMETERS, processorParameters);
        l2JobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry != null ? regionGeometry.toString() : "");

        Date startDate = dateRanges.get(0).getStartDate();
        Date stopDate = dateRanges.get(dateRanges.size() - 1).getStopDate();
        String pathPattern = outputDir + "/.*${yyyy}${MM}${dd}.*.seq$";
        String regionWKT = regionGeometry != null ? regionGeometry.toString() : null;
        String productType = getResultingProductionType(processorBundleName, processorBundleVersion, processorName);
        ProductSet productSet = new ProductSet(productType, productionName, pathPattern, startDate, stopDate, regionName, regionWKT);

        L2WorkflowItem l2WorkflowItem = new L2WorkflowItem(getProcessingService(), productionId, l2JobConfig);
        l2WorkflowItem.addWorkflowStatusListener(new ProductSetSaver(l2WorkflowItem, productSet, outputDir));
        return l2WorkflowItem;
    }

    String getResultingProductionType(String bundleName, String bundleVersion, String executableName) {
        try {
            BundleDescriptor[] bundles = getProcessingService().getBundles("");
            for (BundleDescriptor bundle : bundles) {
                if (bundle.getBundleName().equals(bundleName) && bundle.getBundleVersion().equals(bundleVersion)) {
                    ProcessorDescriptor[] processorDescriptors = bundle.getProcessorDescriptors();
                    for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                        if (processorDescriptor.getExecutableName().equals(executableName)) {
                            return processorDescriptor.getOutputProductType();
                        }
                    }
                }
            }
        } catch (IOException ignore) {
        }
        return "L2";
    }

    public static String[] getInputFiles(InventoryService inventoryService, ProductionRequest productionRequest) throws ProductionException {
        List<DateRange> dateRanges = getDateRanges(productionRequest);
        String inputPath = productionRequest.getString("inputPath");
        String regionName = productionRequest.getRegionName();
        return getInputFiles(inventoryService, inputPath, regionName, dateRanges);
    }

    private static String[] getInputFiles(InventoryService inventoryService, String inputPath, String regionName, List<DateRange> dateRanges) throws ProductionException {
        List<String> inputFileAccumulator = new ArrayList<String>();
        for (DateRange dateRange : dateRanges) {
            String[] inputPaths = getInputPaths(inventoryService, inputPath, dateRange.getStartDate(), dateRange.getStopDate(), regionName);
            inputFileAccumulator.addAll(Arrays.asList(inputPaths));
        }
        if (inputFileAccumulator.size() == 0) {
            throw new ProductionException("No input files found for given time range.");
        }
        return inputFileAccumulator.toArray(new String[inputFileAccumulator.size()]);
    }

    static List<DateRange> getDateRanges(ProductionRequest productionRequest) throws ProductionException {
        List<DateRange> dateRangeList = new ArrayList<DateRange>();
        Date[] dateList = productionRequest.getDates("dateList", null);
        if (dateList != null) {
            Arrays.sort(dateList);
            for (Date date : dateList) {
                dateRangeList.add(new DateRange(date, date));
            }
        } else {
            Date minDate = productionRequest.getDate("minDate", null);
            Date maxDate = productionRequest.getDate("maxDate", null);
            dateRangeList.add(new DateRange(minDate, maxDate));
        }
        return dateRangeList;
    }

    class ProductSetSaver implements WorkflowStatusListener {

        private final L2WorkflowItem l2WorkflowItem;
        private final ProductSet productSet;
        private final String outputDir;

        public ProductSetSaver(L2WorkflowItem l2WorkflowItem, ProductSet productSet, String outputDir) {
            this.l2WorkflowItem = l2WorkflowItem;
            this.productSet = productSet;
            this.outputDir = outputDir;
        }

        public ProductSet getProductSet() {
            return productSet;
        }

        @Override
        public void handleStatusChanged(WorkflowStatusEvent event) {
            if (event.getSource() == l2WorkflowItem && event.getNewStatus().getState() == ProcessState.COMPLETED) {
                String productSetDefinition = ProductSetPersistable.convertToCSV(productSet);
                writeProductSetFile(productSetDefinition);
            }
        }

        private void writeProductSetFile(String text) {
            Path productSetsFile = new Path(outputDir, ProductSetPersistable.FILENAME);
            OutputStreamWriter outputStreamWriter = null;
            try {
                FileSystem fileSystem = FileSystem.get(l2WorkflowItem.getJobConfig());
                OutputStream fsDataOutputStream = fileSystem.create(productSetsFile);
                outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
                outputStreamWriter.write(text);
            } catch (IOException e) {
                // TODO, mz 2011-11-07 log error
                e.printStackTrace();
            } finally {
                if (outputStreamWriter != null) {
                    try {
                        outputStreamWriter.close();
                    } catch (IOException ignore) {
                    }
                }
            }
        }
    }

}
