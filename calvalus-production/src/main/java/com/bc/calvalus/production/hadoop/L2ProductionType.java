package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.WorkflowStatusEvent;
import com.bc.calvalus.commons.WorkflowStatusListener;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.inventory.ProductSetPersistable;
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
import java.text.DateFormat;
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

    public L2ProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                            StagingService stagingService) {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = productionRequest.getProdcutionName(createProductionName("Level 2 ", productionRequest));

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

    static String createProductionName(String prefix, ProductionRequest productionRequest) throws ProductionException {
        StringBuilder sb = new StringBuilder(prefix);
        String processorName = productionRequest.getString("processorName", null);
        if (processorName != null) {
            sb.append(processorName).append(" ");
        }
        List<DateRange> dateRanges = getDateRanges(productionRequest);
        if (dateRanges.size() > 0 && dateRanges.get(0).getStartDate() != null && dateRanges.get(0).getStopDate() != null) {
            DateFormat dateFormat = ProductionRequest.getDateFormat();
            String start = dateFormat.format(dateRanges.get(0).getStartDate());
            String stop = dateFormat.format(dateRanges.get(dateRanges.size() - 1).getStopDate());
            sb.append(start).append(" to ").append(stop).append(" ");
        }
        String regionName = productionRequest.getRegionName();
        if (regionName != null) {
            sb.append("(").append(regionName).append(") ");
        }
        return sb.toString().trim();
    }

    L2WorkflowItem createWorkflowItem(String productionId,
                                      String productionName,
                                      ProductionRequest productionRequest) throws ProductionException {

        productionRequest.ensureParameterSet("processorName");
        List<DateRange> dateRanges = getDateRanges(productionRequest);
        String inputPath = productionRequest.getString("inputPath");
        String regionName = productionRequest.getRegionName();
        String[] inputFiles = getInputFiles(getInventoryService(), inputPath, regionName, dateRanges);
        Geometry regionGeometry = productionRequest.getRegionGeometry(null);

        String outputDir = getOutputPath(productionRequest, productionId, "");

        Configuration l2JobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(l2JobConfig, processorProductionRequest);
        setRequestParameters(l2JobConfig, productionRequest);

        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));
        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        l2JobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                        regionGeometry != null ? regionGeometry.toString() : "");

        Date startDate = dateRanges.get(0).getStartDate();
        Date stopDate = dateRanges.get(dateRanges.size() - 1).getStopDate();
        String pathPattern = outputDir + "/.*${yyyy}${MM}${dd}.*.seq$";
        String regionWKT = regionGeometry != null ? regionGeometry.toString() : null;
        ProcessorDescriptor processorDescriptor = processorProductionRequest.getProcessorDescriptor(getProcessingService());
        ProductSet productSet = new ProductSet(getResultingProductionType(processorDescriptor),
                                               productionName, pathPattern, startDate, stopDate,
                                               regionName, regionWKT);

        L2WorkflowItem l2WorkflowItem = new L2WorkflowItem(getProcessingService(), productionName, l2JobConfig);
        l2WorkflowItem.addWorkflowStatusListener(new ProductSetSaver(l2WorkflowItem, productSet, outputDir));
        return l2WorkflowItem;
    }

    String getResultingProductionType(ProcessorDescriptor processorDescriptor) {
        if (processorDescriptor != null) {
            return processorDescriptor.getOutputProductType();
        }
        return "L2";
    }

    public static String[] getInputFiles(InventoryService inventoryService, ProductionRequest productionRequest) throws
            ProductionException {
        List<DateRange> dateRanges = getDateRanges(productionRequest);
        String inputPath = productionRequest.getString("inputPath");
        String regionName = productionRequest.getRegionName();
        return getInputFiles(inventoryService, inputPath, regionName, dateRanges);
    }

    private static String[] getInputFiles(InventoryService inventoryService, String inputPath, String regionName,
                                          List<DateRange> dateRanges) throws ProductionException {
        List<String> inputFileAccumulator = new ArrayList<String>();
        for (DateRange dateRange : dateRanges) {
            String[] inputPaths = getInputPaths(inventoryService, inputPath, dateRange.getStartDate(),
                                                dateRange.getStopDate(), regionName);
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
