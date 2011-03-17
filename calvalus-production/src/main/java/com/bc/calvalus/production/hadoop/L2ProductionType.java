package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.beam.StreamingProductReader;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.L2WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * A production type used for generating one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2ProductionType implements ProductionType {
    private final static DateFormat dateFormat = ProductData.UTC.createDateFormat("yyyy-MM-dd");
    private final HadoopProcessingService processingService;
    private final StagingService stagingService;

    L2ProductionType(HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        this.processingService = processingService;
        this.stagingService = stagingService;
    }

    @Override
    public String getName() {
        return "calvalus-level2";
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createL2ProductionName(productionRequest);
        final String userName = productionRequest.getUserName();

        L2WorkflowItem workflowItem = createWorkflowItem(productionId, productionRequest);

        // todo - if autoStaging=true, create sequential workflow and add staging job
        boolean autoStaging = isAutoStaging(productionRequest);

        return new Production(productionId,
                              productionName,
                              userName + "/" + productionId,
                              productionRequest,
                              workflowItem);
    }

    @Override
    public Staging createStaging(final Production production) throws ProductionException {

        final String jobOutputDir = getOutputDir(production.getId(), production.getProductionRequest());
        Staging staging = new L2Staging(production, jobOutputDir);
        try {
            stagingService.submitStaging(staging);
        } catch (IOException e) {
            throw new ProductionException(String.format("Failed to order staging for production '%s': %s",
                                                        production.getId(), e.getMessage()), e);
        }
        return staging;
    }

    @Override
    public boolean accepts(ProductionRequest productionRequest) {
        return getName().equalsIgnoreCase(productionRequest.getProductionType());
    }

    public static String createL2ProductionName(ProductionRequest productionRequest) {
        return String.format("Level 2 production using product set '%s' and L2 processor '%s'",
                             productionRequest.getProductionParameter("inputProductSetId"),
                             productionRequest.getProductionParameter("processorName"));

    }

    public L2WorkflowItem createWorkflowItem(String productionId,
                                             ProductionRequest productionRequest) throws ProductionException {
        Map<String, String> productionParameters = productionRequest.getProductionParameters();
        productionRequest.ensureProductionParameterSet("processorBundleName");
        productionRequest.ensureProductionParameterSet("processorBundleVersion");
        productionRequest.ensureProductionParameterSet("processorName");
        productionRequest.ensureProductionParameterSet("processorParameters");
        productionRequest.ensureProductionParameterSet("dateStart");
        productionRequest.ensureProductionParameterSet("dateStop");

        String inputProductSetId = productionRequest.getProductionParameterSafe("inputProductSetId");
        Date startDate = getDate(productionRequest, "dateStart");
        Date stopDate = getDate(productionRequest, "dateStop");
        String bBox = getBBox(productionRequest);
        // todo - use bbox to filter input files
        String[] inputFiles = getInputFiles(inputProductSetId, startDate, stopDate);
        String outputDir = getOutputDir(productionId, productionRequest);

        return new L2WorkflowItem(processingService,
                                  productionId,
                                  inputFiles,
                                  outputDir,
                                  productionParameters.get("processorBundleName"),
                                  productionParameters.get("processorBundleVersion"),
                                  productionParameters.get("processorName"),
                                  productionParameters.get("processorParameters"));
    }

    public String getOutputDir(String productionId, ProductionRequest productionRequest) {
        return processingService.getDataOutputPath() + "/" + productionRequest.getUserName() + "/" + productionId;
    }

    public String[] getInputFiles(String inputProductSetId, Date startDate, Date stopDate) throws ProductionException {
        String eoDataPath = processingService.getDataInputPath();
        List<String> dayPathList = getDayPathList(startDate, stopDate, inputProductSetId);
        try {
            List<String> inputFileList = new ArrayList<String>();
            for (String dayPath : dayPathList) {
                String[] strings = processingService.listFilePaths(eoDataPath + "/" + dayPath);
                inputFileList.addAll(Arrays.asList(strings));
            }
            return inputFileList.toArray(new String[inputFileList.size()]);
        } catch (IOException e) {
            throw new ProductionException("Failed to compute input file list.", e);
        }
    }

    public static List<String> getDayPathList(Date start, Date stop, String prefix) {
        Calendar startCal = ProductData.UTC.createCalendar();
        Calendar stopCal = ProductData.UTC.createCalendar();
        startCal.setTime(start);
        stopCal.setTime(stop);
        List<String> list = new ArrayList<String>();
        do {
            String dateString = String.format("MER_RR__1P/r03/%1$tY/%1$tm/%1$td", startCal);
            if (dateString.startsWith(prefix)) {
                list.add(dateString);
            }
            startCal.add(Calendar.DAY_OF_WEEK, 1);
        } while (!startCal.after(stopCal));

        return list;
    }

    public boolean isAutoStaging(ProductionRequest request) throws ProductionException {
        return getBoolean(request, "autoStaging", false);
    }

    // todo - move to ProductionRequest
    public String getBBox(ProductionRequest request) throws ProductionException {
        return String.format("%s,%s,%s,%s",
                             request.getProductionParameterSafe("lonMin"),
                             request.getProductionParameterSafe("latMin"),
                             request.getProductionParameterSafe("lonMax"),
                             request.getProductionParameterSafe("latMax"));
    }


    // todo - move to ProductionRequest
    public boolean getBoolean(ProductionRequest request, String name, Boolean def) {
        String text = request.getProductionParameter(name);
        if (text != null) {
            return Boolean.parseBoolean(text);
        } else {
            return def;
        }
    }

    // todo - move to ProductionRequest
    public Double getDouble(ProductionRequest request, String name, Double def) {
        String text = request.getProductionParameter(name);
        if (text != null) {
            return Double.parseDouble(text);
        } else {
            return def;
        }
    }

    // todo - move to ProductionRequest
    public Integer getInteger(ProductionRequest request, String name, Integer def) {
        String text = request.getProductionParameter(name);
        if (text != null) {
            return Integer.parseInt(text);
        } else {
            return def;
        }
    }

    // todo - move to ProductionRequest
    public Date getDate(ProductionRequest productionRequest, String name) throws ProductionException {
        try {
            return dateFormat.parse(productionRequest.getProductionParameterSafe(name));
        } catch (ParseException e) {
            throw new ProductionException("Illegal date format for production parameter '" + name + "'");
        }
    }

    public static DateFormat getDateFormat() {
        return dateFormat;
    }

    private class L2Staging extends Staging {

        private final Production production;
        private final String jobOutputDir;

        public L2Staging(Production production, String jobOutputDir) {
            this.production = production;
            this.jobOutputDir = jobOutputDir;
        }

        @Override
        public String call() throws Exception {
            float progress = 0f;
            production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
            try {
                JobClient jobClient = processingService.getJobClient();
                Path outputPath = new Path(jobOutputDir);
                FileSystem fileSystem = outputPath.getFileSystem(jobClient.getConf());
                FileStatus[] seqFiles = fileSystem.listStatus(outputPath, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        return path.getName().endsWith(".seq");
                    }
                });
                File downloadDir = new File(stagingService.getStagingDir(), production.getStagingPath());
                if (!downloadDir.exists()) {
                    downloadDir.mkdirs();
                }
                int index = 0;
                for (FileStatus seqFile : seqFiles) {
                    Path seqProductPath = seqFile.getPath();
                    StreamingProductReader reader = new StreamingProductReader(seqProductPath, jobClient.getConf());
                    Product product = reader.readProductNodes(null, null);
                    String dimapProductName = seqProductPath.getName().replaceFirst(".seq", ".dim");
                    File productFile = new File(downloadDir, dimapProductName);
                    ProductIO.writeProduct(product, productFile, ProductIO.DEFAULT_FORMAT_NAME, false);
                    index++;
                    progress = (index + 1) / seqFiles.length;
                    production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
                }
                production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""));
                // todo - zip or tar.gz all output DIMAPs to outputPath.getName() + ".zip" and remove outputPath.getName()
            } catch (Exception e) {
                production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, production.getStagingStatus().getProgress(), e.getMessage()));
                throw new ProductionException("Error: " + e.getMessage(), e);
            }
            return null;
        }
    }
}
