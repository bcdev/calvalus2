package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.MAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;

import java.util.Date;

/**
 * Trend analysis: A production type used for generating a time-series generated from L3 products and a numb er of
 * given regions.
 *
 * @author MarcoZ
 * @author Norman
 */
public class MAProductionType extends HadoopProductionType {


    public static final String NAME = "MA";

    public MAProductionType(InventoryService inventoryService, HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createTAProductionName(productionRequest);

        String inputPath = productionRequest.getParameter("inputPath");
        Date minDate = productionRequest.getDate("minDate", null);
        Date maxDate = productionRequest.getDate("maxDate", null);
        // todo - use geoRegion to filter input files (nf,20.04.2011)
        String[] l1InputFiles = getInputPaths(inputPath, minDate, maxDate);

        WorkflowItem workflowItem;
        if (l1InputFiles.length > 0) {
            String outputDir = getOutputDir(productionRequest.getUserName(), productionId);

            MAConfig maConfig = MAConfig.fromXml(productionRequest.getParameter("calvalus.ma.parameters"));
            workflowItem = new MAWorkflowItem(getProcessingService(),
                                              productionId,
                                              l1InputFiles,
                                              outputDir,
                                              maConfig);

        } else {
            throw new ProductionException("No input products found for given time range.");
        }
        String stagingDir = String.format("%s/%s", productionRequest.getUserName(), productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new MAStaging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    String getOutputDir(String userName, String dirName) {
        return getInventoryService().getDataOutputPath(String.format("%s/%s", userName, dirName));
    }

    static String createTAProductionName(ProductionRequest productionRequest) throws ProductionException {
        return String.format("Match-up extraction using input path '%s'",
                             productionRequest.getParameter("inputPath"));

    }
}
