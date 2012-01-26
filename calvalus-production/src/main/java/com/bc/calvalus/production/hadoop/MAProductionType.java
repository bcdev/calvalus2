package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.MAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

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
        String defaultProductionName = L2ProductionType.createProductionName("Match-up extraction ", productionRequest);
        final String productionName = productionRequest.getProdcutionName(defaultProductionName);


        String[] l1InputFiles = L2ProductionType.getInputFiles(getInventoryService(), productionRequest);
        String inputFormat = productionRequest.getString("calvalus.input.format", "ENVISAT");
        Geometry regionGeometry = productionRequest.getRegionGeometry(null);

        String processorName = productionRequest.getString("processorName", null);
        String processorParameters = null;
        String processorBundle = null;
        if (processorName != null) {
            processorParameters = productionRequest.getString("processorParameters", "<parameters/>");
            processorBundle = String.format("%s-%s",
                                            productionRequest.getString("processorBundleName"),
                                            productionRequest.getString("processorBundleVersion"));
        }

        String outputDir = getOutputPath(productionRequest, productionId, "");
        String maParametersXml = getMAConfigXml(productionRequest);

        Configuration maJobConfig = createJobConfig(productionRequest);
        maJobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(l1InputFiles, ","));
        maJobConfig.set(JobConfigNames.CALVALUS_INPUT_FORMAT, inputFormat);
        maJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        if (processorName != null) {
            maJobConfig.set(JobConfigNames.CALVALUS_L2_BUNDLE, processorBundle);
            maJobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, processorName);
            maJobConfig.set(JobConfigNames.CALVALUS_L2_PARAMETERS, processorParameters);
        }
        maJobConfig.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maParametersXml);
        maJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry != null ? regionGeometry.toString() : "");
        MAWorkflowItem workflowItem = new MAWorkflowItem(getProcessingService(), productionName, maJobConfig);

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
        return new MAStaging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    static String getMAConfigXml(ProductionRequest productionRequest) throws ProductionException {
        String maParametersXml = productionRequest.getString("calvalus.ma.parameters", null);
        if (maParametersXml == null) {
            MAConfig maConfig = getMAConfig(productionRequest);
            maParametersXml = maConfig.toXml();
        } else {
            // Check MA XML before sending it to Hadoop
            try {
                MAConfig.fromXml(maParametersXml);
            } catch (BindingException e) {
                throw new ProductionException("Illegal match-up configuration: " + e.getMessage(), e);
            }
        }
        return maParametersXml;
    }

    static MAConfig getMAConfig(ProductionRequest productionRequest) throws ProductionException {
        MAConfig l3Config = new MAConfig();
        l3Config.setMacroPixelSize(productionRequest.getInteger("macroPixelSize", 1));
        l3Config.setFilteredMeanCoeff(productionRequest.getDouble("filteredMeanCoeff", 1.5));
        l3Config.setCopyInput(productionRequest.getBoolean("copyInput", true));
        l3Config.setGoodPixelExpression(productionRequest.getString("goodPixelExpression", ""));
        l3Config.setGoodRecordExpression(productionRequest.getString("goodRecordExpression", ""));
        l3Config.setMaxTimeDifference(productionRequest.getDouble("maxTimeDifference", 3.0));
        l3Config.setSortInputByPixelYX(productionRequest.getBoolean("sortInputByPixelYX", false));
        l3Config.setOutputGroupName(productionRequest.getString("outputGroupName", "SITE"));
        l3Config.setOutputTimeFormat(productionRequest.getString("outputTimeFormat", "yyyy-MM-dd hh:mm:ss"));
        l3Config.setRecordSourceUrl(productionRequest.getString("recordSourceUrl"));
        l3Config.setRecordSourceSpiClassName(productionRequest.getString("recordSourceSpiClassName", null));
        return l3Config;
    }

}
