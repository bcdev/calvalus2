package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.MAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

import java.util.List;

/**
 * Trend analysis: A production type used for generating a time-series generated from L3 products and a numb er of
 * given regions.
 *
 * @author MarcoZ
 * @author Norman
 */
public class MAProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new MAProductionType(inventory, processing, staging);
        }
    }

    MAProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                            StagingService stagingService) {
        super("MA", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Match-up extraction ", productionRequest);
        final String productionName = productionRequest.getProdcutionName(defaultProductionName);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);

        Configuration maJobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, maJobConfig);
        setRequestParameters(productionRequest, maJobConfig);
        processorProductionRequest.configureProcessor(maJobConfig);

        String outputDir = getOutputPath(productionRequest, productionId, "");
        String maParametersXml = getMAConfigXml(productionRequest);

        List<DateRange> dateRanges = productionRequest.getDateRanges();
        maJobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
        maJobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        maJobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        maJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        maJobConfig.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maParametersXml);
        maJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                        regionGeometry != null ? regionGeometry.toString() : "");
        MAWorkflowItem workflowItem = new MAWorkflowItem(getProcessingService(), productionName, maJobConfig);

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              outputDir,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new CopyStaging(production,
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
        l3Config.setOutputTimeFormat(productionRequest.getString("outputTimeFormat", "yyyy-MM-dd HH:mm:ss"));
        l3Config.setRecordSourceUrl(productionRequest.getString("recordSourceUrl"));
        l3Config.setRecordSourceSpiClassName(productionRequest.getString("recordSourceSpiClassName", null));
        return l3Config;
    }

}
