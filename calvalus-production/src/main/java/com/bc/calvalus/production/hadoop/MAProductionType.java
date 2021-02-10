package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.inventory.FileSystemService;
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
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Match-up analysis: A production type used for generating match-ups between Level1/Level2 products and in-situ data.
 *
 * @author MarcoZ
 * @author Norman
 */
public class MAProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new MAProductionType(fileSystemService, processing, staging);
        }
    }

    MAProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                     StagingService stagingService) {
        super("MA", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Match-up extraction ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);

        Configuration maJobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, maJobConfig);
        setRequestParameters(productionRequest, maJobConfig);
        processorProductionRequest.configureProcessor(maJobConfig);

        String outputDir = getOutputPath(productionRequest, productionId, "");
        String maParametersXml = getMAConfigXml(productionRequest);

        List<DateRange> dateRanges = productionRequest.getDateRanges();
        setInputLocationParameters(productionRequest, maJobConfig);
        maJobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        maJobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        maJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        maJobConfig.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maParametersXml);
        maJobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                        regionGeometry != null ? regionGeometry.toString() : "");
        MAWorkflowItem workflowItem = new MAWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                         productionName, maJobConfig);

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
        MAConfig maConfig = new MAConfig();
        maConfig.setMacroPixelSize(productionRequest.getInteger("macroPixelSize", 1));
        maConfig.setFilteredMeanCoeff(productionRequest.getDouble("filteredMeanCoeff", 1.5));
        maConfig.setFilterOverlapping(productionRequest.getBoolean("filterOverlapping", false));
        maConfig.setCopyInput(productionRequest.getBoolean("copyInput", true));
        maConfig.setGoodPixelExpression(productionRequest.getXmlDecodedString("goodPixelExpression", ""));
        maConfig.setGoodRecordExpression(productionRequest.getXmlDecodedString("goodRecordExpression", ""));
        String maxTimeDifference = productionRequest.getString("maxTimeDifference", "");
        if (!MAConfig.isMaxTimeDifferenceValid(maxTimeDifference)) {
            throw new ProductionException("Production parameter 'maxTimeDifference' must be hours (number > 0) or " +
                                                  "days (with 'd' at the and number >= 0). " +
                                                  "Use '0' or empty value to disable time test.");             
        }
        maConfig.setMaxTimeDifference(maxTimeDifference);
        maConfig.setOutputGroupName(productionRequest.getString("outputGroupName", "SITE"));
        maConfig.setOutputTimeFormat(productionRequest.getString("outputTimeFormat", "yyyy-MM-dd HH:mm:ss"));
        maConfig.setRecordSourceUrl(productionRequest.getString("recordSourceUrl"));
        maConfig.setRecordSourceSpiClassName(productionRequest.getString("recordSourceSpiClassName", null));
        maConfig.setVariableMappings(parseVariableMappings(productionRequest.getString("variableMappings", null)));
        maConfig.setOnlyExtractComplete(productionRequest.getBoolean("onlyExtractComplete", true));
        return maConfig;
    }

    static MAConfig.VariableMapping[] parseVariableMappings(String variableMappingsString) {
        if ( StringUtils.isNullOrEmpty(variableMappingsString)) {
            return null;
        }
        String[] mappings = variableMappingsString.split(",");
        List<MAConfig.VariableMapping> mappingList = new ArrayList<>(mappings.length);
        for (String mapping : mappings) {
            String[] refSat = mapping.split("=");
            if (refSat.length == 2) {
                String ref = refSat[0].trim();
                String sat = refSat[1].trim();
                if (!ref.isEmpty() && !sat.isEmpty()) {
                    mappingList.add(new MAConfig.VariableMapping(ref, sat));
                }
            }
        }
        if (mappingList.isEmpty()) {
            return null;
        }
        return mappingList.toArray(new MAConfig.VariableMapping[mappingList.size()]);
    }

}
