package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.analysis.QLWorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.production.util.DateRangeCalculator;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.Property;
import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.PropertyDescriptor;
import com.bc.ceres.binding.PropertySet;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.CompositingType;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.VariableConfig;
import org.esa.snap.binning.support.SEAGrid;

import java.beans.PropertyChangeListener;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * A production type used for generating one or more Level-3 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L3ProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new L3ProductionType(fileSystemService, processing, staging);
        }
    }

    static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

    L3ProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                     StagingService stagingService) {
        super("L3", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Level-3 ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        List<DateRange> dateRanges = getDateRanges(productionRequest, null);
        if (dateRanges.size() == 0) {
            throw new ProductionException("No time ranges specified.");
        }

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        Integer numReducers = productionRequest.getInteger(JobConfigNames.CALVALUS_L3_REDUCERS, null);
        boolean singleReducer = numReducers != null && numReducers == 1;

        String outputFormat = productionRequest.getString("outputFormat", productionRequest.getString(
                JobConfigNames.CALVALUS_OUTPUT_FORMAT, null));
        String outputDir = getOutputPath(productionRequest, productionId, "-L3-output");
        boolean requiresFormatting = outputFormat != null && !outputFormat.equalsIgnoreCase("SEQ");

        String l3ConfigXml = getL3ConfigXml(productionRequest);
        String[] l3OutputDirs = new String[dateRanges.size()];

        Workflow workflow = new Workflow.Parallel();
        workflow.setSustainable(false);
        for (int i = 0; i < dateRanges.size(); i++) {
            DateRange dateRange = dateRanges.get(i);

            String singleRangeOutputDir = getOutputPath(productionRequest, productionId, "-L3-" + (i + 1));
            l3OutputDirs[i] = singleRangeOutputDir;

            Configuration jobConfig = createJobConfig(productionRequest);
            setDefaultProcessorParameters(processorProductionRequest, jobConfig);
            setRequestParameters(productionRequest, jobConfig);
            processorProductionRequest.configureProcessor(jobConfig);

            setInputLocationParameters(productionRequest, jobConfig);
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, dateRange.toString());

            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, singleRangeOutputDir);

            // we have to replace here again since it is the default processor parameters that contains the bundle info
            jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS,
                          (jobConfig.get("calvalus.snap.bundle", "").contains("beam"))
                                  ? l3ConfigXml.replace("<planetaryGrid>org.esa.snap.binning.support.",
                                                        "<planetaryGrid>org.esa.beam.binning.support.")
                                  : l3ConfigXml);
            jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY,
                          regionGeometry != null ? regionGeometry.toString() : "");
            String date1Str = ProductionRequest.getDateFormat().format(dateRange.getStartDate());
            String date2Str = ProductionRequest.getDateFormat().format(dateRange.getStopDate());
            jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, date1Str);
            jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, date2Str);

            Integer superSampling = productionRequest.getInteger("superSampling", 1);
            if (superSampling > 1) {
                jobConfig.setBoolean("calvalus.system.snap.pixelGeoCoding.fractionAccuracy", true);
                jobConfig.setBoolean("calvalus.system.beam.pixelGeoCoding.fractionAccuracy", true);
            }

            if (singleReducer && requiresFormatting) {
                jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

                // is in fact dependent on the outputFormat TODO unify
                String outputCompression = productionRequest.getString("outputCompression", productionRequest.getString(
                        JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz"));
                jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, outputCompression);
            }
            WorkflowItem item = new L3WorkflowItem(getProcessingService(), productionRequest.getUserName(), productionName + " " + date1Str, jobConfig);
            workflow.add(item);
        }

        boolean hasQuicklookParameters = productionRequest.getString(JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS, null) != null;
        CalvalusLogger.getLogger().info("singleReducer=" + singleReducer + " requiresFormatting=" + requiresFormatting + " hasQuicklookParameters=" + hasQuicklookParameters);
        if (!singleReducer && requiresFormatting) {
            Configuration jobConfig = createJobConfig(productionRequest);
            setDefaultProcessorParameters(processorProductionRequest, jobConfig);
            setRequestParameters(productionRequest, jobConfig);
            jobConfig.unset(JobConfigNames.CALVALUS_INPUT_FORMAT);  // the input format of the processing request
            processorProductionRequest.configureProcessor(jobConfig);

            jobConfig.setStrings(JobConfigNames.CALVALUS_INPUT_DIR, l3OutputDirs);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

            // is in fact dependent on the outputFormat TODO unify
            String outputCompression = productionRequest.getString("outputCompression", productionRequest.getString(
                    JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz"));
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, outputCompression);

            WorkflowItem formatItem = new L3FormatWorkflowItem(getProcessingService(),
                                                               productionRequest.getUserName(),
                                                               productionName + " Format", jobConfig);
            workflow = new Workflow.Sequential(workflow, formatItem);

            if (hasQuicklookParameters) {
                CalvalusLogger.getLogger().info("creating ql step for reducers+formatting");
                Configuration qlJobConfig = createJobConfig(productionRequest);
                setDefaultProcessorParameters(processorProductionRequest, qlJobConfig);
                setRequestParameters(productionRequest, qlJobConfig);

                qlJobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, outputDir + "/(?!_.*|part-[rm]-\\d+).*");
                qlJobConfig.set(JobConfigNames.CALVALUS_INPUT_FORMAT, outputFormat);
                qlJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

                WorkflowItem qlItem = new QLWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                         productionName + " RGB", qlJobConfig);
                workflow.add(qlItem);
            }
        } else if (singleReducer && requiresFormatting && hasQuicklookParameters) {
            CalvalusLogger.getLogger().info("creating ql step for single reducer+formatting");
            Configuration qlJobConfig = createJobConfig(productionRequest);
            setDefaultProcessorParameters(processorProductionRequest, qlJobConfig);
            setRequestParameters(productionRequest, qlJobConfig);

            String[] qlInputDirs = new String[l3OutputDirs.length];
            for (int i = 0; i < qlInputDirs.length; i++) {
                qlInputDirs[i] = l3OutputDirs[i] + "/(?!_.*|part-[rm]-\\d+).*";
            }
            qlJobConfig.setStrings(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, qlInputDirs);
            qlJobConfig.set(JobConfigNames.CALVALUS_INPUT_FORMAT, outputFormat);
            qlJobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

            WorkflowItem qlItem = new QLWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                     productionName + " RGB", qlJobConfig);
            workflow = new Workflow.Sequential(workflow, qlItem);
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              outputDir,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflow);
    }

    /**
     * Generates a list of date ranges from min, may, period, and compositing period.
     * The method supports also monthly with the period -30 and weekly with the period -7.
     * Monthly means one range per calendar month.
     * Weekly splits the year into weeks starting with 1st January.
     * The week containing the 29th of Feb in leap years and the last week are prolonged to 8 days.
     *
     * @param productionRequest
     * @param periodLengthDefault
     * @return list of date ranges
     * @throws ProductionException
     */
    static List<DateRange> getDateRanges(ProductionRequest productionRequest, String periodLengthDefault)
            throws ProductionException {

        Date[] dateList = productionRequest.getDates("dateList", null);
        if (dateList != null) {
            return DateRangeCalculator.fromDateList(dateList);
        } else {
            Date minDate = productionRequest.getDate("minDate");
            Date maxDate = productionRequest.getDate("maxDate");
            String periodLength;
            if (periodLengthDefault != null) {
                periodLength = productionRequest.getString("periodLength", periodLengthDefault);
            } else {
                periodLength = productionRequest.getString("periodLength");
            }
            String compositingPeriodLength = productionRequest.getString("compositingPeriodLength", periodLength);
            return DateRangeCalculator.fromMinMax(minDate, maxDate, periodLength, compositingPeriodLength);
        }
    }


    public static String getL3ConfigXml(ProductionRequest productionRequest) throws ProductionException {
        String l3ConfigXml = productionRequest.getString(JobConfigNames.CALVALUS_L3_PARAMETERS, null);
        if (l3ConfigXml == null) {
            BinningConfig l3Config = getBinningConfig(productionRequest);
            l3ConfigXml = l3Config.toXml();
        } else {
            // Check L3 XML before sending it to Hadoop
            try {
                BinningConfig.fromXml(l3ConfigXml);
            } catch (Throwable ignore) {
                // if the aggregator is not on the classpath (because it is in a bundle)
                // Conversion is not possible. So only a warning:
                CalvalusLogger.getLogger().warning("Failed to de-serialize l3XML:" + ignore.getMessage());
            }
        }

        // todo - detect usable PlanetaryGrid-implementations, and use configured value
        CalvalusLogger.getLogger().info("Validating grid....");
        if (productionRequest.getParameter("processorBundles", false) != null && productionRequest.getString("processorBundles").contains("beam")) {
            l3ConfigXml = l3ConfigXml.replace("<planetaryGrid>org.esa.snap.binning.support.",
                                              "<planetaryGrid>org.esa.beam.binning.support.");
            CalvalusLogger.getLogger().warning(String.format("Falling back to '%s'", "org.esa.beam.binning.support.SEAGrid"));
        }

        return l3ConfigXml;
    }

    static BinningConfig getBinningConfig(ProductionRequest productionRequest) throws ProductionException {
        BinningConfig binningConfig = new BinningConfig();
        binningConfig.setCompositingType(CompositingType.valueOf(productionRequest.getString("compositingType", "BINNING")));
        if (productionRequest.getParameters().containsKey("planetaryGrid")) {
            binningConfig.setPlanetaryGrid(productionRequest.getString("planetaryGrid"));
        }
        binningConfig.setNumRows(getNumRows(productionRequest));
        binningConfig.setSuperSampling(productionRequest.getInteger("superSampling", 1));
        binningConfig.setMaskExpr(productionRequest.getXmlDecodedString("maskExpr", ""));
        binningConfig.setVariableConfigs(getVariables(productionRequest));
        binningConfig.setAggregatorConfigs(getAggregators(productionRequest));
        return binningConfig;
    }

    static AggregatorConfig[] getAggregators(ProductionRequest request) throws ProductionException {

        // TODO

        int variableCount = request.getInteger("variables.count");
        AggregatorConfig[] aggregatorConfigs = new AggregatorConfig[variableCount];
        for (int vIndex = 0; vIndex < variableCount; vIndex++) {
            String prefix = "variables." + vIndex;
            String aggregatorName = request.getString(prefix + ".aggregator");
            ParameterisedAggregatorConfig aggregatorConfig = new ParameterisedAggregatorConfig(aggregatorName);

            int parameterCount = request.getInteger(prefix + ".parameter.count");
            for (int pIndex = 0; pIndex < parameterCount; pIndex++) {
                String pName = request.getString(prefix + ".parameter." + pIndex + ".name");
                String pValue = request.getString(prefix + ".parameter." + pIndex + ".value");
                aggregatorConfig.addProperty(Property.create(pName, String.class));
                aggregatorConfig.setValue(pName, pValue);
            }
            aggregatorConfigs[vIndex] = aggregatorConfig;
        }
        return aggregatorConfigs;
    }

    static VariableConfig[] getVariables(ProductionRequest request) throws ProductionException {
        int expressionCount = request.getInteger("expression.count", 0);
        int vIndex = 0;
        VariableConfig[] variableConfigs = new VariableConfig[expressionCount];
        for (int i = 0; i < expressionCount; i++) {
            String prefix = "expression." + i;
            String name = request.getString(prefix + ".variable");
            String exp = request.getXmlDecodedString(prefix + ".expression", "");
            if (!exp.isEmpty()){
                variableConfigs[vIndex++] = new VariableConfig(name, exp);
            }
        }
        if (expressionCount == vIndex) {
            return variableConfigs;
        } else {
            return Arrays.copyOfRange(variableConfigs, 0, vIndex);
        }
    }

    static int getNumRows(ProductionRequest request) throws ProductionException {
        double resolution = request.getDouble("resolution");
        return SEAGrid.computeRowCount(resolution);
    }

    private static class ParameterisedAggregatorConfig extends AggregatorConfig implements PropertySet {
        private final PropertySet delegate = new PropertyContainer();

        public ParameterisedAggregatorConfig(String aggregatorName) {
            super(aggregatorName);
            delegate.addProperty(Property.create("type", String.class));
            delegate.setValue("type", aggregatorName);
        }

        @Override
        public Property[] getProperties() {
            return delegate.getProperties();
        }

        @Override
        public boolean isPropertyDefined(String name) {
            return delegate.isPropertyDefined(name);
        }

        @Override
        public Property getProperty(String name) {
            return delegate.getProperty(name);
        }

        @Override
        public void addProperty(Property property) {
            delegate.addProperty(property);
        }

        @Override
        public void addProperties(Property... properties) {
            delegate.addProperties(properties);
        }

        @Override
        public void removeProperty(Property property) {
            delegate.removeProperty(property);
        }

        @Override
        public void removeProperties(Property... properties) {
            delegate.removeProperties(properties);
        }

        @Override
        public <T> T getValue(String name) throws ClassCastException {
            return delegate.getValue(name);
        }

        @Override
        public void setValue(String name, Object value) throws IllegalArgumentException {
            delegate.setValue(name, value);
        }

        @Override
        public void setDefaultValues() throws IllegalStateException {
            delegate.setDefaultValues();
        }

        @Override
        public PropertyDescriptor getDescriptor(String name) {
            return delegate.getDescriptor(name);
        }

        @Override
        public void addPropertyChangeListener(PropertyChangeListener l) {
            delegate.addPropertyChangeListener(l);
        }

        @Override
        public void addPropertyChangeListener(String name, PropertyChangeListener l) {
            delegate.addPropertyChangeListener(name, l);
        }

        @Override
        public void removePropertyChangeListener(PropertyChangeListener l) {
            delegate.removePropertyChangeListener(l);
        }

        @Override
        public void removePropertyChangeListener(String name, PropertyChangeListener l) {
            delegate.removePropertyChangeListener(name, l);
        }
    }
}
