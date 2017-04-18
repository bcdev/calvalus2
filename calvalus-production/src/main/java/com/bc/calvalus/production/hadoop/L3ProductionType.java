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
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.ceres.binding.Property;
import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.PropertyDescriptor;
import com.bc.ceres.binding.PropertySet;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.CompositingType;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.VariableConfig;
import org.esa.snap.binning.support.SEAGrid;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

/**
 * A production type used for generating one or more Level-3 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L3ProductionType extends HadoopProductionType {

    public static final int MONTHLY = -30;
    public static final int WEEKLY = -7;

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

        List<DateRange> dateRanges = getDateRanges(productionRequest, 10);
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
        CalvalusLogger.getLogger().info("singleReducer="+singleReducer+" requiresFormatting="+requiresFormatting+" hasQuicklookParameters="+hasQuicklookParameters);
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

    @Override
    protected Staging createUnsubmittedStaging(Production production) throws IOException {
        return new CopyStaging(production,
                getProcessingService().getJobClient(production.getProductionRequest().getUserName()).getConf(),
                getStagingService().getStagingDir());
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
    static List<DateRange> getDateRanges(ProductionRequest productionRequest, int periodLengthDefault) throws
            ProductionException {
        List<DateRange> dateRangeList = new ArrayList<DateRange>();
        Date[] dateList = productionRequest.getDates("dateList", null);

        if (dateList != null) {
            for (Date date : dateList) {
                dateRangeList.add(new DateRange(date, date));
            }
        } else {
            Date minDate = productionRequest.getDate("minDate");
            Date maxDate = productionRequest.getDate("maxDate");
            int periodLength = productionRequest.getInteger("periodLength", periodLengthDefault); // unit=days
            int compositingPeriodLength = productionRequest.getInteger("compositingPeriodLength",
                    periodLength); // unit=days

            final GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

            // set end of the interval to beginning of the following day for simpler comparison
            calendar.setTime(maxDate);
            calendar.add(Calendar.DAY_OF_MONTH, 1);
            final Date maxDate1 = calendar.getTime();

            // adjust start date in monthly and weekly period
            calendar.setTime(minDate);
            maybeAdjustCalendarToPeriodStart(calendar, periodLength);

            while (true) {

                // determine start and end of period
                final Date date1 = calendar.getTime();
                forwardCalendarByPeriod(calendar, compositingPeriodLength);
                calendar.add(Calendar.SECOND, -1);
                final Date date2 = calendar.getTime();

                // check whether end of period exceeds end of overall interval
                if (date2.after(maxDate1)) {
                    break;
                }
                // accumulate date range for period
                dateRangeList.add(new DateRange(date1, date2));

                // proceed by one period length
                calendar.setTime(date1);
                forwardCalendarByPeriod(calendar, periodLength);
            }
        }

        return dateRangeList;
    }

    /**
     * Forwards to start of month in case of monthly
     * and start of "week" (beginning with 1st of Jan) in case of weekly.
     *
     * @param calendar
     * @param periodLength larger than 0 for days, -30 for monthly, -7 for weekly periods
     */
    private static void maybeAdjustCalendarToPeriodStart(GregorianCalendar calendar, int periodLength) {
        if (periodLength == MONTHLY) {
            if (calendar.get(Calendar.DAY_OF_MONTH) != 1) {
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                calendar.add(Calendar.MONTH, 1);
            }
        } else if (periodLength == WEEKLY) {
            final int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
            calendar.set(Calendar.DAY_OF_YEAR, 1);
            while (calendar.get(Calendar.DAY_OF_YEAR) < dayOfYear) {
                forwardByOneWeekOfYear(calendar);
            }
        } else if (periodLength <= 0) {
            throw new IllegalArgumentException("Compositing period " + periodLength + " not supported");
        }
    }

    /**
     * Forwards calendar by period days, or one calendar month, or one "week" (see next method).
     *
     * @param calendar
     * @param period   larger than 0 for days, -30 for monthly, -7 for weekly periods
     */
    private static void forwardCalendarByPeriod(GregorianCalendar calendar, int period) {
        if (period > 0) {
            calendar.add(Calendar.DATE, period);
        } else if (period == MONTHLY) {
            calendar.add(Calendar.MONTH, 1);
        } else if (period == WEEKLY) {
            forwardByOneWeekOfYear(calendar);
        } else {
            throw new IllegalArgumentException("Compositing period " + period + " not supported");
        }
    }

    /**
     * Proceed calendar by 7 days, and add one additional day for the week containing the
     * 29th of Feb in leap years and for the last week of the year to contain the 31th of Dec.
     *
     * @param calendar
     */
    private static void forwardByOneWeekOfYear(GregorianCalendar calendar) {
        final int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
        if ((dayOfYear == 8 * 7 + 1 && calendar.isLeapYear(calendar.get(Calendar.YEAR))) || dayOfYear >= 51 * 7 + 1) {
            calendar.add(Calendar.DATE, 8);
        } else {
            calendar.add(Calendar.DATE, 7);
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
        binningConfig.setMaskExpr(productionRequest.getString("maskExpr", ""));
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
        VariableConfig[] variableConfigs = new VariableConfig[expressionCount];
        for (int i = 0; i < expressionCount; i++) {
            String prefix = "expression." + i;
            String name = request.getString(prefix + ".variable");
            String exp = request.getString(prefix + ".expression");
            variableConfigs[i] = new VariableConfig(name, exp);
        }
        return variableConfigs;
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
