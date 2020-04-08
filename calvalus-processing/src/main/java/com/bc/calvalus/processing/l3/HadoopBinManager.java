package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.binding.BindingException;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.binning.Aggregator;
import org.esa.snap.binning.BinManager;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.CellProcessorConfig;
import org.esa.snap.binning.DataPeriod;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.support.BinningContextImpl;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.StringUtils;

import java.text.ParseException;

/**
 * Overrides the default implementation in order to create instances of {@link L3SpatialBin} and {@link L3TemporalBin}.
 *
 * @author Norman Fomferra
 */
public class HadoopBinManager extends BinManager {

    public static final String DATE_INPUT_PATTERN = "yyyy-MM-dd";
    public static final String DATETIME_INPUT_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public HadoopBinManager(VariableContext variableContext, CellProcessorConfig postProcessorConfig, Aggregator... aggregators) {
        super(variableContext, postProcessorConfig, aggregators);
    }

    @Override
    public L3SpatialBin createSpatialBin(long binIndex) {
        final L3SpatialBin spatialBin = new L3SpatialBin(binIndex, getSpatialFeatureCount(), getGrowableAggregatorCount());
        initSpatialBin(spatialBin);
        return spatialBin;
    }

    @Override
    public L3TemporalBin createTemporalBin(long binIndex) {
        final L3TemporalBin temporalBin = new L3TemporalBin(binIndex, getTemporalFeatureCount());
        initTemporalBin(temporalBin);
        return temporalBin;
    }

    @Override
    public L3TemporalBin createOutputBin(long binIndex) {
        return new L3TemporalBin(binIndex, getOutputFeatureCount());
    }

    @Override
    public L3TemporalBin createProcessBin(long binIndex) {
        return new L3TemporalBin(binIndex, getPostProcessFeatureCount());
    }

    public static BinningConfig getBinningConfig(Configuration jobConfig) {
        String xml = jobConfig.get(JobConfigNames.CALVALUS_L3_PARAMETERS);
        if (xml == null) {
            throw new IllegalArgumentException(
                    "Missing L3 Binning configuration '" + JobConfigNames.CALVALUS_L3_PARAMETERS + "'");
        }
        try {
            return BinningConfig.fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid L3 Binning configuration: " + e.getMessage(), e);
        }
    }

    public static BinningContext createBinningContext(BinningConfig binningConfig, DataPeriod dataPeriod, Geometry regionGeometry) {
        VariableContext variableContext = binningConfig.createVariableContext();
        Aggregator[] aggregators = binningConfig.createAggregators(variableContext);
        HadoopBinManager binManager = new HadoopBinManager(variableContext, binningConfig.getPostProcessorConfig(),
                                                           aggregators);
        PlanetaryGrid planetaryGrid = binningConfig.createPlanetaryGrid();
        Integer superSampling = binningConfig.getSuperSampling();


        return new BinningContextImpl(planetaryGrid,
                                      binManager,
                                      binningConfig.getCompositingType(),
                                      superSampling != null ? superSampling : 1,
                                      dataPeriod,
                                      regionGeometry);
    }

    public static ProductData.UTC parseStartDateUtc(String date) {
        try {
            if (date.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                return ProductData.UTC.parse(date, DATETIME_INPUT_PATTERN);
            } else {
                return ProductData.UTC.parse(date, DATE_INPUT_PATTERN);
            }
        } catch (ParseException e) {
            String msg = String.format("Error while parsing start date parameter '%s': %s", date,
                                       e.getMessage());
            throw new IllegalArgumentException(msg, e);
        }
    }

    public static DataPeriod createDataPeriod(Configuration conf, Double minDataHour) {
        ProductData.UTC startUtc = null;
        Double periodDuration = null;

        String startUtcString = conf.get(JobConfigNames.CALVALUS_L3_START_UTC);
        if (StringUtils.isNotNullAndNotEmpty(startUtcString)) {
            startUtc = parseStartDateUtc(startUtcString);
        }
        String periodDurationString = conf.get(JobConfigNames.CALVALUS_L3_PERIOD_DURATION);
        if (StringUtils.isNotNullAndNotEmpty(startUtcString)) {
            periodDuration = Double.parseDouble(periodDurationString);
        }
        return BinningConfig.createDataPeriod(startUtc, periodDuration, minDataHour);
    }

}
