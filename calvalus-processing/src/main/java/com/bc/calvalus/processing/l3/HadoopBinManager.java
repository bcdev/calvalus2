package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.Aggregator;
import org.esa.beam.binning.BinManager;
import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.CellProcessorConfig;
import org.esa.beam.binning.DataPeriod;
import org.esa.beam.binning.PlanetaryGrid;
import org.esa.beam.binning.VariableContext;
import org.esa.beam.binning.operator.BinningConfig;
import org.esa.beam.binning.support.BinningContextImpl;

/**
 * Overrides the default implementation in order to create instances of {@link L3SpatialBin} and {@link L3TemporalBin}.
 *
 * @author Norman Fomferra
 */
public class HadoopBinManager extends BinManager {

    public HadoopBinManager(VariableContext variableContext, CellProcessorConfig postProcessorConfig, Aggregator... aggregators) {
        super(variableContext, postProcessorConfig, aggregators);
    }

    @Override
    public L3SpatialBin createSpatialBin(long binIndex) {
        final L3SpatialBin spatialBin = new L3SpatialBin(binIndex, getSpatialFeatureCount());
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
            throw new IllegalArgumentException("Missing L3 Binning configuration '" + JobConfigNames.CALVALUS_L3_PARAMETERS + "'");
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
        HadoopBinManager binManager = new HadoopBinManager(variableContext, binningConfig.getPostProcessorConfig(), aggregators);
        PlanetaryGrid planetaryGrid = binningConfig.createPlanetaryGrid();
        Integer superSampling = binningConfig.getSuperSampling();


        return new BinningContextImpl(planetaryGrid,
                                      binManager,
                                      binningConfig.getCompositingType(),
                                      superSampling != null ? superSampling : 1,
                                      dataPeriod,
                                      regionGeometry);
    }
}
