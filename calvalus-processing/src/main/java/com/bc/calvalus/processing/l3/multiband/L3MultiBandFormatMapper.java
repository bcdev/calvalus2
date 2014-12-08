package com.bc.calvalus.processing.l3.multiband;

import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionBinIndex;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionFormatConfig;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionTemporalBin;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.binning.PlanetaryGrid;
import org.esa.beam.binning.operator.BinningConfig;

import java.io.IOException;

/**
 *  The mapper for for formatting
 *  multiple regions of a Binning product at once.
 *
 *  For each region that contains the center lat/lon of the
 *  bin cell the bin emitted.
 */
public class L3MultiBandFormatMapper extends Mapper<LongWritable, L3TemporalBin, L3MultiRegionBinIndex, L3MultiBandIndexedValue> implements Configurable {
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        L3MultiRegionBinIndex key = new L3MultiRegionBinIndex(0, binIndex.get());
        L3MultiBandIndexedValue value = new L3MultiBandIndexedValue(key.getBinIndex(), 0.0f);
        for (int i=0; i<temporalBin.getFeatureValues().length; ++i) {
            key.setRegionIndex(i);
            value.setValue(temporalBin.getFeatureValues()[i]);
            context.write(key, value);
        }
    }
}
