package com.bc.calvalus.processing.l3.multiregion;

import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.operator.BinningConfig;

import java.io.IOException;

/**
 *  The mapper for for formatting
 *  multiple regions of a Binning product at once.
 *
 *  For each region that contains the center lat/lon of the
 *  bin cell the bin emitted.
 */
public class L3MultiRegionFormatMapper extends Mapper<LongWritable, L3TemporalBin, L3MultiRegionBinIndex, L3MultiRegionTemporalBin> implements Configurable {
    private Configuration conf;
    private Geometry[] geometries;
    private PlanetaryGrid planetaryGrid;
    private GeometryFactory geometryFactory;
    private L3MultiRegionTemporalBin mBin;

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        long binIndexLong = binIndex.get();
        double[] centerLatLon = planetaryGrid.getCenterLatLon(binIndexLong);
        Point point = geometryFactory.createPoint(new Coordinate(centerLatLon[1], centerLatLon[0]));

        boolean binContentCopied = false;
        for (int regionId = 0; regionId < geometries.length; regionId++) {
            if (geometries[regionId].contains(point)) {
                L3MultiRegionBinIndex mBinIndex = new L3MultiRegionBinIndex(regionId, binIndexLong);
                if (!binContentCopied) {
                    float[] srcValues = temporalBin.getFeatureValues();
                    if (mBin == null) {
                        mBin = new L3MultiRegionTemporalBin(binIndexLong, srcValues.length);
                    } else {
                        mBin.setIndex(binIndexLong);
                    }
                    mBin.setNumObs(temporalBin.getNumObs());
                    mBin.setNumPasses(temporalBin.getNumPasses());
                    System.arraycopy(srcValues, 0, mBin.getFeatureValues(), 0, srcValues.length);
                    binContentCopied = true;
                }
                context.write(mBinIndex, mBin);
            }
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;

        BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        planetaryGrid = binningConfig.createPlanetaryGrid();
        geometryFactory = new GeometryFactory();

        L3MultiRegionFormatConfig l3MultiRegionFormatConfig = L3MultiRegionFormatConfig.get(conf);
        L3MultiRegionFormatConfig.Region[] regions = l3MultiRegionFormatConfig.getRegions();
        geometries = new Geometry[regions.length];
        for (int i = 0; i < regions.length; i++) {
            // create buffer around given geometry to include also bins that
            // only partially fall into the given geometry
            // work on envelope as the product is rectangular anyways
            Geometry givenGeometry = GeometryUtils.createGeometry(regions[i].getRegionWKT());
            Envelope envelope = givenGeometry.getEnvelopeInternal();
            double longitudeExtend1 = getLongitudeExtend(envelope.getMinY());
            double longitudeExtend2 = getLongitudeExtend(envelope.getMaxY());
            double longitudeExtend = Math.max(longitudeExtend1, longitudeExtend2);
            envelope.expandBy(longitudeExtend, 0.0);
            Geometry extendedGeometry = geometryFactory.toGeometry(envelope);
            geometries[i] = extendedGeometry;
        }
    }

    private double getLongitudeExtend(double latitude) {
        long binIndex = planetaryGrid.getBinIndex(latitude, 0);
        int rowIndex = planetaryGrid.getRowIndex(binIndex);
        int numberOfBinsInRow = planetaryGrid.getNumCols(rowIndex);
        return 360.0 / numberOfBinsInRow;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
