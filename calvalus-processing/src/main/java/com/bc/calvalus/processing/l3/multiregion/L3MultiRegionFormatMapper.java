package com.bc.calvalus.processing.l3.multiregion;

import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.binning.PlanetaryGrid;

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

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        long binIndexLong = binIndex.get();
        double[] centerLatLon = planetaryGrid.getCenterLatLon(binIndexLong);
        Point point = geometryFactory.createPoint(new Coordinate(centerLatLon[1], centerLatLon[0]));

        L3MultiRegionTemporalBin mBin = null;
        for (int regionId = 0; regionId < geometries.length; regionId++) {
            if (geometries[regionId].contains(point)) {
                L3MultiRegionBinIndex mBinIndex = new L3MultiRegionBinIndex(regionId, binIndexLong);
                if (mBin == null) {
                    mBin = new L3MultiRegionTemporalBin(binIndexLong, temporalBin);
                }
                context.write(mBinIndex, mBin);
            }
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        L3Config l3Config = L3Config.get(conf);
        planetaryGrid = l3Config.createPlanetaryGrid();
        geometryFactory = new GeometryFactory();

        L3MultiRegionFormatConfig l3MultiRegionFormatConfig = L3MultiRegionFormatConfig.get(conf);
        L3MultiRegionFormatConfig.Region[] regions = l3MultiRegionFormatConfig.getRegions();
        geometries = new Geometry[regions.length];
        for (int i = 0; i < regions.length; i++) {
            // create buffer around given geometry to include also bins that
            // only partially fall into the given geometry
            // work on envelope as the product is rectangular anyways
            Geometry givenGeometry = JobUtils.createGeometry(regions[i].getRegionWKT());
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
