package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobUtils;
import com.vividsolutions.jts.geom.Coordinate;
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
public class L3MultiRegionFormatMapper extends Mapper<LongWritable, L3TemporalBin, L3RegionBinIndex, L3TemporalBin> implements Configurable {
    private Configuration conf;
    private Geometry[] geometries;
    private PlanetaryGrid planetaryGrid;
    private GeometryFactory geometryFactory;

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        long binIndexLong = binIndex.get();
        double[] centerLatLon = planetaryGrid.getCenterLatLon(binIndexLong);
        Point point = geometryFactory.createPoint(new Coordinate(centerLatLon[1], centerLatLon[0]));

        for (int regionId = 0; regionId < geometries.length; regionId++) {
            if (geometries[regionId].contains(point)) {
                L3RegionBinIndex l3RegionBinIndex = new L3RegionBinIndex(regionId, binIndexLong);
                context.write(l3RegionBinIndex, temporalBin);
            }
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        L3MultiRegionFormatConfig l3MultiRegionFormatConfig = L3MultiRegionFormatConfig.get(conf);
        L3MultiRegionFormatConfig.Region[] regions = l3MultiRegionFormatConfig.getRegions();
        geometries = new Geometry[regions.length];
        for (int i = 0; i < regions.length; i++) {
            geometries[i] = JobUtils.createGeometry(regions[i].getRegionWKT());
        }
        L3Config l3Config = L3Config.get(conf);
        planetaryGrid = l3Config.createPlanetaryGrid();
        geometryFactory = new GeometryFactory();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
