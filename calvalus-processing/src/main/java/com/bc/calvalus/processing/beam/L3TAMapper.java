package com.bc.calvalus.processing.beam;

import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.TemporalBin;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Maps the output of L3Reducer to given geographical regions.
 */
public class L3TAMapper extends Mapper<LongWritable, TemporalBin, IntWritable, TemporalBin> implements Configurable {
    private Configuration conf;
    private BinningGrid binningGrid;
    private ROI[] rois;
    private GeometryFactory geometryFactory;

    @Override
    protected void map(LongWritable key, TemporalBin value, Context context) throws IOException, InterruptedException {
        double[] centerLonLat = binningGrid.getCenterLonLat(key.get());
        Point point = geometryFactory.createPoint(new Coordinate(centerLonLat[0], centerLonLat[1]));
        for (int i = 0; i < rois.length; i++) {
            ROI roi = rois[i];
            if (roi.geometry.contains(point)) {
                 context.write(new IntWritable(i), value);
            }
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String level3Parameters = conf.get(ProcessingConfiguration.CALVALUS_L3_PARAMETER);
        L3Config l3Config = L3Config.create(level3Parameters);
        binningGrid = l3Config.getBinningContext().getBinningGrid();

        geometryFactory = new GeometryFactory();
        rois = getRegionsOfInterest(conf);
    }

    private ROI[] getRegionsOfInterest(Configuration conf) {
        return new ROI[]{
                new ROI("region1",
                        geometryFactory.createPolygon(geometryFactory.createLinearRing(
                                new Coordinate[]{
                                        new Coordinate(20, 20),
                                }
                        ), null),
                        0.0),
                new ROI("region2",
                        geometryFactory.createPolygon(geometryFactory.createLinearRing(
                                new Coordinate[]{
                                        new Coordinate(20, 20),
                                }
                        ), null),
                        0.0),
                new ROI("region3",
                        geometryFactory.createPolygon(geometryFactory.createLinearRing(
                                new Coordinate[]{
                                        new Coordinate(20, 20),
                                }
                        ), null),
                        0.0),
        };
    }

    @Override
    public Configuration getConf() {
        return conf;
    }


    public static class ROI {
        String name;
        Geometry geometry;
        double minDepth;

        public ROI(String name, Geometry geometry, double minDepth) {
            this.name = name;
            this.geometry = geometry;
            this.minDepth = minDepth;
        }
    }
}
