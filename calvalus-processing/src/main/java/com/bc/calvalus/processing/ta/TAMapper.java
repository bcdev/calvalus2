package com.bc.calvalus.processing.ta;

import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.L3Config;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Maps temporal bins (as produced by L3Reducer) to region keys.
 *
 * @author Norman
 */
public class TAMapper extends Mapper<LongWritable, TemporalBin, Text, TemporalBin> implements Configurable {
    private Configuration conf;
    private BinningGrid binningGrid;
    private TAConfig taConfig;

    @Override
    protected void map(LongWritable binIndex, TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        GeometryFactory geometryFactory = new GeometryFactory();
        double[] centerLonLat = binningGrid.getCenterLonLat(binIndex.get());
        Point point = geometryFactory.createPoint(new Coordinate(centerLonLat[0], centerLonLat[1]));
        TAConfig.RegionConfiguration[] regions = taConfig.getRegions();
        for (TAConfig.RegionConfiguration region : regions) {
            if (region.getGeometry().contains(point)) {
                context.write(new Text(region.getName()), temporalBin);
            }
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        L3Config l3Config = L3Config.fromXml(conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
        binningGrid = l3Config.getBinningContext().getBinningGrid();
        taConfig = TAConfig.fromXml(conf.get(JobConfigNames.CALVALUS_TA_PARAMETERS));
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
