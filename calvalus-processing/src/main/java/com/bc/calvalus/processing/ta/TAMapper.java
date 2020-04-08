package com.bc.calvalus.processing.ta;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.binning.PlanetaryGrid;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.logging.Logger;

/**
 * Sorts temporal bins (as produced by L3Reducer) by region and maps them to region-time-binIndex keys.
 * The time parameter must be determined by parsing the file path since it is not available in the data.
 *
 * @author Norman
 * @author Martin
 */
public class TAMapper extends Mapper<LongWritable, L3TemporalBin, TAKey, L3TemporalBinWithIndex> implements Configurable {

    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateFormat DATE_FORMAT = DateUtils.createDateFormat(DATE_PATTERN);
    public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    public static final Logger LOGGER = CalvalusLogger.getLogger();

    private Configuration conf;
    private PlanetaryGrid planetaryGrid;
    private TAConfig.RegionConfiguration[] regions;

    public static Date parseDate(String dateString) throws ParseException {
        synchronized (DATE_FORMAT) {
            return DATE_FORMAT.parse(dateString);
        }
    }

    public static String formatDate(Date date) {
        synchronized (DATE_FORMAT) {
            return DATE_FORMAT.format(date);
        }
    }

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        long time = 0;
        double[] centerLatLon = planetaryGrid.getCenterLatLon(binIndex.get());
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(centerLatLon[1], centerLatLon[0]));
        for (int regionId=0; regionId<regions.length; ++regionId) {
            TAConfig.RegionConfiguration region = regions[regionId];
            if (region.getGeometry().contains(point)) {
                if (time == 0) {
                    time = getTimeOfL3(context);
                }
                temporalBin.setIndex(binIndex.get());
                context.write(new TAKey(regionId, time, binIndex.get()), new L3TemporalBinWithIndex(temporalBin, time));
            }
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        planetaryGrid = HadoopBinManager.getBinningConfig(conf).createPlanetaryGrid();
        regions = TAConfig.get(conf).getRegions();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * Retrieval of time from directory name suffix of the input split.
     * hdfs://master00:9000/calvalus/home/martin/425349590245-a245sdfa665/xxx-L3-2013-01-04/part-r-00007
     * @param context  mapper context with reference to input split
     * @return Time long representation of the date of the lowest level directory of the L3 outputs
     * @throws IOException  if the path cannot be parsed
     * @throws InterruptedException
     */
    private long getTimeOfL3(Context context) throws IOException, InterruptedException {
        final String path = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();
        try {
            final String timeString = path.substring(path.length()-DATE_PATTERN.length(), path.length());
            return TAMapper.parseDate(timeString).getTime() + 43200000L;
        } catch (Exception e) {
            throw new IOException("Cannot parse date out of " + path, e);
        }
    }

}
