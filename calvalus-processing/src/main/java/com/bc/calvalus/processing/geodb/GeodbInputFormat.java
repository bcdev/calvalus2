/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.geodb;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordSource;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.SafeUpdateInventory;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.utils.SimpleRecord;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.util.StringUtils;

import java.awt.geom.Point2D;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * An input format using the geo inventory for quickly finding products that
 * match the given time and/or geo constrains.
 *
 * For compatibility reasons this input format is ONLY used from within the PatternBasedInputFormat !!!
 *
 */
public class GeodbInputFormat extends InputFormat {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int requestSizeLimit = conf.getInt(JobConfigNames.CALVALUS_REQUEST_SIZE_LIMIT, 0);
        Set<String> paths = queryGeoInventory(true, conf);
        List<InputSplit> splits = createInputSplits(conf, paths, requestSizeLimit);
        String geoInventory = conf.get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY);
        LOG.info(String.format("%d splits added (from %d returned from geo-inventory '%s').", splits.size(), paths.size(), geoInventory));
        return splits;
    }

    public static List<InputSplit> createInputSplits(Configuration conf, Collection<String> paths, int requestSizeLimit) throws IOException {
        List<InputSplit> splits = new ArrayList<>(requestSizeLimit);
        for (String stringPath : paths) {
            final Path path = new Path(stringPath);
            FileSystem fileSystem = path.getFileSystem(conf);
            try {
                final FileStatus status = fileSystem.getFileStatus(path);
                if (status != null) {
                    final BlockLocation[] locations = fileSystem.getFileBlockLocations(status, 0, status.getLen());
                    if (locations == null || locations.length == 0) {
                        LOG.warning("cannot find hosts of input " + stringPath);
                    } else {
                        LOG.fine("adding input split for  " + path.toUri().getPath());
                        splits.add(new ProductSplit(path, status.getLen(), locations[0].getHosts()));
                    }
                } else {
                    LOG.warning("cannot find input " + stringPath);
                }
            } catch (FileNotFoundException e) {
                LOG.warning("cannot find input " + stringPath);
            }
            if (requestSizeLimit > 0 && splits.size() == requestSizeLimit) {
                break;
            }
        }
        return splits;
    }

    public static Set<String> queryGeoInventory(boolean failOnMissingDB, Configuration conf) throws IOException {
        Constrain constrain = parseConstraint(conf);
        Set<String> paths = new HashSet<>();
        String[] geoInventories = conf.get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY).split(",");
        for (String geoInventory : geoInventories) {
            StreamFactory streamFactory = new HDFSStreamFactory(conf);
            SafeUpdateInventory inventory = new SafeUpdateInventory(streamFactory, geoInventory);
            inventory.setUpdatePrefix("scan.");
            inventory.setVerbose(true);
            inventory.setFailOnMissingDB(failOnMissingDB);
            paths.addAll(inventory.query(constrain));
        }
        return paths;
    }

    private static Constrain parseConstraint(Configuration conf) throws IOException {
        Constrain.Builder cb = new Constrain.Builder();
        String geometryWkt = conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY);
        if (StringUtils.isNotNullAndNotEmpty(geometryWkt)) {
            Geometry geometry = GeometryUtils.parseWKT(geometryWkt);
            if (geometry != null && !isBiggerThanAHalfSphere(geometry)) {
                cb.withPolygon(geometryWkt);    
            }
        }
        cb.useOnlyProductStartDate(true);
        parseMatchupParameters(conf, cb);

        String dateRangesString = conf.get(JobConfigNames.CALVALUS_INPUT_DATE_RANGES);
        boolean isDateRangeSet = StringUtils.isNotNullAndNotEmpty(dateRangesString);
        if (isDateRangeSet) {
            String[] dateRangesStrings = dateRangesString.split(",");
            for (String dateRangeString : dateRangesStrings) {
                try {
                    DateRange dateRange = DateRange.parseDateRange(dateRangeString);
                    cb.addDateRang(dateRange.getStartDate(), dateRange.getStopDate());
                } catch (ParseException e) {
                    throw new IOException(e);
                }
            }
        }
        return cb.build();
    }

    private static boolean isBiggerThanAHalfSphere(Geometry geometry) {
        double geometryArea = geometry.getArea();
        double halfSphereCartesian = 360 * 180 * 0.5;
        if (geometryArea > halfSphereCartesian) {
            System.out.printf("geometryArea %s is bigger than half a sphere an not supported by S2, dropping it for DB query.%n", halfSphereCartesian);
            return true;
        }
        return false;
    }

    private static void parseMatchupParameters(Configuration conf, Constrain.Builder cb) {
        String maXML = conf.get(JobConfigNames.CALVALUS_MA_PARAMETERS);
        if (StringUtils.isNotNullAndNotEmpty(maXML)) {
            try {
                MAConfig maConfig = MAConfig.fromXml(maXML);
                RecordSource maRecordSource = maConfig.createRecordSource(conf);
                List<SimpleRecord> simpleRecords = new ArrayList<>();
                if (!maRecordSource.getHeader().hasLocation()) {
                    return;
                }
                boolean hasTime = maRecordSource.getHeader().hasTime();
                for (Record record : maRecordSource.getRecords()) {
                    GeoPos geoPos = record.getLocation();
                    Point2D location = new Point2D.Double(geoPos.getLon(), geoPos.getLat());
                    if (hasTime) {
                        long time = record.getTime().getTime();
                        simpleRecords.add(new SimpleRecord(time, location));
                    } else {
                        simpleRecords.add(new SimpleRecord(location));
                    }
                }
                cb.withInsituRecords(simpleRecords);
                if (hasTime) {
                    cb.useOnlyProductStartDate(false);
                }
                String maxTimeDifference = maConfig.getMaxTimeDifference();
                if (maxTimeDifference != null) {
                    if (maxTimeDifference.trim().endsWith("d")) {
                        String trimmed = maxTimeDifference.trim();
                        String daysAsString = trimmed.substring(0, trimmed.length() - 1);
                        int days = Integer.parseInt(daysAsString);
                        cb.withInsituTimeDelta((days + 2) * 24 * 60 * 60 * 1000L); // TODO teach geoDB to understand 0d,1d,..
                    } else {
                        double timeDifferenceHours = Double.parseDouble(maxTimeDifference);
                        if (timeDifferenceHours > 0) {
                            long timeDelta = Math.round(timeDifferenceHours * 60 * 60 * 1000); // h to ms
                            cb.withInsituTimeDelta(timeDelta);
                        }
                    }
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse matchups parameters.", e);
            }
        }
    }

    /**
     * Creates a {@link NoRecordReader} because records are not used with this input format.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split,
                                                                       TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new NoRecordReader();
    }

}
