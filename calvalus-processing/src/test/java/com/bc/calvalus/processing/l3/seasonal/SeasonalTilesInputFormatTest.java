package com.bc.calvalus.processing.l3.seasonal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class SeasonalTilesInputFormatTest {
    @Test
    public void testCreateSplits() throws Exception {
        BlockLocation[] blockLocations = {new BlockLocation(new String[]{"name"}, new String[]{"host"}, 0, 99)};
        SeasonalTilesInputFormat inputFormat = new SeasonalTilesInputFormat();
        FileStatus[] files = new FileStatus[] {
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20090108-v2.0.nc")),
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20090115-v2.0.nc")),
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20110115-v2.0.nc")),
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v09-20090115-v2.0.nc")),
        };
        List<InputSplit> splits = new ArrayList<>();
        Iterator<FileStatus> iterator = Arrays.asList(files).iterator();
        RemoteIterator<LocatedFileStatus> remoteIter = new RemoteIterator<LocatedFileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
                return iterator.hasNext();
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                return new LocatedFileStatus(iterator.next(), blockLocations);
            }
        };
        inputFormat.createSplits(null, remoteIter, splits, new Configuration(), 0, false);

        assertEquals(2, splits.size());
    }
}
