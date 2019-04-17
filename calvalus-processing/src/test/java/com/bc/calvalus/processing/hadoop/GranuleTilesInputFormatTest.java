package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.processing.l3.seasonal.SeasonalTilesInputFormat;
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
public class GranuleTilesInputFormatTest {
    @Test
    public void testCreateSplits() throws Exception {
        BlockLocation[] blockLocations = {new BlockLocation(new String[]{"name"}, new String[]{"host"}, 0, 99)};
        GranuleTilesInputFormat inputFormat = new GranuleTilesInputFormat();
        FileStatus[] files = new FileStatus[] {
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/S2A_MSIL1C_20170224T180431_N0204_R041_T12RWV_20170224T180427.zip")),
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/S2A_MSIL1C_20170214T180431_N0204_R041_T12RWV_20170214T180427.zip")),
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/S2A_MSIL1C_20170215T180431_N0204_R041_T12XXX_20170215T180427.zip")),
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/S2B_MSIL1C_20170217T180431_N0204_R041_T12RWV_20170217T180427.zip")),
                new FileStatus(99, false, 1, 99, 0, new Path("/ab/cd/S2A_OPER_PRD_MSIL1C_PDMC_20161123T211306_R141_V20161123T175652_20161123T175652_T12RWV.zip")),
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

        assertEquals(4, splits.size());
    }
}
