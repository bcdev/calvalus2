package com.bc.calvalus.processing.l3;


import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.processing.UnixTestRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InMemoryFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Progressable;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import static org.junit.Assert.*;

@RunWith(UnixTestRunner.class)
public class SequenceFileBinIteratorTest {
    public static final Path PATH = new Path("SequenceFileBinIteratorTest.seq");
    private Configuration conf;
    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        fs = FileSystem.getLocal(conf);
    }

    @Test
    public void testIO() throws Exception {
        SequenceFile.Writer writer = SequenceFile.createWriter(fs,
                                                               conf,
                                                               PATH,
                                                               LongWritable.class,
                                                               TemporalBin.class);

        TemporalBin val = new TemporalBin(2L, 3);
        val.getFeatureValues()[0] = 0.1F;
        val.getFeatureValues()[1] = 0.2F;
        val.getFeatureValues()[2] = 0.3F;
        val.setNumObs(2354);
        val.setNumPasses(54);
        writer.append(new LongWritable(2L), val);

        writer.close();

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, PATH, conf);
        Iterator<TemporalBin> it = new SequenceFileBinIterator(reader);

        assertTrue(it.hasNext());
        TemporalBin bin = it.next();
        assertNotNull(bin);
        assertNotNull(bin.getFeatureValues());
        assertEquals(3, bin.getFeatureValues().length);
        assertEquals(2L, bin.getIndex());
        assertEquals(2354, bin.getNumObs());
        assertEquals(54, bin.getNumPasses());

        assertFalse(it.hasNext());
    }
}
