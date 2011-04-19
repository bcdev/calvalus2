package com.bc.calvalus.processing.beam;

import com.bc.calvalus.processing.hadoop.HadoopStandaloneTestRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link com.bc.calvalus.processing.beam.BeamOperatorTool}, tests deployment of BEAM and the MERIS.Radiometry operator on a local Hadoop instance.
 *
 */
@RunWith(HadoopStandaloneTestRunner.class)
public class BeamOutputConverterToolTest {
    private static final String INPUT_RESOURCE = "calvalus-processing/src/test/resources/L2_of_MER_RR__1P.N1.seq";
    private static final String OUTPUT_DIR = "file:///tmp/meris-l2beam-88";

    @Test
    public void testConversion() throws Exception {

        // verify that output is empty
        Path outputDir = new Path(OUTPUT_DIR);
        final FileSystem fileSystem = outputDir.getFileSystem(new Configuration());
        assertTrue("Shall not exist: " + outputDir, !fileSystem.exists(outputDir));

        // call the conversion tool
        Configuration conf = new Configuration();
        conf.addResource("mini-conf/core-site.xml");
        conf.addResource("mini-conf/hdfs-site.xml");
        conf.addResource("mini-conf/mapred-site.xml");
        final File inputFile = new File(INPUT_RESOURCE);
        final String input = "file://" + inputFile.getAbsolutePath();
        final String output = outputDir.toUri().getPath() + "/" + inputFile.getName();
        ToolRunner.run(conf, new BeamOutputConverterTool(), new String[] {input, output});

        assertTrue("Shall now exist: " + outputDir, fileSystem.exists(outputDir));
        final FileStatus[] outputFiles = fileSystem.listStatus(outputDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(".dim");
            }
        });
        assertEquals(1, outputFiles.length);
    }

    @Before
    public void init() throws IOException {
        clearWorkingDirs();
    }

//    @After
//    public void finish() throws IOException {
//        clearWorkingDirs();
//    }

    private void clearWorkingDirs() throws IOException {
        Path outputDir = new Path(OUTPUT_DIR);
        outputDir.getFileSystem(new Configuration()).delete(outputDir, true);
    }
}
