package com.bc.calvalus.processing.beam;

import com.bc.calvalus.processing.shellexec.ExecutablesTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.FilenameFilter;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Test for {@link BeamOperatorTool}.
 */
public class BeamOperatorToolTest {
    private static final String REQUEST = "calvalus-processing/src/test/resources/beam-l2-mini-request.xml";
    private static final String OUTPUT_DIR = "file:///tmp/meris-l2beam-99";

    @Test
    public void testCommandLine() throws Exception {
        
        // verify that output is empty
        Path outputDir = new Path(OUTPUT_DIR);
        final FileSystem fileSystem = outputDir.getFileSystem(new Configuration());
        assertTrue("Shall not exist: " + outputDir, !fileSystem.exists(outputDir));

        // Run the job
        ToolRunner.run(new BeamOperatorTool(), new String[] { REQUEST });

        assertTrue("Shall now exist: " + outputDir, fileSystem.exists(outputDir));
        final FileStatus[] outputFiles = fileSystem.listStatus(outputDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(".seq");
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
