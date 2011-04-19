package com.bc.calvalus.processing.shellexec;

import com.bc.calvalus.processing.beam.HadoopStandaloneAndAccountTestRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * 
 */
@RunWith(HadoopStandaloneAndAccountTestRunner.class)
public class ExecutablesToolTest {
    private static final String REQUEST = "calvalus-processing/src/test/resources/l2gen-mini-request.xml";
    private static final String INPUT_DIR = "file:///tmp/meris-l2gen-01";
    private static final String INPUT_FILE = "MER_RR__1P.N1";
    private static final String OUTPUT_DIR = "file:///tmp/meris-l2gen-99";
    private static final String INPUT_RESOURCE_PATH = "calvalus-processing/src/test/resources/MER_RR__1P.N1";

    @Test
    public void runProcessing() throws Exception {
        
        // verify that output is empty
        Path outputFile = new Path(OUTPUT_DIR);
        assertTrue("Shall not exist: " + outputFile,
                   ! outputFile.getFileSystem(new Configuration()).exists(outputFile));

        // put input to directory used in request
        provideInput();

        // Run the job
        ToolRunner.run(new ExecutablesTool(), new String[] { REQUEST });

        assertTrue("Shall now exist: " + outputFile,
                   outputFile.getFileSystem(new Configuration()).exists(outputFile));
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

    private void provideInput() throws IOException {
        Path sourcePath = new Path("file://" + new File(INPUT_RESOURCE_PATH).getAbsolutePath());
        Path targetPath = new Path(INPUT_DIR);
        targetPath.getFileSystem(new Configuration()).copyFromLocalFile(sourcePath, new Path(targetPath, INPUT_FILE));
    }

}
