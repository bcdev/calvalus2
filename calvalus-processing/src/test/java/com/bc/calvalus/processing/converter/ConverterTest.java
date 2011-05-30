package com.bc.calvalus.processing.converter;

import com.bc.calvalus.processing.shellexec.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;


@Ignore
public class ConverterTest {
    private static final String INPUT_DIR = "file:///tmp/converter-input";
    private static final String OUTPUT_DIR = "file:///tmp/converter-output";
    private static final String INPUT_FILE = "L2_of_MER_RR__1P.N1.seq";
    private static final String INPUT_FILE2 = "wps-request.xml";
    private static final String INPUT_RESOURCE_PATH = "calvalus-processing/src/test/resources/" + INPUT_FILE;
    private static final String INPUT_RESOURCE_PATH2 = "calvalus-processing/src/test/resources/" + INPUT_FILE2;

    @Test
    public void testSeqToDimConverter() throws Exception {

        final Configuration configuration = new Configuration();

        final Path outputPath = new Path(OUTPUT_DIR);
        assertTrue("Shall not exist: " + outputPath,
                   !outputPath.getFileSystem(configuration).exists(outputPath));

        provideInput();
        outputPath.getFileSystem(new Configuration()).mkdirs(outputPath);

        new SeqToDimConverter().convert("test", new Path(INPUT_DIR + File.separator + INPUT_FILE), outputPath.toUri().getPath(), null, configuration);

        final Path outputFile = new Path(OUTPUT_DIR + File.separator + INPUT_FILE.replace(".seq", ".dim"));
        assertTrue("Shall now exist: " + outputFile,
                   outputPath.getFileSystem(configuration).exists(outputFile));
    }

    @Test
    public void testLcL3Converter() throws Exception {

        final Configuration configuration = new Configuration();
        final String request = FileUtil.readFile("calvalus-processing/src/test/resources/convert-l3-test-request.xml");
        configuration.set("calvalus.request", request);

        final Path outputPath = new Path(OUTPUT_DIR);
        assertTrue("Shall not exist: " + outputPath,
                   !outputPath.getFileSystem(configuration).exists(outputPath));

        provideInput2();
        outputPath.getFileSystem(new Configuration()).mkdirs(outputPath);

        new LcL3Converter().convert("test", new Path(INPUT_DIR + File.separator + INPUT_FILE2), outputPath.toUri().getPath(), null, configuration);

        final Path outputFile = new Path(OUTPUT_DIR + File.separator + "l3-sr-test.nc");
        assertTrue("Shall now exist: " + outputFile,
                   outputPath.getFileSystem(configuration).exists(outputFile));
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

    private void provideInput2() throws IOException {
        Path sourcePath = new Path("file://" + new File(INPUT_RESOURCE_PATH2).getAbsolutePath());
        Path targetPath = new Path(INPUT_DIR);
        targetPath.getFileSystem(new Configuration()).copyFromLocalFile(sourcePath, new Path(targetPath, INPUT_FILE2));
    }

}
