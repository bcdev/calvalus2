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
import java.io.FilenameFilter;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Test for {@link BeamOperatorTool}, tests deployment of BEAM and the MERIS.Radiometry operator on a local Hadoop instance.
 *
 */
@RunWith(HadoopStandaloneTestRunner.class)
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
        Configuration conf = new Configuration();
        conf.addResource("mini-conf/core-site.xml");
        conf.addResource("mini-conf/hdfs-site.xml");
        conf.addResource("mini-conf/mapred-site.xml");
        ToolRunner.run(conf, new BeamOperatorTool(), new String[] { REQUEST });

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
        installJars(new File("calvalus-processing/target/calvalus-processing-0.1-SNAPSHOT-beam"), "beam-4.9-SNAPSHOT");
        installJars(new File("calvalus-processing/target/beam-meris-radiometry-1.0-SNAPSHOT"), "beam-meris-radiometry-1.0-SNAPSHOT");
    }

//    @After
//    public void finish() throws IOException {
//        clearWorkingDirs();
//    }

    private void clearWorkingDirs() throws IOException {
        Path outputDir = new Path(OUTPUT_DIR);
        outputDir.getFileSystem(new Configuration()).delete(outputDir, true);
    }

    /**
     * Copies jars of source dir to HDFS software installation dir for package
     * @param sourceDir  local directory of package jar files
     * @param packageName   name and version of package, must be the same as later in the request
     * @throws IOException  if copying fails
     */
    private void installJars(File sourceDir, String packageName) throws IOException {
        // determine jars of package
        final File[] jars = sourceDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                return name.endsWith("jar") && ! name.endsWith("sources.jar");
            }
        });
        // define destination in HDFS software dir
        final Path targetPath = new Path("hdfs://localhost:9000/calvalus/software/0.5/" + packageName);
        final FileSystem targetFileSystem = targetPath.getFileSystem(new Configuration());
        // copy jars of package to destination
        for (File jar : jars) {
            Path sourcePath = new Path("file://" + jar.getAbsolutePath());
            targetFileSystem.copyFromLocalFile(sourcePath, new Path(targetPath, jar.getName()));
        }
    }
}
