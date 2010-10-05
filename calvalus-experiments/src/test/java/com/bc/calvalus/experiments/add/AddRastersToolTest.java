package com.bc.calvalus.experiments.add;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AddRastersToolTest {
    @Test
    public void runAddRasters() throws Exception {
        String[] args = new String[]{
                "/tmp/input", "/tmp/output"
        };
        ToolRunner.run(new AddRastersTool(), args);

        LocalFileSystem fileSystem = LocalFileSystem.getLocal(new Configuration());
        final Path file = new Path(args[1], "part-r-00000");
        assertTrue(fileSystem.exists(file));
        FSDataInputStream isOutput = fileSystem.open(file);
        for (int i = 0; i < 4; i++) {
            assertEquals(AddRastersTool.generateInputRasterPixel(0, i)
                    + AddRastersTool.generateInputRasterPixel(1, i)
                    + AddRastersTool.generateInputRasterPixel(2, i),
                                isOutput.read());
        }

        fileSystem.delete(new Path(args[0]), true);
        fileSystem.delete(new Path(args[1]), true);
    }

}
