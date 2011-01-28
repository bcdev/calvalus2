package com.bc.calvalus.processing.shellexec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * 
 */
public class CommandLineTest {

    @Test
    public void submitEmptyCommandLine() throws Exception {
        // submit empty command line
        final int returnCode = ToolRunner.run(new ExecutablesTool(), new String[]{});
        // check return code
        assertTrue("failure return code", returnCode != 0);
    }
}
