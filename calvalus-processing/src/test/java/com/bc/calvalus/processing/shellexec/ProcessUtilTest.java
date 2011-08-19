package com.bc.calvalus.processing.shellexec;

import com.bc.calvalus.processing.UnixTestRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;

/**
 * @author Martin Boettcher
 */
@RunWith(UnixTestRunner.class)
public class ProcessUtilTest {

    @Test
    public void testShellOutput() throws Exception {
        final ProcessUtil p = new ProcessUtil();
        p.directory(new File("."));
        p.run("/bin/bash", "-c", "ls nonexistingdir ; date");
        final String o = p.getOutputString();
        System.out.println("|" + o + "|");
        Assert.assertNotSame("output:", "", o);
    }
}
