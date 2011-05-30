package com.bc.calvalus.processing.shellexec;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
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
