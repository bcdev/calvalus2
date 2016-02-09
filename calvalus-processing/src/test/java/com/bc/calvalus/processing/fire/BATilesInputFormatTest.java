package com.bc.calvalus.processing.fire;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author thomas
 */
public class BATilesInputFormatTest {

    @Test
    public void testValidatePattern_1() throws Exception {
        new BATilesInputFormat().validatePattern("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2008/v03h07/2008/2008.*fire-nc/.*nc$");
    }

    @Test
    public void testValidatePattern_2() throws Exception {
        new BATilesInputFormat().validatePattern("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2007/.*/2007/2007.*fire-nc/.*nc$");

    }

    @Test(expected = IOException.class)
    public void testValidatePattern_fail_1() throws Exception {
        new BATilesInputFormat().validatePattern("hdfs://calvaluss/calvalus/projects/fire/sr-fr-default-nc-classic/2007/.*/2007/2007.*fire-nc/.*nc$");
    }

    @Test(expected = IOException.class)
    public void testValidatePattern_fail_2() throws Exception {
        new BATilesInputFormat().validatePattern("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2007/.*/2007.*fire-nc/.*nc$");
    }

    @Test(expected = IOException.class)
    public void testValidatePattern_fail_3() throws Exception {
        new BATilesInputFormat().validatePattern("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2007/.*/2007/2007/.*nc$");
    }

    @Test
    public void testCreateWingsPathPatterns_1() throws Exception {
        String wingsPathPatterns[] = BATilesInputFormat.createWingsPathPatterns("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2008/.*/2008/2008.*fire-nc/.*nc$");

        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2007/.*/2007/2007-1[12]-.*fire-nc/.*nc$",
                wingsPathPatterns[0]);

        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2009/.*/2009/2009-0[12]-.*fire-nc/.*nc$",
                wingsPathPatterns[1]);
    }

    @Test
    public void testAreMyPatternsCorrect() throws Exception {
        assertTrue("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2007/.*/2007/2007-12-26-fire-nc/.*nc$"
                .matches("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2007/.*/2007/2007-1[12]-\\d\\d-fire-nc/.*nc\\$"));

        assertTrue("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2009/v03h07/2009/2009-02-13-fire-nc/.*nc$"
                .matches("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2009/.*/2009/2009-0[12]-\\d\\d-fire-nc/.*nc\\$"));

    }
}
