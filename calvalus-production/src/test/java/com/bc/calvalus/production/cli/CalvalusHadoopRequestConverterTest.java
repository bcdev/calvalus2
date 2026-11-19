package com.bc.calvalus.production.cli;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusHadoopRequestConverterTest extends TestCase {

    @Test
    public void testIsRelative() {
        assertTrue(CalvalusHadoopRequestConverter.isRelativePath("test-results/converter"));
        assertFalse(CalvalusHadoopRequestConverter.isRelativePath("/calvalus/home/cvop/test-results/converter"));
        assertFalse(CalvalusHadoopRequestConverter.isRelativePath("s3a://hroc/calvalus/home/cvop/test-results/converter"));
        assertFalse(CalvalusHadoopRequestConverter.isRelativePath("hdfs://calvalus/calvalus/home/cvop/test-results/converter"));
    }
}