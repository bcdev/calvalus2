package com.bc.calvalus.ingestion;

import org.junit.Test;

import static org.junit.Assert.*;


public class IngestionToolTest {
    @Test
    public void testDataPath() {
          assertEquals("2006/07/18", IngestionTool.getDatePath("MER_RR__1POBCM20060718_091715_000001012049_00308_22907_0113.N1"));
    }

    @Test
    public void testArchivePath() {
        String archivePath = IngestionTool.getArchivePath("MER_RR__1POBCM20060718_091715_000001012049_00308_22907_0113.N1", "MER_RR__1P", "r03");
        assertEquals("/calvalus/eodata/MER_RR__1P/r03/2006/07/18", archivePath);
    }

    @Test
    public void testAcceptFilename() {
        IngestionTool.ProductFilenameFilter filenameFilter = new IngestionTool.ProductFilenameFilter("MER_RR__1P");
        assertTrue(filenameFilter.accept(null, "MER_RR__1POBCM20060718_091715_000001012049_00308_22907_0113.N1"));
        assertFalse(filenameFilter.accept(null, "MER_RR__1POBCM20060718_091715_000001012049_00308_22907_0113.N2"));
        assertFalse(filenameFilter.accept(null, "MER_RR__2POBCM20060718_091715_000001012049_00308_22907_0113.N1"));
    }
}
