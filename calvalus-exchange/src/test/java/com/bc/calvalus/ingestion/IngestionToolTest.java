package com.bc.calvalus.ingestion;

import org.junit.Test;

import java.io.File;
import java.util.regex.Pattern;

import static org.junit.Assert.*;


public class IngestionToolTest {
    @Test
    public void testPatternDatePath() {
          assertEquals("2006/07/18", IngestionTool.getDatePath(new File("MER_RR__1POBCM20060718_091715_000001012049_00308_22907_0113.N1"), "", Pattern.compile("..............(....)(..)(..).*")));
    }
    @Test
    public void testTypeDatePath() {
          assertEquals("2006/07/18", IngestionTool.getDatePath(new File("MER_RR__1POBCM20060718_091715_000001012049_00308_22907_0113.N1"), "MER_RR__1P", Pattern.compile(".*")));
    }
    @Test
    public void testPattern2DatePath() {
          assertEquals("2006/07/18", IngestionTool.getDatePath(new File("A2006199xxx.hdf"), "", Pattern.compile(".(....)(...).*")));
    }
    @Test
    public void testPattern3DatePath() {
          assertEquals("2006/07/18", IngestionTool.getDatePath(new File("NSS.GHRR.NM.D06199.S1409.E1554.B2348788.WI.gz"), "", Pattern.compile("NSS\\.GHRR\\...\\.D(..)(...).*")));
    }
    @Test
    public void testDirectoryDatePath() {
          assertEquals("2006/07/18", IngestionTool.getDatePath(new File("/mnt/test/2006/07/18/A2006199xxx.hdf"), "notype", Pattern.compile(".*")));
    }
    @Test
    public void testNoPatternDatePath() {
          assertEquals(".", IngestionTool.getDatePath(new File("A2006199xxx.hdf"), "notype", Pattern.compile(".*")));
    }
    @Test
    public void testAvhrrDatePath() {
          assertEquals("1999/07/01", IngestionTool.getDatePath(new File("ao14070199085656_090012.l1b"), "AVHRR_L1B", Pattern.compile(".*")));
    }

    @Test
    public void testArchivePath() {
        String archivePath = IngestionTool.getArchivePath(new File("MER_RR__1POBCM20060718_091715_000001012049_00308_22907_0113.N1"), "MER_RR__1P", "r03", Pattern.compile("..............(....)(..)(..).*"));
        assertEquals("/calvalus/eodata/MER_RR__1P/r03/2006/07/18", archivePath);
    }

//    @Test
//    public void testAcceptFilename() {
//        IngestionTool.ProductFilenameFilter filenameFilter = new IngestionTool.ProductFilenameFilter("MER_RR__1P");
//        assertTrue(filenameFilter.accept(null, "MER_RR__1POBCM20060718_091715_000001012049_00308_22907_0113.N1"));
//        assertFalse(filenameFilter.accept(null, "MER_RR__1POBCM20060718_091715_000001012049_00308_22907_0113.N2"));
//        assertFalse(filenameFilter.accept(null, "MER_RR__2POBCM20060718_091715_000001012049_00308_22907_0113.N1"));
//    }

}
