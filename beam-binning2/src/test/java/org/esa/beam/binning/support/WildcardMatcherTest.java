package org.esa.beam.binning.support;

import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author Norman Fomferra
 */
public class WildcardMatcherTest {
    @Test
    public void testFileAssumptions() throws Exception {
        String baseDir = new File("").getCanonicalPath();
        assertEquals(baseDir, new File(".").getCanonicalPath());
        assertEquals(baseDir, new File("./").getCanonicalPath());
        assertEquals(baseDir + File.separator + "test.txt",
                     new File("./test.txt").getCanonicalPath());
        assertEquals(baseDir + File.separator + "test.txt",
                     new File("sub/../test.txt").getCanonicalPath());
        assertEquals(baseDir + File.separator + "sub" + File.separator + "test.txt",
                     new File("sub/./test.txt").getCanonicalPath());
    }


    @Test
    public void testNoWildcardUsed() throws Exception {
        WildcardMatcher m = new WildcardMatcher("test.N1");
        assertEquals(WildcardMatcher.isWindowsOs() ? "test\\.n1" : "test\\.N1", m.getRegex());

        assertTrue(m.matches("test.N1"));

        assertFalse(m.matches("test.jpg"));
        assertFalse(m.matches("rest.N1"));
        assertFalse(m.matches("x/test.N1"));
    }

    @Test
    public void testQuoteInFilename() throws Exception {
        WildcardMatcher m = new WildcardMatcher("te?t.N1");
        assertEquals(WildcardMatcher.isWindowsOs() ? "te.t\\.n1" : "te.t\\.N1", m.getRegex());

        assertTrue(m.matches("test.N1"));
        assertTrue(m.matches("te?t.N1"));

        assertFalse(m.matches("tet.N1"));
    }

    @Test
    public void testStarInFilename() throws Exception {
        WildcardMatcher m = new WildcardMatcher("*.N1");
        assertEquals(WildcardMatcher.isWindowsOs() ? "[^/:]*\\.n1" : "[^/:]*\\.N1", m.getRegex());

        assertTrue(m.matches("test.N1"));
        assertTrue(m.matches("MER_RR.N1"));

        assertFalse(m.matches("MER_RR"));
        assertFalse(m.matches("MER_RR.txt"));
    }

    @Test
    public void testStarInBetween() throws Exception {
        WildcardMatcher m = new WildcardMatcher("foo/*/test.txt");
        assertEquals("foo/[^/:]*/test\\.txt", m.getRegex());

        assertTrue(m.matches("foo//test.txt"));
        assertTrue(m.matches("foo/bar/test.txt"));

        assertFalse(m.matches("/foo/test.txt"));
        assertFalse(m.matches("foo/bar/doz/gna/test.txt"));
    }

    @Test
    public void testStarAtEnd() throws Exception {
        WildcardMatcher m = new WildcardMatcher("foo/*");
        assertEquals("foo/[^/:]*", m.getRegex());

        assertTrue(m.matches("foo/test.txt"));
        assertTrue(m.matches("foo/bar"));

        assertFalse(m.matches("foo"));
        assertFalse(m.matches("foo/bar/test.txt"));
        assertFalse(m.matches("/foo/"));
        assertFalse(m.matches("foo/bar/"));
        assertFalse(m.matches("foo/bar/doz/gna/test.txt"));
    }

    @Test
    public void testDoubleStarInBetween() throws Exception {
        WildcardMatcher m = new WildcardMatcher("foo/**/test.txt");
        assertEquals("foo((/.*/)?|/)test\\.txt", m.getRegex());

        assertTrue(m.matches("foo/test.txt"));
        assertTrue(m.matches("foo/bar/test.txt"));
        assertTrue(m.matches("foo/bar/doz/test.txt"));
        assertTrue(m.matches("foo/bar/doz/gna/test.txt"));

        assertFalse(m.matches("/foo/test.txt"));
        assertFalse(m.matches("foo/bar/doz/gna/test.zip"));
    }

    @Test
    public void testDoubleStarAtEnd() throws Exception {
        WildcardMatcher m = new WildcardMatcher("foo/**");
        assertEquals("foo(/.*)?", m.getRegex());

        assertTrue(m.matches("foo"));
        assertTrue(m.matches("foo/"));
        assertTrue(m.matches("foo/bar/doz/test.txt"));
        assertTrue(m.matches("foo/bar/doz/gna/test.txt"));
        assertTrue(m.matches("foo/test.txt"));
        assertTrue(m.matches("foo/bar/doz/gna/test.zip"));
        if (WildcardMatcher.isWindowsOs()) {
            assertTrue(m.matches("foo\\bar\\doz\\gna\\test.txt"));
        }

        assertFalse(m.matches("/foo/bar/doz/gna/test.zip"));
        assertFalse(m.matches("bar/doz/gna/test.zip"));
    }

    @Test
    public void testGlobWithDoubleStar() throws Exception {
        URL resource = WildcardMatcherTest.class.getResource("");
        String dir = new File(resource.toURI()).getPath();
        File[] files = WildcardMatcher.glob(dir + "/**/*.txt");
        assertNotNull(files);
        for (File file : files) {
            //System.out.println("file = " + file);
        }
        assertEquals(3, files.length);
        Arrays.sort(files);
        assertEquals(new File(dir, "foo/bar/test1.txt"), files[0]);
        assertEquals(new File(dir, "foo/bar/test3.txt"), files[1]);
        assertEquals(new File(dir, "foo/test1.txt"), files[2]);
    }

    @Test
    public void testGlobStarAtEnd() throws Exception {
        URL resource = WildcardMatcherTest.class.getResource("");
        String dir = new File(resource.toURI()).getPath();
        File[] files = WildcardMatcher.glob(dir + "/foo/bar/*");
        assertNotNull(files);
        for (File file : files) {
            //System.out.println("file = " + file);
        }
        assertEquals(3, files.length);
        Arrays.sort(files);
        assertEquals(new File(dir, "foo/bar/test1.txt"), files[0]);
        assertEquals(new File(dir, "foo/bar/test2.dat"), files[1]);
        assertEquals(new File(dir, "foo/bar/test3.txt"), files[2]);
    }

    @Test
    public void testGlobDoubleStarAtEnd() throws Exception {
        URL resource = WildcardMatcherTest.class.getResource("");
        String dir = new File(resource.toURI()).getPath();
        File[] files = WildcardMatcher.glob(dir + "/foo/**");
        assertNotNull(files);
        for (File file : files) {
            //System.out.println("file = " + file);
        }
        assertEquals(4, files.length);
        Arrays.sort(files);
        assertEquals(new File(dir, "foo/bar"), files[0]);
        assertEquals(new File(dir, "foo/test1.txt"), files[1]);
        assertEquals(new File(dir, "foo/test2.dat"), files[2]);
        assertEquals(new File(dir, "foo/test3.dat"), files[3]);
    }

    @Test
    public void testGlobAllFiles() throws Exception {
        URL resource = WildcardMatcherTest.class.getResource("");
        String dir = new File(resource.toURI()).getPath();
        File[] files = WildcardMatcher.glob(dir + "/foo/**/*.*");
        assertNotNull(files);
        for (File file : files) {
            //System.out.println("file = " + file);
        }
        assertEquals(6, files.length);
        Arrays.sort(files);
        assertEquals(new File(dir, "foo/bar/test1.txt"), files[0]);
        assertEquals(new File(dir, "foo/bar/test2.dat"), files[1]);
        assertEquals(new File(dir, "foo/bar/test3.txt"), files[2]);
        assertEquals(new File(dir, "foo/test1.txt"), files[3]);
        assertEquals(new File(dir, "foo/test2.dat"), files[4]);
        assertEquals(new File(dir, "foo/test3.dat"), files[5]);
    }
}
