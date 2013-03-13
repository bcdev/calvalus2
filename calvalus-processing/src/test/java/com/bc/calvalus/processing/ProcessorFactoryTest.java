package com.bc.calvalus.processing;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.*;


public class ProcessorFactoryTest {

    @Test
    public void testIsArchive() throws Exception {
        assertTrue(ProcessorFactory.isArchive(new Path("myarchie.tgz")));
        assertTrue(ProcessorFactory.isArchive(new Path("myarchie.zip")));
        assertTrue(ProcessorFactory.isArchive(new Path("myarchie.tar.gz")));
        assertTrue(ProcessorFactory.isArchive(new Path("myarchie.tar")));
        assertFalse(ProcessorFactory.isArchive(new Path("myarchie.TAR")));
        assertFalse(ProcessorFactory.isArchive(new Path("myarchie.sh")));
    }

    @Test
    public void testStripArchiveExtension() throws Exception {
        assertEquals("myarchie", ProcessorFactory.stripArchiveExtension("myarchie.tgz"));
        assertEquals("myarchie", ProcessorFactory.stripArchiveExtension("myarchie.zip"));
        assertEquals("myarchie", ProcessorFactory.stripArchiveExtension("myarchie.tar.gz"));
        assertEquals("myarchie", ProcessorFactory.stripArchiveExtension("myarchie.tar"));
        assertEquals(null, ProcessorFactory.stripArchiveExtension("myarchie.sh"));
    }
}
