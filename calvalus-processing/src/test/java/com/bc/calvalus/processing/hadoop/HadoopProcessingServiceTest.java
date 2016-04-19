package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HadoopProcessingServiceTest {

    @Test
    public void testCalculateProgress() {
        assertEquals((9 * 0.8f + 0.1f) / 10, calcProgress(0.8f, 0.1f, true), 1e-5f);
        assertEquals(0.8f, calcProgress(0.8f, 0.0f, false), 1e-5f);
        assertEquals(1f, calcProgress(1f, 1f, true), 1e-5f);
    }

    private static float calcProgress(float mpaProgress, float  reuceProgress, boolean hasReducer) {
        JobID jobID1 = new JobID("34627598547", 6);
        JobStatus jobStatus = new JobStatus(jobID1, mpaProgress, reuceProgress, JobStatus.SUCCEEDED);
        return HadoopProcessingService.calculateProgress(jobStatus, hasReducer);
    }

    @Test
    public void testIsArchive() throws Exception {
        assertTrue(HadoopProcessingService.isArchive(new Path("myarchie.tgz")));
        assertTrue(HadoopProcessingService.isArchive(new Path("myarchie.zip")));
        assertTrue(HadoopProcessingService.isArchive(new Path("myarchie.tar.gz")));
        assertTrue(HadoopProcessingService.isArchive(new Path("myarchie.tar")));
        assertFalse(HadoopProcessingService.isArchive(new Path("myarchie.TAR")));
        assertFalse(HadoopProcessingService.isArchive(new Path("myarchie.sh")));
    }

    @Test
    public void testStripArchiveExtension() throws Exception {
        assertEquals("myarchie", HadoopProcessingService.stripArchiveExtension("myarchie.tgz"));
        assertEquals("myarchie", HadoopProcessingService.stripArchiveExtension("myarchie.zip"));
        assertEquals("myarchie", HadoopProcessingService.stripArchiveExtension("myarchie.tar.gz"));
        assertEquals("myarchie", HadoopProcessingService.stripArchiveExtension("myarchie.tar"));
        assertEquals(null, HadoopProcessingService.stripArchiveExtension("myarchie.sh"));
    }

}
