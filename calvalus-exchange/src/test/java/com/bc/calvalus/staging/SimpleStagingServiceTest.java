package com.bc.calvalus.staging;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author MarcoZ
 * @author Norman
 */
public class SimpleStagingServiceTest {

    @Test
    public void testGetStagingAreaPath() throws IOException {
        SimpleStagingService stagingService = new SimpleStagingService(new File("/foo/bar/buz"), 1);
        assertEquals(new File("/foo/bar/buz").getAbsoluteFile(),
                     stagingService.getStagingDir());
    }

    @Test
    public void testSubmitStaging() throws IOException {
        SimpleStagingService stagingService = new SimpleStagingService(new File("/foo/bar/buz"), 1);
        Staging staging = new Staging() {
            @Override
            public void run() {
            }
        };
        stagingService.submitStaging(staging);
    }

    @Test
    public void testDeleteTree() throws IOException {
        SimpleStagingService stagingService = new SimpleStagingService(new File("/foo/bar/buz"), 1);
        stagingService.deleteTree("bas");


        try {
            stagingService.deleteTree("../..");
            fail("IOE expected, because canonical names do not match");
        } catch (IOException e) {
            // ok
        }
    }
}
