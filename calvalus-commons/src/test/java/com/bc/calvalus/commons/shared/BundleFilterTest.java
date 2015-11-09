package com.bc.calvalus.commons.shared;

import org.junit.Test;

import static org.junit.Assert.*;

public class BundleFilterTest {

    @Test
    public void testToString() throws Exception {
        final BundleFilter filter = new BundleFilter();
        assertNotNull(filter.withProvider(BundleFilter.PROVIDER_ALL_USERS));
        assertNotNull(filter.withProvider(BundleFilter.PROVIDER_USER));
        assertNotNull(filter.withTheBundle("testname", "9.9"));
        assertNotNull(filter.withTheProcessor("proc", "1.7-SNAPSHOT"));
        assertNotNull(filter.withTheUser("Marco"));


        assertEquals("provider=ALL_USER,USER;bundle=testname,9.9;processor=proc,1.7-SNAPSHOT;user=Marco", filter.toString());
        assertTrue(filter.isProviderSupported(BundleFilter.PROVIDER_ALL_USERS));
        assertTrue(filter.isProviderSupported(BundleFilter.PROVIDER_USER));
        assertFalse(filter.isProviderSupported(BundleFilter.PROVIDER_SYSTEM));
        assertEquals("testname", filter.getBundleName());
        assertEquals("9.9", filter.getBundleVersion());
        assertEquals("proc", filter.getProcessorName());
        assertEquals("1.7-SNAPSHOT", filter.getProcessorVersion());
        assertEquals("Marco", filter.getUserName());

    }

    @Test
    public void testToStringNotAllValuesSet() throws Exception {
        final BundleFilter filter = new BundleFilter();
        assertNotNull(filter.withProvider(BundleFilter.PROVIDER_USER));
        assertNotNull(filter.withTheProcessor("c2r", "1.0"));
        assertEquals("provider=USER;processor=c2r,1.0", filter.toString());
    }

    @Test
    public void testFromString() throws Exception {
        final BundleFilter filter = BundleFilter.fromString("provider=SYSTEM,USER;bundle=coastcolour,1.6;processor=idepix,1.8-cc;user=Hugo");

        assertTrue(filter.isProviderSupported(BundleFilter.PROVIDER_SYSTEM));
        assertTrue(filter.isProviderSupported(BundleFilter.PROVIDER_USER));
        assertFalse(filter.isProviderSupported(BundleFilter.PROVIDER_ALL_USERS));

        assertEquals("coastcolour", filter.getBundleName());
        assertEquals("1.6", filter.getBundleVersion());
        assertEquals("idepix", filter.getProcessorName());
        assertEquals("1.8-cc", filter.getProcessorVersion());
        assertEquals("Hugo", filter.getUserName());

    }
}
