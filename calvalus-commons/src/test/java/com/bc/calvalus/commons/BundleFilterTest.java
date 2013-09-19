package com.bc.calvalus.commons;

import org.junit.Test;

import static org.junit.Assert.*;

public class BundleFilterTest {

    @Test
    public void testToString() throws Exception {
        final BundleFilter filter = new BundleFilter();
        assertNotNull(filter.withProvider(BundleFilter.Provider.ALL_USERS));
        assertNotNull(filter.withTheBundle("testname", "9.9"));
        assertNotNull(filter.withTheProcessor("proc", "1.7-SNAPSHOT"));
        assertNotNull(filter.withTheUser("Marco"));

        assertTrue(filter.isProviderSupported(BundleFilter.Provider.ALL_USERS));
        assertFalse(filter.isProviderSupported(BundleFilter.Provider.SYSTEM));
        assertEquals("testname", filter.getBundleName());
        assertEquals("9.9", filter.getBundleVersion());
        assertEquals("proc", filter.getProcessorName());
        assertEquals("1.7-SNAPSHOT", filter.getProcessorVersion());
        assertEquals("Marco", filter.getUserName());

    }

    @Test
    public void testFromString() throws Exception {
        final BundleFilter filter = BundleFilter.fromString("provider=SYSTEM,USER;bundle=coastcolour,1.6;processor=idepix,1.8-cc;user=Hugo");

        assertTrue(filter.isProviderSupported(BundleFilter.Provider.SYSTEM));
        assertTrue(filter.isProviderSupported(BundleFilter.Provider.USER));
        assertFalse(filter.isProviderSupported(BundleFilter.Provider.ALL_USERS));

        assertEquals("coastcolour", filter.getBundleName());
        assertEquals("1.6", filter.getBundleVersion());
        assertEquals("idepix", filter.getProcessorName());
        assertEquals("1.8-cc", filter.getProcessorVersion());
        assertEquals("Hugo", filter.getUserName());

    }
}
