package com.bc.calvalus.wpsrest.services;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.exception.WpsRuntimeException;
import org.junit.*;

/**
 * @author hans
 */
public class WpsServiceFactoryTest {

    private WpsServiceFactory wpsServiceFactory;

    @Before
    public void setUp() throws Exception {
        ServletRequestWrapper mockServletRequestWrapper = mock(ServletRequestWrapper.class);

        wpsServiceFactory = new WpsServiceFactory(mockServletRequestWrapper);
    }

    @Test
    public void testGetCalvalusWpsService() throws Exception {
        WpsServiceProvider wpsService = wpsServiceFactory.getWpsService("calvalus");

        assertThat(wpsService, instanceOf(CalvalusWpsService.class));
    }

    @Test (expected = WpsRuntimeException.class)
    public void testGetInvalidWpsService() throws Exception {
        wpsServiceFactory.getWpsService("invalid");
    }
}