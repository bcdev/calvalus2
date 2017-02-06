package com.bc.calvalus.wps;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.exceptions.WpsRuntimeException;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;
import org.junit.runner.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CalvalusWpsSpi.class, PropertiesWrapper.class})
public class CalvalusWpsSpiTest {

    private WpsServerContext mockWpsServerContext;

    private CalvalusWpsSpi calvalusWpsSpi;

    @Before
    public void setUp() throws Exception {
        calvalusWpsSpi = new CalvalusWpsSpi();
        mockWpsServerContext = mock(WpsServerContext.class);
    }

    @Test
    public void canGetId() throws Exception {
        assertThat(calvalusWpsSpi.getId(), equalTo("calvalus"));
    }

    @Test
    public void canGetName() throws Exception {
        assertThat(calvalusWpsSpi.getName(), equalTo("Calvalus WPS Server"));
    }

    @Test
    public void canGetDescription() throws Exception {
        assertThat(calvalusWpsSpi.getDescription(), equalTo("This is a Calvalus WPS implementation"));
    }

    @Test
    public void canCreateServiceInstance() throws Exception {
        PowerMockito.mockStatic(PropertiesWrapper.class);
        assertThat(calvalusWpsSpi.createServiceInstance(mockWpsServerContext), instanceOf(CalvalusWpsProvider.class));
    }

    @Test(expected = WpsRuntimeException.class)
    public void canThrowIOExceptionWhenCreateServiceInstanceNoPropertiesFile() throws Exception {
        assertThat(calvalusWpsSpi.createServiceInstance(mockWpsServerContext), instanceOf(CalvalusWpsProvider.class));
    }

}