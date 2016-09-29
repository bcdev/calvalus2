package com.bc.calvalus.wps;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wps.calvalusfacade.CalvalusProductionService;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.wpsoperations.CalvalusDescribeProcessOperation;
import com.bc.calvalus.wps.wpsoperations.CalvalusExecuteOperation;
import com.bc.calvalus.wps.wpsoperations.CalvalusGetCapabilitiesOperation;
import com.bc.calvalus.wps.wpsoperations.CalvalusGetStatusOperation;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.exceptions.WpsServiceException;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Timer;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
            CalvalusWpsProvider.class, CalvalusProductionService.class, PropertiesWrapper.class,
            CalvalusGetCapabilitiesOperation.class, CalvalusDescribeProcessOperation.class,
            CalvalusExecuteOperation.class
})
public class CalvalusWpsProviderTest {

    private static final String DUMMY_ID = "dummyId";
    private WpsRequestContext mockRequestContext;

    private CalvalusWpsProvider calvalusProvider;

    @Before
    public void setUp() throws Exception {
        calvalusProvider = new CalvalusWpsProvider();
        mockRequestContext = mock(WpsRequestContext.class);
    }

    @Test
    public void canGetCapabilities() throws Exception {
        CalvalusGetCapabilitiesOperation mockGetCapabilities = mock(CalvalusGetCapabilitiesOperation.class);
        PowerMockito.whenNew(CalvalusGetCapabilitiesOperation.class).withArguments(mockRequestContext).thenReturn(mockGetCapabilities);

        calvalusProvider.getCapabilities(mockRequestContext);

        verify(mockGetCapabilities, times(1)).getCapabilities();
    }

    @Test(expected = WpsServiceException.class)
    public void canThrowExceptionGetCapabilities() throws Exception {
        CalvalusGetCapabilitiesOperation mockGetCapabilities = mock(CalvalusGetCapabilitiesOperation.class);
        when(mockGetCapabilities.getCapabilities()).thenThrow(new WpsProcessorNotFoundException("process not available"));
        PowerMockito.whenNew(CalvalusGetCapabilitiesOperation.class).withArguments(mockRequestContext).thenReturn(mockGetCapabilities);

        calvalusProvider.getCapabilities(mockRequestContext);
    }

    @Test
    public void canDescribeProcess() throws Exception {
        CalvalusDescribeProcessOperation mockDescribeProcess = mock(CalvalusDescribeProcessOperation.class);
        PowerMockito.whenNew(CalvalusDescribeProcessOperation.class).withArguments(mockRequestContext).thenReturn(mockDescribeProcess);
        ArgumentCaptor<String> jobId = ArgumentCaptor.forClass(String.class);

        calvalusProvider.describeProcess(mockRequestContext, DUMMY_ID);

        verify(mockDescribeProcess, times(1)).getProcesses(jobId.capture());

        assertThat(jobId.getValue(), equalTo("dummyId"));
    }

    @Test(expected = WpsServiceException.class)
    public void canThrowExceptionDescribeProcess() throws Exception {
        CalvalusDescribeProcessOperation mockDescribeProcess = mock(CalvalusDescribeProcessOperation.class);
        when(mockDescribeProcess.getProcesses(DUMMY_ID)).thenThrow(new WpsProcessorNotFoundException("process not available"));
        PowerMockito.whenNew(CalvalusDescribeProcessOperation.class).withArguments(mockRequestContext).thenReturn(mockDescribeProcess);

        calvalusProvider.describeProcess(mockRequestContext, DUMMY_ID);
    }

    @Test
    public void canDoExecute() throws Exception {
        CalvalusExecuteOperation mockExecute = mock(CalvalusExecuteOperation.class);
        Execute mockExecuteRequest = mock(Execute.class);
        PowerMockito.whenNew(CalvalusExecuteOperation.class).withArguments(mockRequestContext).thenReturn(mockExecute);

        calvalusProvider.doExecute(mockRequestContext, mockExecuteRequest);

        verify(mockExecute, times(1)).execute(mockExecuteRequest);
    }

    @Test(expected = WpsServiceException.class)
    public void canThrowExceptionDoExecute() throws Exception {
        Execute mockExecuteRequest = mock(Execute.class);
        CalvalusExecuteOperation mockExecute = mock(CalvalusExecuteOperation.class);
        when(mockExecute.execute(mockExecuteRequest)).thenThrow(new IOException());
        PowerMockito.whenNew(CalvalusExecuteOperation.class).withArguments(mockRequestContext).thenReturn(mockExecute);

        calvalusProvider.doExecute(mockRequestContext, mockExecuteRequest);
    }

    @Test
    public void canGetStatus() throws Exception {
        CalvalusGetStatusOperation mockGetStatus = mock(CalvalusGetStatusOperation.class);
        PowerMockito.whenNew(CalvalusGetStatusOperation.class).withArguments(mockRequestContext).thenReturn(mockGetStatus);
        ArgumentCaptor<String> jobId = ArgumentCaptor.forClass(String.class);

        calvalusProvider.getStatus(mockRequestContext, DUMMY_ID);

        verify(mockGetStatus, times(1)).getStatus(jobId.capture());

        assertThat(jobId.getValue(), equalTo("dummyId"));
    }

    @Test(expected = WpsServiceException.class)
    public void canCatchIOExceptionWhenCreatingGetStatus() throws Exception {
        PowerMockito.whenNew(CalvalusGetStatusOperation.class).withArguments(mockRequestContext).thenThrow(new IOException());

        calvalusProvider.getStatus(mockRequestContext, DUMMY_ID);
    }

    @Test
    public void canDispose() throws Exception {
        PowerMockito.mockStatic(PropertiesWrapper.class);
        PowerMockito.mockStatic(CalvalusProductionService.class);
        Timer mockTimer = mock(Timer.class);
        PowerMockito.when(CalvalusProductionService.getStatusObserverSingleton()).thenReturn(mockTimer);

        calvalusProvider.dispose();

        verify(mockTimer, times(1)).cancel();
    }

}