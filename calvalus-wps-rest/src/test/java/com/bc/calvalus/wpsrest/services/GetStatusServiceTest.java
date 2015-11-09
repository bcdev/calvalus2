package com.bc.calvalus.wpsrest.services;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.junit.runner.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({GetStatusService.class, CalvalusHelper.class, JaxbHelper.class})
public class GetStatusServiceTest {

    private static final String JOB_ID = "dummyJobId";
    public static final float PROGRESS = 0.25f;

    /**
     * Class under test.
     */
    private GetStatusService getStatusService;

    private CalvalusHelper mockCalvalusHelper;
    private ProductionService mockProductionService;
    private Production mockProduction;
    private ServletRequestWrapper mockServletRequestWrapper;

    @Before
    public void setUp() throws Exception {
        mockCalvalusHelper = mock(CalvalusHelper.class);
        mockProductionService = mock(ProductionService.class);
        mockProduction = mock(Production.class);
        mockServletRequestWrapper = mock(ServletRequestWrapper.class);

        getStatusService = new GetStatusService();
    }

    @Test
    public void canReturnExecuteSuccessfulResponse() throws Exception {
        List<String> mockProductResultUrls = getMockProductionResultUrls();

        ProcessStatus mockProcessStatus = getMockProcessStatus(ProcessState.COMPLETED);
        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatus);

        when(mockProductionService.getProduction(JOB_ID)).thenReturn(mockProduction);
        when(mockCalvalusHelper.getProductResultUrls(mockProduction)).thenReturn(mockProductResultUrls);
        when(mockCalvalusHelper.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusHelper);

        String getStatusResponse = getStatusService.getStatus(mockServletRequestWrapper, JOB_ID);

        assertThat(getStatusResponse, containsString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                     "<ExecuteResponse service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" " +
                                                     "xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" " +
                                                     "xmlns:ns1=\"http://www.opengis.net/ows/1.1\" " +
                                                     "xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\"" +
                                                     " xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
                                                     "    <Status creationTime=")); // Status creationTime is dynamically generated, so it is not checked here.
        assertThat(getStatusResponse, containsString("        <ProcessSucceeded>The request has been processed successfully.</ProcessSucceeded>\n" +
                                                     "    </Status>\n" +
                                                     "    <ProcessOutputs>\n" +
                                                     "        <Output>\n" +
                                                     "            <ns1:Identifier>productionResults</ns1:Identifier>\n" +
                                                     "            <Reference href=\"productionResultUrl1\" mimeType=\"binary\"/>\n" +
                                                     "        </Output>\n" +
                                                     "        <Output>\n" +
                                                     "            <ns1:Identifier>productionResults</ns1:Identifier>\n" +
                                                     "            <Reference href=\"productionResultUrl2\" mimeType=\"binary\"/>\n" +
                                                     "        </Output>\n" +
                                                     "        <Output>\n" +
                                                     "            <ns1:Identifier>productionResults</ns1:Identifier>\n" +
                                                     "            <Reference href=\"productionResultUrl3\" mimeType=\"binary\"/>\n" +
                                                     "        </Output>\n" +
                                                     "    </ProcessOutputs>\n" +
                                                     "</ExecuteResponse>\n"));
    }

    @Test
    public void canReturnExecuteInProgressResponse() throws Exception {
        configureProcessRunningMockings();

        String getStatusResponse = getStatusService.getStatus(mockServletRequestWrapper, JOB_ID);

        assertThat(getStatusResponse, CoreMatchers.containsString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                                  "<ExecuteResponse service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" " +
                                                                  "xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" " +
                                                                  "xmlns:ns1=\"http://www.opengis.net/ows/1.1\" " +
                                                                  "xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" " +
                                                                  "xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
                                                                  "    <Status creationTime="));// Status creationTime is dynamically generated, so it is not checked here.
        assertThat(getStatusResponse, CoreMatchers.containsString("        <ProcessStarted percentCompleted=\"25\">RUNNING</ProcessStarted>\n" +
                                                                  "    </Status>\n" +
                                                                  "</ExecuteResponse>\n"));
    }

    @Test
    public void canReturnExecuteFailedResponse() throws Exception {
        ProcessStatus mockProcessStatusUnknown = getMockProcessStatus(ProcessState.UNKNOWN);
        ProcessStatus mockProcessStatusError = getMockProcessStatus(ProcessState.ERROR);

        when(mockProcessStatusError.getMessage()).thenReturn("Error message here");
        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatusUnknown);
        when(mockProduction.getProcessingStatus()).thenReturn(mockProcessStatusError);
        when(mockProductionService.getProduction(JOB_ID)).thenReturn(mockProduction);
        when(mockCalvalusHelper.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusHelper);

        String getStatusResponse = getStatusService.getStatus(mockServletRequestWrapper, JOB_ID);

        assertThat(getStatusResponse, containsString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                     "<ExecuteResponse service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" " +
                                                     "xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" " +
                                                     "xmlns:ns1=\"http://www.opengis.net/ows/1.1\" " +
                                                     "xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" " +
                                                     "xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
                                                     "    <Status creationTime="));// Status creationTime is dynamically generated, so it is not checked here.
        assertThat(getStatusResponse, containsString("        <ProcessFailed>\n" +
                                                     "            <ns1:ExceptionReport version=\"1\">\n" +
                                                     "                <Exception>\n" +
                                                     "                    <ExceptionText>Error message here</ExceptionText>\n" +
                                                     "                </Exception>\n" +
                                                     "            </ns1:ExceptionReport>\n" +
                                                     "        </ProcessFailed>\n" +
                                                     "    </Status>\n" +
                                                     "</ExecuteResponse>\n"));

    }

    @Test
    public void canCatchProductionException() throws Exception {
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenThrow(new ProductionException("production exception"));

        String getStatusResponse = getStatusService.getStatus(mockServletRequestWrapper, JOB_ID);

        assertThat(getStatusResponse, containsString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                     "<ExecuteResponse service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
                                                     "    <Status creationTime="));
        assertThat(getStatusResponse, containsString("        <ProcessFailed>\n" +
                                                     "            <ns1:ExceptionReport version=\"1\">\n" +
                                                     "                <Exception>\n" +
                                                     "                    <ExceptionText>production exception</ExceptionText>\n" +
                                                     "                </Exception>\n" +
                                                     "            </ns1:ExceptionReport>\n" +
                                                     "        </ProcessFailed>\n" +
                                                     "    </Status>\n" +
                                                     "</ExecuteResponse>"));

    }

    @Test
    public void canCatchIOException() throws Exception {
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenThrow(new IOException("IO exception"));

        String getStatusResponse = getStatusService.getStatus(mockServletRequestWrapper, JOB_ID);

        assertThat(getStatusResponse, containsString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                     "<ExecuteResponse service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
                                                     "    <Status creationTime="));
        assertThat(getStatusResponse, containsString("        <ProcessFailed>\n" +
                                                     "            <ns1:ExceptionReport version=\"1\">\n" +
                                                     "                <Exception>\n" +
                                                     "                    <ExceptionText>IO exception</ExceptionText>\n" +
                                                     "                </Exception>\n" +
                                                     "            </ns1:ExceptionReport>\n" +
                                                     "        </ProcessFailed>\n" +
                                                     "    </Status>\n" +
                                                     "</ExecuteResponse>"));

    }


    @Test
    public void canCatchJAXBException() throws Exception {
        JaxbHelper mockJaxbHelper = mock(JaxbHelper.class);
        when(mockJaxbHelper.marshal(anyObject(), any(StringWriter.class))).thenThrow(new JAXBException("JAXB exception")).thenCallRealMethod();
        PowerMockito.whenNew(JaxbHelper.class).withNoArguments().thenReturn(mockJaxbHelper);

        configureProcessRunningMockings();

        String getStatusResponse = getStatusService.getStatus(mockServletRequestWrapper, JOB_ID);

        assertThat(getStatusResponse, containsString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                     "<ExecuteResponse service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
                                                     "    <Status creationTime="));
        assertThat(getStatusResponse, containsString("        <ProcessFailed>\n" +
                                                     "            <ns1:ExceptionReport version=\"1\">\n" +
                                                     "                <Exception>\n" +
                                                     "                    <ExceptionText>JAXB exception</ExceptionText>\n" +
                                                     "                </Exception>\n" +
                                                     "            </ns1:ExceptionReport>\n" +
                                                     "        </ProcessFailed>\n" +
                                                     "    </Status>\n" +
                                                     "</ExecuteResponse>"));
    }

    @Test
    public void canCatchJAXBExceptionWhenCreatingExceptionXml() throws Exception {
        JaxbHelper mockJaxbHelper = mock(JaxbHelper.class);
        when(mockJaxbHelper.marshal(anyObject(), any(StringWriter.class))).thenThrow(new JAXBException("JAXB exception"));
        PowerMockito.whenNew(JaxbHelper.class).withNoArguments().thenReturn(mockJaxbHelper);

        configureProcessRunningMockings();

        String getStatusResponse = getStatusService.getStatus(mockServletRequestWrapper, JOB_ID);

        assertThat(getStatusResponse, containsString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                     "<ExecuteResponse service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
                                                     "    <Status creationTime="));
        assertThat(getStatusResponse, containsString("        <ProcessFailed>\n" +
                                                     "            <ns1:ExceptionReport version=\"1\">\n" +
                                                     "                <Exception>\n" +
                                                     "                    <ExceptionText>Unable to generate the requested status : JAXB Exception.</ExceptionText>\n" +
                                                     "                </Exception>\n" +
                                                     "            </ns1:ExceptionReport>\n" +
                                                     "        </ProcessFailed>\n" +
                                                     "    </Status>\n" +
                                                     "</ExecuteResponse>"));
    }

    private void configureProcessRunningMockings() throws Exception {
        ProcessStatus mockProcessStatusUnknown = getMockProcessStatus(ProcessState.UNKNOWN);
        ProcessStatus mockProcessStatusRunning = getMockProcessStatus(ProcessState.RUNNING);
        when(mockProcessStatusRunning.getProgress()).thenReturn(PROGRESS);

        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatusUnknown);
        when(mockProduction.getProcessingStatus()).thenReturn(mockProcessStatusRunning);
        when(mockProductionService.getProduction(JOB_ID)).thenReturn(mockProduction);
        when(mockCalvalusHelper.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusHelper);
    }

    private List<String> getMockProductionResultUrls() {
        List<String> mockProductResultUrls = new ArrayList<>();
        mockProductResultUrls.add("productionResultUrl1");
        mockProductResultUrls.add("productionResultUrl2");
        mockProductResultUrls.add("productionResultUrl3");
        return mockProductResultUrls;
    }

    private ProcessStatus getMockProcessStatus(ProcessState completed) {
        ProcessStatus mockProcessStatus = mock(ProcessStatus.class);
        when(mockProcessStatus.getState()).thenReturn(completed);
        return mockProcessStatus;
    }
}