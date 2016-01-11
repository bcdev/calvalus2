package com.bc.calvalus.wpsrest.services;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.exception.InvalidRequestException;
import com.bc.calvalus.wpsrest.wpsoperations.describeprocess.CalvalusDescribeProcessOperation;
import com.bc.calvalus.wpsrest.wpsoperations.execute.AbstractExecuteOperation;
import com.bc.calvalus.wpsrest.wpsoperations.execute.CalvalusExecuteOperation;
import com.bc.calvalus.wpsrest.wpsoperations.getcapabilities.CalvalusGetCapabilitiesOperation;
import com.bc.calvalus.wpsrest.wpsoperations.getstatus.CalvalusGetStatusOperation;
import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.Locale;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
            CalvalusWpsService.class, CalvalusDescribeProcessOperation.class,
            WpsServiceFactory.class, WpsService.class, ServletRequestWrapper.class
})
public class WpsServiceTest {

    private WpsService wpsService;

    private HttpServletRequest mockServletRequest;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        Locale.setDefault(Locale.ENGLISH);
        getMockServletRequest();
        wpsService = new WpsService();
    }

    @Test
    public void canGetCapabilitiesWithValidRequest() throws Exception {
        WpsServiceFactory mockWpsServiceFactory = mock(WpsServiceFactory.class);
        WpsServiceProvider mockWpsServiceProvider = mock(WpsServiceProvider.class);
        when(mockWpsServiceFactory.getWpsService(anyString())).thenReturn(mockWpsServiceProvider);

        PowerMockito.whenNew(WpsServiceFactory.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockWpsServiceFactory);

        wpsService = new WpsService();
        wpsService.getWpsService("calvalus", "WPS", "GetCapabilities", "", "", "", "", "", mockServletRequest);

        verify(mockWpsServiceProvider).getCapabilities();
    }

    @Test
    public void canReturnExceptionGetCapabilitiesWithInvalidService() throws Exception {
        getMockGetCapabilitiesService("validGetCapabilitiesXmlResponse");

        String wpsResponse = wpsService.getWpsService("calvalus", "invalidService", "GetCapabilities", "", "", "", "", "", mockServletRequest);

        assertThat(wpsResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                        "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                        "    <Exception exceptionCode=\"InvalidParameterValue\" locator=\"invalidService\">\n" +
                                        "        <ExceptionText>Invalid value of parameter 'invalidService'</ExceptionText>\n" +
                                        "    </Exception>\n" +
                                        "</ExceptionReport>\n"));
    }

    @Test
    public void canReturnExceptionWhenDescribeProcessWithNoProcessorId() throws Exception {
        getMockDescribeProcessService("validDescribeProcessXmlResponse");

        String wpsResponse = wpsService.getWpsService("calvalus", "WPS", "DescribeProcess", "", "", "", "1.0.0", "", mockServletRequest);

        assertThat(wpsResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                        "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                        "    <Exception exceptionCode=\"MissingParameterValue\" locator=\"Identifier\">\n" +
                                        "        <ExceptionText>Missing value from parameter : Identifier</ExceptionText>\n" +
                                        "    </Exception>\n" +
                                        "</ExceptionReport>\n"));
    }

    @Test
    public void canReturnExceptionWhenDescribeProcessWithNoVersionNumber() throws Exception {
        getMockDescribeProcessService("validDescribeProcessXmlResponse");

        String wpsResponse = wpsService.getWpsService("calvalus", "WPS", "DescribeProcess", "", "", "bundle~name~version", "", "", mockServletRequest);

        assertThat(wpsResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                        "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                        "    <Exception exceptionCode=\"MissingParameterValue\" locator=\"Version\">\n" +
                                        "        <ExceptionText>Missing value from parameter : Version</ExceptionText>\n" +
                                        "    </Exception>\n" +
                                        "</ExceptionReport>\n"));
    }

    @Test
    public void canGetStatusWithValidRequest() throws Exception {
        getMockGetStatusService("validGetStatusXmlResponse");

        String wpsResponse = wpsService.getWpsService("calvalus", "WPS", "GetStatus", "", "", "", "", "dummyJobId", mockServletRequest);

        assertThat(wpsResponse, equalTo("validGetStatusXmlResponse"));
    }

    @Test
    public void canReturnExceptionWhenGetStatusWithoutJobId() throws Exception {
        String wpsResponse = wpsService.getWpsService("calvalus", "WPS", "GetStatus", "", "", "", "", "", mockServletRequest);

        assertThat(wpsResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                        "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                        "    <Exception exceptionCode=\"MissingParameterValue\" locator=\"JobId\">\n" +
                                        "        <ExceptionText>Missing value from parameter : JobId</ExceptionText>\n" +
                                        "    </Exception>\n" +
                                        "</ExceptionReport>\n"));
    }

    @Test
    public void canReturnExceptionWhenRequestIsUnknown() throws Exception {
        String wpsResponse = wpsService.getWpsService("calvalus", "WPS", "InvalidService", "", "", "", "", "", mockServletRequest);

        assertThat(wpsResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                        "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                        "    <Exception exceptionCode=\"InvalidParameterValue\" locator=\"Request\">\n" +
                                        "        <ExceptionText>Invalid value of parameter 'Request'</ExceptionText>\n" +
                                        "    </Exception>\n" +
                                        "</ExceptionReport>\n"));
    }

    @Test
    public void canExecuteWithValidXmlRequest() throws Exception {
        configureMockExecuteService("validExecuteXmlResponse");

        String wpsResponse = wpsService.postExecuteService("calvalus", getValidExecuteRequest(), mockServletRequest);

        assertThat(wpsResponse, equalTo("validExecuteXmlResponse"));
    }

    @Test
    public void canGetDefaultServiceAndVersionInRequestXml() throws Exception {
        configureMockExecuteService("validExecuteXmlResponse");

        String wpsResponse = wpsService.postExecuteService("calvalus", getExecuteRequestWithoutServiceAndVersion(), mockServletRequest);

        assertThat(wpsResponse, equalTo("validExecuteXmlResponse"));
    }

    @Test
    public void canReturnExceptionWhenNoProcessorId() throws Exception {
        String wpsResponse = wpsService.postExecuteService("calvalus", getExecuteRequestWithoutIdentifier(), mockServletRequest);

        assertThat(wpsResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                        "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                        "    <Exception exceptionCode=\"MissingParameterValue\" locator=\"Identifier\">\n" +
                                        "        <ExceptionText>Missing value from parameter : Identifier</ExceptionText>\n" +
                                        "    </Exception>\n" +
                                        "</ExceptionReport>\n"));
    }

    @Test
    public void canThrowExceptionWhenExecuteWithMalformedXmlRequest() throws Exception {
        configureMockExecuteService("invalidExecuteXmlResponse");

        thrown.expect(InvalidRequestException.class);
        thrown.expectMessage("Invalid Execute request. Content is not allowed in prolog.");
        wpsService.postExecuteService("calvalus", "requestXml", mockServletRequest);
    }

    @Test
    public void canThrowExceptionWhenExecuteWithUnknownXmlRequest() throws Exception {
        configureMockExecuteService("invalidExecuteXmlResponse");

        thrown.expect(InvalidRequestException.class);
        thrown.expectMessage("Invalid Execute request. unexpected element (uri:\"http://java.sun.com/xml/ns/j2ee\", local:\"web-app\"). " +
                             "Expected elements are <{}AllowedValues>,<{}AnyValue>,<{}Capabilities>,<{}DCP>,<{}DescribeProcess>,<{}ExceptionReport>," +
                             "<{}Execute>,<{}ExecuteResponse>,<{}GetCapabilities>,<{}HTTP>,<{}Languages>,<{}NoValues>,<{}Operation>,<{}OperationsMetadata>," +
                             "<{}ProcessDescriptions>,<{}ProcessOfferings>,<{}ServiceIdentification>,<{}ServiceProvider>,<{}ValuesReference>,<{}WSDL>," +
                             "<{http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd}parameters>");
        wpsService.postExecuteService("calvalus", getUnknownXmlRequest(), mockServletRequest);
    }

    @Test
    public void canThrowExceptionWhenExecuteWithMissingElement() throws Exception {
        configureMockExecuteService("invalidExecuteXmlResponse");

        thrown.expect(InvalidRequestException.class);
        thrown.expectMessage("Invalid Execute request. Please see the WPS 1.0.0 guideline for the right Execute request structure.");
        wpsService.postExecuteService("calvalus", getInvalidExecuteRequest(), mockServletRequest);
    }

    @Test
    public void canReturnExceptionWhenNoServiceParameter() throws Exception {
        String wpsResponse = wpsService.getWpsService("calvalus", "", "", "", "", "", "", "", mockServletRequest);

        assertThat(wpsResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                        "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                        "    <Exception exceptionCode=\"MissingParameterValue\" locator=\"Service\">\n" +
                                        "        <ExceptionText>Missing value from parameter : Service</ExceptionText>\n" +
                                        "    </Exception>\n" +
                                        "</ExceptionReport>\n"));
    }

    @Test
    public void canReturnExceptionWhenNoRequestTypeParameter() throws Exception {
        String wpsResponse = wpsService.getWpsService("calvalus", "WPS", "", "", "", "", "", "", mockServletRequest);

        assertThat(wpsResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                        "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                        "    <Exception exceptionCode=\"MissingParameterValue\" locator=\"Request\">\n" +
                                        "        <ExceptionText>Missing value from parameter : Request</ExceptionText>\n" +
                                        "    </Exception>\n</ExceptionReport>\n"));
    }

    private String getValidExecuteRequest() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
               "\n" +
               "<Execute service=\"WPS\"\n" +
               "             version=\"1.0.0\"\n" +
               "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
               "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
               "\t\t\t xmlns:cal= \"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\"\n" +
               "             xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "\n" +
               "        <ows:Identifier>beam-idepix~2.0.9~Idepix.Water</ows:Identifier>\n" +
               "</Execute>";
    }

    private String getExecuteRequestWithoutServiceAndVersion() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
               "\n" +
               "<Execute xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
               "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
               "\t\t\t xmlns:cal= \"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\"\n" +
               "             xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "\n" +
               "        <ows:Identifier>beam-idepix~2.0.9~Idepix.Water</ows:Identifier>\n" +
               "</Execute>";
    }

    private String getExecuteRequestWithoutIdentifier() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
               "\n" +
               "<Execute xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
               "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
               "\t\t\t xmlns:cal= \"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\"\n" +
               "             xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "</Execute>";
    }

    private String getUnknownXmlRequest() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
               "<web-app version=\"2.4\" xmlns=\"http://java.sun.com/xml/ns/j2ee\"\n" +
               "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
               "         xsi:schemaLocation=\"http://java.sun.com/xml/ns/j2ee\n" +
               "http://java.sun.com/xml/ns/j2ee/web-app_2_4.xsd\">\n" +
               "\n" +
               "    <display-name>Calvalus WPS</display-name>\n" +
               "    <description>Calvalus WPS</description>\n" +
               "</web-app>";
    }

    private String getInvalidExecuteRequest() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
               "<ProcessDescriptions service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:bc=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:wps=\"http://www.opengis.net/wps/1.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "\t<ProcessDescription storeSupported=\"true\" statusSupported=\"true\" wps:processVersion=\"2.1.3-SNAPSHOT\">\n" +
               "\t\t<ows:Identifier>beam-idepix~2.1.3-SNAPSHOT~Idepix.Land</ows:Identifier>\n" +
               "\t\t<ows:Title>beam-idepix~2.1.3-SNAPSHOT~Idepix.Land</ows:Title>\n" +
               "\t\t<ows:Abstract>\n" +
               "\t\t</ows:Abstract>\n" +
               "\t\t<DataInputs>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>productionName</ows:Identifier>\n" +
               "\t\t\t\t<ows:Title>Production name</ows:Title>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t</DataInputs>\n" +
               "\t\t<ProcessOutputs>\n" +
               "\t\t\t<Output>\n" +
               "\t\t\t\t<ows:Identifier>productionResults</ows:Identifier>\n" +
               "\t\t\t\t<ows:Title>URL to the production result(s)</ows:Title>\n" +
               "\t\t\t\t<ComplexOutput>\n" +
               "\t\t\t\t</ComplexOutput>\n" +
               "\t\t\t</Output>\n" +
               "\t\t</ProcessOutputs>\n" +
               "\t</ProcessDescription>\n" +
               "</ProcessDescriptions>";
    }


    private void getMockGetCapabilitiesService(String response) throws Exception {
        CalvalusGetCapabilitiesOperation mockGetCapabilitiesOperation = mock(CalvalusGetCapabilitiesOperation.class);
        when(mockGetCapabilitiesOperation.getCapabilities()).thenReturn(response);
        PowerMockito.whenNew(CalvalusGetCapabilitiesOperation.class).withAnyArguments().thenReturn(mockGetCapabilitiesOperation);
    }

    private void getMockDescribeProcessService(String response) throws Exception {
        CalvalusDescribeProcessOperation mockDescribeProcessOperation = mock(CalvalusDescribeProcessOperation.class);
        when(mockDescribeProcessOperation.describeProcess()).thenReturn(response);
        PowerMockito.whenNew(CalvalusDescribeProcessOperation.class).withAnyArguments().thenReturn(mockDescribeProcessOperation);
    }

    private void getMockGetStatusService(String response) throws Exception {
        CalvalusGetStatusOperation mockGetStatusOperation = mock(CalvalusGetStatusOperation.class);
        when(mockGetStatusOperation.getStatus()).thenReturn(response);
        PowerMockito.whenNew(CalvalusGetStatusOperation.class).withAnyArguments().thenReturn(mockGetStatusOperation);
    }

    private void configureMockExecuteService(String response) throws Exception {
        CalvalusExecuteOperation mockExecuteOperation = mock(CalvalusExecuteOperation.class);
        when(mockExecuteOperation.execute()).thenReturn(response);
        PowerMockito.whenNew(CalvalusExecuteOperation.class).withAnyArguments().thenReturn(mockExecuteOperation);
    }

    private void getMockServletRequest() {
        mockServletRequest = mock(HttpServletRequest.class);
        Principal mockPrincipal = mock(Principal.class);
        when(mockPrincipal.getName()).thenReturn("dummyUser");
        when(mockServletRequest.getUserPrincipal()).thenReturn(mockPrincipal);
    }
}