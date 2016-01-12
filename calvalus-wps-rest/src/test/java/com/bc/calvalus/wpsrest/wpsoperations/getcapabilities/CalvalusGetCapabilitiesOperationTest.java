package com.bc.calvalus.wpsrest.wpsoperations.getcapabilities;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.CalvalusProcessor;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.responses.IWpsProcess;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CalvalusGetCapabilitiesOperation.class, CalvalusFacade.class, CalvalusLogger.class})
public class CalvalusGetCapabilitiesOperationTest {

    private AbstractGetCapabilitiesOperation getCapabilitiesOperation;

    private ServletRequestWrapper mockServletRequestWrapper;
    private WpsMetadata mockWpsMetadata;

    @Before
    public void setUp() throws Exception {
        mockWpsMetadata = mock(WpsMetadata.class);
        mockServletRequestWrapper = mock(ServletRequestWrapper.class);
        when(mockWpsMetadata.getServletRequestWrapper()).thenReturn(mockServletRequestWrapper);

        getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(mockWpsMetadata);
    }

    @Test
    public void canGetCapabilities() throws Exception {
        List<IWpsProcess> mockProcessorList = getMockProcessors();
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        when(mockCalvalusFacade.getProcessors()).thenReturn(mockProcessorList);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusFacade);

        String response = getCapabilitiesOperation.getCapabilities();

        assertThat(response, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                     "<Capabilities service=\"WPS\" xml:lang=\"en\" version=\"1.0.0\" " +
                                     "xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" " +
                                     "xmlns:ns1=\"http://www.opengis.net/ows/1.1\" " +
                                     "xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" " +
                                     "xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
                                     "    <ServiceIdentification>\n" +
                                     "        <ns1:Title>Calvalus WPS server</ns1:Title>\n" +
                                     "        <ns1:Abstract>Web Processing Service for Calvalus</ns1:Abstract>\n" +
                                     "        <ServiceType>WPS</ServiceType>\n" +
                                     "        <ServiceTypeVersion>1.0.0</ServiceTypeVersion>\n" +
                                     "    </ServiceIdentification>\n" +
                                     "    <ServiceProvider>\n" +
                                     "        <ProviderName>Brockmann-Consult</ProviderName>\n" +
                                     "        <ProviderSite ns3:href=\"http://www.brockmann-consult.de\"/>\n" +
                                     "        <ServiceContact>\n" +
                                     "            <IndividualName>Dr. Martin Boettcher</IndividualName>\n" +
                                     "            <PositionName>Project Manager</PositionName>\n" +
                                     "            <ContactInfo>\n" +
                                     "                <Phone>\n" +
                                     "                    <Voice>+49 4152 889300</Voice>\n" +
                                     "                    <Facsimile>+49 4152 889333</Facsimile>\n" +
                                     "                </Phone>\n" +
                                     "                <Address>\n" +
                                     "                    <DeliveryPoint>Max-Planck-Str. 2</DeliveryPoint>\n" +
                                     "                    <City>Geesthacht</City>\n" +
                                     "                    <AdministrativeArea>SH</AdministrativeArea>\n" +
                                     "                    <PostalCode>21502</PostalCode>\n" +
                                     "                    <Country>Germany</Country>\n" +
                                     "                    <ElectronicMailAddress>info@brockmann-consult.de</ElectronicMailAddress>\n" +
                                     "                </Address>\n" +
                                     "                <OnlineResource ns3:href=\"http://www.brockmann-consult.de\"/>\n" +
                                     "                <HoursOfService>24x7</HoursOfService>\n" +
                                     "                <ContactInstructions>Don't hesitate to call</ContactInstructions>\n" +
                                     "            </ContactInfo>\n" +
                                     "            <Role>PointOfContact</Role>\n" +
                                     "        </ServiceContact>\n" +
                                     "    </ServiceProvider>\n" +
                                     "    <OperationsMetadata>\n" +
                                     "        <Operation name=\"GetCapabilities\"/>\n" +
                                     "        <Operation name=\"DescribeProcess\"/>\n" +
                                     "        <Operation name=\"Execute\"/>\n" +
                                     "        <Operation name=\"GetStatus\"/>\n" +
                                     "    </OperationsMetadata>\n" +
                                     "    <ProcessOfferings>\n" +
                                     "        <Processor>\n" +
                                     "            <ns1:Identifier>beam-idepix~2.0.9~Idepix.Water</ns1:Identifier>\n" +
                                     "            <ns1:Title>Idepix (Water Pixel Classification)</ns1:Title>\n" +
                                     "            <ns1:Abstract>Idepix (Water Pixel Classification) Description</ns1:Abstract>\n" +
                                     "        </Processor>\n" +
                                     "    </ProcessOfferings>\n" +
                                     "    <Languages>\n" +
                                     "        <Default>\n" +
                                     "            <ns1:Language>EN</ns1:Language>\n" +
                                     "        </Default>\n" +
                                     "        <Supported>\n" +
                                     "            <ns1:Language>EN</ns1:Language>\n" +
                                     "        </Supported>\n" +
                                     "    </Languages>\n" +
                                     "</Capabilities>\n"));
    }

    @Test
    public void canCatchProductionException() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenThrow(new ProductionException("production exception"));

        String response = getCapabilitiesOperation.getCapabilities();

        assertThat(response, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                     "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                     "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
                                     "        <ExceptionText>Unable to retrieve available processors : production exception</ExceptionText>\n" +
                                     "    </Exception>\n" +
                                     "</ExceptionReport>\n"));
    }

    @Test
    public void canCatchIOException() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenThrow(new IOException("IO exception"));

        String response = getCapabilitiesOperation.getCapabilities();

        assertThat(response, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                     "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                     "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
                                     "        <ExceptionText>Unable to retrieve available processors : IO exception</ExceptionText>\n" +
                                     "    </Exception>\n" +
                                     "</ExceptionReport>\n"));
    }

    @Test
    public void canLogIOException() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenThrow(new IOException("IO exception"));
        Logger mockLogger = mock(Logger.class);
        PowerMockito.mockStatic(CalvalusLogger.class);
        PowerMockito.when(CalvalusLogger.getLogger()).thenReturn(mockLogger);

        ArgumentCaptor<String> errorMessage = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Level> errorLevel = ArgumentCaptor.forClass(Level.class);

        getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(mockWpsMetadata);
        getCapabilitiesOperation.getCapabilities();

        verifyStatic(times(1));
        CalvalusLogger.getLogger();

        verify(mockLogger).log(errorLevel.capture(), errorMessage.capture(), any(Throwable.class));
        assertThat(errorLevel.getValue(), equalTo(Level.SEVERE));
        assertThat(errorMessage.getValue(), equalTo("Unable to create a response to a GetCapabilities request."));
    }

    @Test
    public void canCatchJAXBException() throws Exception {
        JaxbHelper mockJaxbHelper = mock(JaxbHelper.class);
        when(mockJaxbHelper.marshal(anyObject(), any(StringWriter.class))).thenThrow(new JAXBException("JAXB exception")).thenCallRealMethod();
        PowerMockito.whenNew(JaxbHelper.class).withNoArguments().thenReturn(mockJaxbHelper);

        List<IWpsProcess> mockProcessorList = getMockProcessors();
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        when(mockCalvalusFacade.getProcessors()).thenReturn(mockProcessorList);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusFacade);

        String response = getCapabilitiesOperation.getCapabilities();

        assertThat(response, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                     "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                     "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
                                     "        <ExceptionText>JAXB exception</ExceptionText>\n" +
                                     "    </Exception>\n" +
                                     "</ExceptionReport>\n"));
    }

    @Test
    public void canCatchJAXBExceptionWhenMarshallingExceptionReport() throws Exception {
        JaxbHelper mockJaxbHelper = mock(JaxbHelper.class);
        when(mockJaxbHelper.marshal(anyObject(), any(StringWriter.class))).thenThrow(new JAXBException("JAXB exception"));
        PowerMockito.whenNew(JaxbHelper.class).withNoArguments().thenReturn(mockJaxbHelper);

        List<IWpsProcess> mockProcessorList = getMockProcessors();
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        when(mockCalvalusFacade.getProcessors()).thenReturn(mockProcessorList);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusFacade);

        String response = getCapabilitiesOperation.getCapabilities();

        assertThat(response, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                     "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                     "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
                                     "        <ExceptionText>Unable to generate the exception XML : JAXB Exception.</ExceptionText>\n" +
                                     "    </Exception>\n" +
                                     "</ExceptionReport>\n"));
    }

    private List<IWpsProcess> getMockProcessors() {
        List<IWpsProcess> mockProcessorList = new ArrayList<>();
        CalvalusProcessor mockCalvalusProcessor = mock(CalvalusProcessor.class);
        when(mockCalvalusProcessor.getIdentifier()).thenReturn("beam-idepix~2.0.9~Idepix.Water");
        when(mockCalvalusProcessor.getTitle()).thenReturn("Idepix (Water Pixel Classification)");
        when(mockCalvalusProcessor.getAbstractText()).thenReturn("Idepix (Water Pixel Classification) Description");
        mockProcessorList.add(mockCalvalusProcessor);

        return mockProcessorList;
    }
}