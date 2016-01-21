package com.bc.calvalus.wps.wpsoperations;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.calvalusfacade.CalvalusProcessor;
import com.bc.calvalus.wps.responses.IWpsProcess;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.Capabilities;
import org.junit.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;

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
public class CalvalusGetCapabilitiesOperationTest {

    private CalvalusGetCapabilitiesOperation getCapabilitiesOperation;
    private WpsRequestContext mockRequestContext;

    @Before
    public void setUp() throws Exception {
        mockRequestContext = mock(WpsRequestContext.class);

        getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(mockRequestContext);
    }

    @Test
    public void canGetCapabilities() throws Exception {
        List<IWpsProcess> mockProcessorList = getMockProcessors();
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        when(mockCalvalusFacade.getProcessors()).thenReturn(mockProcessorList);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockRequestContext).thenReturn(mockCalvalusFacade);

        Capabilities capabilities = getCapabilitiesOperation.getCapabilities();

    }

//    @Test
//    public void canCatchProductionException() throws Exception {
//        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenThrow(new ProductionException("production exception"));
//
//        String response = getCapabilitiesOperation.getCapabilities();
//
//        assertThat(response, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
//                                     "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
//                                     "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
//                                     "        <ExceptionText>Unable to retrieve available processors : production exception</ExceptionText>\n" +
//                                     "    </Exception>\n" +
//                                     "</ExceptionReport>\n"));
//    }
//
//    @Test
//    public void canCatchIOException() throws Exception {
//        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenThrow(new IOException("IO exception"));
//
//        String response = getCapabilitiesOperation.getCapabilities();
//
//        assertThat(response, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
//                                     "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
//                                     "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
//                                     "        <ExceptionText>Unable to retrieve available processors : IO exception</ExceptionText>\n" +
//                                     "    </Exception>\n" +
//                                     "</ExceptionReport>\n"));
//    }
//
//    @Test
//    public void canLogIOException() throws Exception {
//        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenThrow(new IOException("IO exception"));
//        Logger mockLogger = mock(Logger.class);
//        PowerMockito.mockStatic(CalvalusLogger.class);
//        PowerMockito.when(CalvalusLogger.getLogger()).thenReturn(mockLogger);
//
//        ArgumentCaptor<String> errorMessage = ArgumentCaptor.forClass(String.class);
//        ArgumentCaptor<Level> errorLevel = ArgumentCaptor.forClass(Level.class);
//
//        getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(mockWpsMetadata);
//        getCapabilitiesOperation.getCapabilities();
//
//        verifyStatic(times(1));
//        CalvalusLogger.getLogger();
//
//        verify(mockLogger).log(errorLevel.capture(), errorMessage.capture(), any(Throwable.class));
//        assertThat(errorLevel.getValue(), equalTo(Level.SEVERE));
//        assertThat(errorMessage.getValue(), equalTo("Unable to create a response to a GetCapabilities request."));
//    }
//
//    @Test
//    public void canCatchJAXBException() throws Exception {
//        JaxbHelper mockJaxbHelper = mock(JaxbHelper.class);
//        when(mockJaxbHelper.marshal(anyObject(), any(StringWriter.class))).thenThrow(new JAXBException("JAXB exception")).thenCallRealMethod();
//        PowerMockito.whenNew(JaxbHelper.class).withNoArguments().thenReturn(mockJaxbHelper);
//
//        List<IWpsProcess> mockProcessorList = getMockProcessors();
//        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
//        when(mockCalvalusFacade.getProcessors()).thenReturn(mockProcessorList);
//        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusFacade);
//
//        String response = getCapabilitiesOperation.getCapabilities();
//
//        assertThat(response, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
//                                     "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
//                                     "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
//                                     "        <ExceptionText>JAXB exception</ExceptionText>\n" +
//                                     "    </Exception>\n" +
//                                     "</ExceptionReport>\n"));
//    }
//
//    @Test
//    public void canCatchJAXBExceptionWhenMarshallingExceptionReport() throws Exception {
//        JaxbHelper mockJaxbHelper = mock(JaxbHelper.class);
//        when(mockJaxbHelper.marshal(anyObject(), any(StringWriter.class))).thenThrow(new JAXBException("JAXB exception"));
//        PowerMockito.whenNew(JaxbHelper.class).withNoArguments().thenReturn(mockJaxbHelper);
//
//        List<IWpsProcess> mockProcessorList = getMockProcessors();
//        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
//        when(mockCalvalusFacade.getProcessors()).thenReturn(mockProcessorList);
//        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusFacade);
//
//        String response = getCapabilitiesOperation.getCapabilities();
//
//        assertThat(response, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
//                                     "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
//                                     "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
//                                     "        <ExceptionText>Unable to generate the exception XML : JAXB Exception.</ExceptionText>\n" +
//                                     "    </Exception>\n" +
//                                     "</ExceptionReport>\n"));
//    }

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