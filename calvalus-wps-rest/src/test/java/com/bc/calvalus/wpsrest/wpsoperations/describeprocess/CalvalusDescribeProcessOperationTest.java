package com.bc.calvalus.wpsrest.wpsoperations.describeprocess;

import static com.bc.calvalus.wpsrest.jaxb.ProcessDescriptionType.DataInputs;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.CodeType;
import com.bc.calvalus.wpsrest.jaxb.InputDescriptionType;
import com.bc.calvalus.wpsrest.jaxb.LanguageStringType;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptionType;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.responses.CalvalusDescribeProcessResponse;
import com.bc.calvalus.wpsrest.responses.IWpsProcess;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CalvalusDescribeProcessOperation.class, CalvalusDescribeProcessResponse.class, CalvalusHelper.class, ProcessorNameParser.class})
public class CalvalusDescribeProcessOperationTest {

    private CalvalusDescribeProcessOperation describeProcessOperation;

    private WpsMetadata mockWpsMetadata;
    private CalvalusDescribeProcessResponse mockDescribeProcessResponse;
    private CalvalusHelper mockCalvalusHelper;
    private List<IWpsProcess> mockProcessList;
    private ServletRequestWrapper mockServletRequestWrapper;

    @Before
    public void setUp() throws Exception {
        mockWpsMetadata = mock(WpsMetadata.class);
        mockDescribeProcessResponse = mock(CalvalusDescribeProcessResponse.class);
        mockCalvalusHelper = mock(CalvalusHelper.class);
        mockProcessList = getMockProcessList();
        mockServletRequestWrapper = mock(ServletRequestWrapper.class);
        when(mockWpsMetadata.getServletRequestWrapper()).thenReturn(mockServletRequestWrapper);

        PowerMockito.whenNew(CalvalusDescribeProcessResponse.class).withArguments(mockWpsMetadata).thenReturn(mockDescribeProcessResponse);
    }

    @Test
    public void canGetProcessDescription() throws Exception {
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusHelper);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);
        Processor processor1 = getProcess1();
        when(mockCalvalusHelper.getProcessor(any(ProcessorNameParser.class))).thenReturn(processor1);
        when(mockDescribeProcessResponse.getSingleDescribeProcessResponse(any(Processor.class))).thenCallRealMethod();
        when(mockDescribeProcessResponse.createBasicProcessDescriptions()).thenCallRealMethod();

        ProcessDescriptionType processDescription = getProcessDescriptionType();

        when(mockDescribeProcessResponse.getSingleProcessDescription(any(IWpsProcess.class), any(WpsMetadata.class))).thenReturn(processDescription);
        PowerMockito.whenNew(CalvalusDescribeProcessResponse.class).withArguments(mockWpsMetadata).thenReturn(mockDescribeProcessResponse);

        describeProcessOperation = new CalvalusDescribeProcessOperation();
        String describeProcessResponse = describeProcessOperation.describeProcess(mockWpsMetadata, "process1");

        assertThat(describeProcessResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                    "<ProcessDescriptions service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
                                                    "    <ns4:ProcessDescription>\n" +
                                                    "        <ns1:Title>process1</ns1:Title>\n" +
                                                    "        <ns1:Abstract>Description</ns1:Abstract>\n" +
                                                    "        <ns4:DataInputs>\n" +
                                                    "            <ns4:Input>\n" +
                                                    "                <ns1:Identifier>input1</ns1:Identifier>\n" +
                                                    "                <ns1:Abstract>input description</ns1:Abstract>\n" +
                                                    "            </ns4:Input>\n" +
                                                    "        </ns4:DataInputs>\n" +
                                                    "    </ns4:ProcessDescription>\n" +
                                                    "</ProcessDescriptions>\n"));
    }

    @Test
    public void canGetAllProcesses() throws Exception {
        ArgumentCaptor<List> processes = ArgumentCaptor.forClass(List.class);
        when(mockCalvalusHelper.getProcessors()).thenReturn(mockProcessList);
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusHelper);

        describeProcessOperation = new CalvalusDescribeProcessOperation();
        describeProcessOperation.getProcessDescriptions(mockWpsMetadata, "all");

        verify(mockDescribeProcessResponse).getMultipleDescribeProcessResponse(processes.capture());
        verify(mockCalvalusHelper).getProcessors();

        assertThat(processes.getValue(), equalTo(mockProcessList));
    }

    @Test
    public void canGetMultipleProcesses() throws Exception {
        ArgumentCaptor<List> processes = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<String> processName = ArgumentCaptor.forClass(String.class);
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);

        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusHelper);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);
        Processor processor1 = getProcess1();
        Processor processor2 = getProcess2();
        when(mockCalvalusHelper.getProcessor(any(ProcessorNameParser.class))).thenReturn(processor1, processor2);

        describeProcessOperation = new CalvalusDescribeProcessOperation();
        describeProcessOperation.getProcessDescriptions(mockWpsMetadata, "process1,process2");

        PowerMockito.verifyNew(ProcessorNameParser.class, times(2)).withArguments(processName.capture());
        verify(mockDescribeProcessResponse).getMultipleDescribeProcessResponse(processes.capture());
        assertThat(processName.getAllValues().get(0), equalTo("process1"));
        assertThat(processName.getAllValues().get(1), equalTo("process2"));
        //still need to verify the processes being passed in describeProcessResponse.getMultipleDescribeProcessResponse() method
    }

    @Test
    public void canGetSingleProcess() throws Exception {
        ArgumentCaptor<Processor> singleProcess = ArgumentCaptor.forClass(Processor.class);
        ArgumentCaptor<String> processName = ArgumentCaptor.forClass(String.class);
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusHelper);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);
        Processor processor1 = getProcess1();
        when(mockCalvalusHelper.getProcessor(any(ProcessorNameParser.class))).thenReturn(processor1);

        describeProcessOperation = new CalvalusDescribeProcessOperation();
        describeProcessOperation.getProcessDescriptions(mockWpsMetadata, "process1");

        PowerMockito.verifyNew(ProcessorNameParser.class).withArguments(processName.capture());
        verify(mockDescribeProcessResponse).getSingleDescribeProcessResponse(singleProcess.capture());
        assertThat(processName.getValue(), equalTo("process1"));
        assertThat(singleProcess.getValue(), equalTo(processor1));
    }

    @Test(expected = WpsException.class)
    public void canThrowExceptionWhenProcessorIsNull() throws Exception {
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusHelper);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);
        when(mockCalvalusHelper.getProcessor(any(ProcessorNameParser.class))).thenReturn(null);

        describeProcessOperation = new CalvalusDescribeProcessOperation();
        describeProcessOperation.getProcessDescriptions(mockWpsMetadata, "process1");
    }

    @Test
    public void canCatchJaxbException() throws Exception {
        JaxbHelper mockJaxbHelper = mock(JaxbHelper.class);
        when(mockJaxbHelper.marshal(anyObject(), any(StringWriter.class))).thenThrow(new JAXBException("JAXB exception")).thenCallRealMethod();
        PowerMockito.whenNew(JaxbHelper.class).withNoArguments().thenReturn(mockJaxbHelper);
        when(mockDescribeProcessResponse.getMultipleDescribeProcessResponse(any())).thenReturn(new ProcessDescriptions());
        PowerMockito.whenNew(CalvalusDescribeProcessResponse.class).withArguments(any(WpsMetadata.class)).thenReturn(mockDescribeProcessResponse);
        Processor processor1 = getProcess1();
        when(mockCalvalusHelper.getProcessor(any(ProcessorNameParser.class))).thenReturn(processor1);
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusHelper);

        describeProcessOperation = new CalvalusDescribeProcessOperation();
        String exceptionResponse = describeProcessOperation.describeProcess(mockWpsMetadata, "all");

        assertThat(exceptionResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
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
        when(mockDescribeProcessResponse.getMultipleDescribeProcessResponse(any())).thenReturn(new ProcessDescriptions());
        PowerMockito.whenNew(CalvalusDescribeProcessResponse.class).withArguments(any(WpsMetadata.class)).thenReturn(mockDescribeProcessResponse);
        Processor processor1 = getProcess1();
        when(mockCalvalusHelper.getProcessor(any(ProcessorNameParser.class))).thenReturn(processor1);
        PowerMockito.whenNew(CalvalusHelper.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusHelper);

        describeProcessOperation = new CalvalusDescribeProcessOperation();
        String exceptionResponse = describeProcessOperation.describeProcess(mockWpsMetadata, "all");

        assertThat(exceptionResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                              "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
                                              "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
                                              "        <ExceptionText>Unable to generate the exception XML : JAXB Exception.</ExceptionText>\n" +
                                              "    </Exception>\n</ExceptionReport>\n"));
    }

    private ProcessDescriptionType getProcessDescriptionType() {
        ProcessDescriptionType processDescription = new ProcessDescriptionType();
        LanguageStringType title = new LanguageStringType();
        title.setValue("process1");
        processDescription.setTitle(title);

        LanguageStringType abstractValue = new LanguageStringType();
        abstractValue.setValue("Description");
        processDescription.setAbstract(abstractValue);

        DataInputs dataInputs = new DataInputs();
        InputDescriptionType input = new InputDescriptionType();
        CodeType input1 = new CodeType();
        input1.setValue("input1");
        input.setIdentifier(input1);

        LanguageStringType inputAbstract = new LanguageStringType();
        inputAbstract.setValue("input description");
        input.setAbstract(inputAbstract);

        dataInputs.getInput().add(input);
        processDescription.setDataInputs(dataInputs);
        return processDescription;
    }

    private List<IWpsProcess> getMockProcessList() {
        List<IWpsProcess> mockProcessList = new ArrayList<>();

        Processor process1 = getProcess1();
        Processor process2 = getProcess2();

        mockProcessList.add(process1);
        mockProcessList.add(process2);

        return mockProcessList;
    }

    private Processor getProcess2() {
        Processor process2 = mock(Processor.class);
        when(process2.getIdentifier()).thenReturn("beam-buildin~1.0~urban-tep-indices");
        when(process2.getTitle()).thenReturn("Urban TEP seasonality indices from MERIS SR");
        when(process2.getAbstractText()).thenReturn("Some description");
        return process2;
    }

    private Processor getProcess1() {
        Processor process1 = mock(Processor.class);
        when(process1.getIdentifier()).thenReturn("beam-buildin~1.0~BandMaths");
        when(process1.getTitle()).thenReturn("Band arythmetic processor");
        when(process1.getAbstractText()).thenReturn("Some description");
        return process1;
    }
}