package com.bc.calvalus.wps.wpsoperations;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.calvalusfacade.CalvalusProcessor;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.ProcessesNotAvailableException;
import com.bc.calvalus.wps.responses.IWpsProcess;
import com.bc.calvalus.wps.utils.ProcessorNameParser;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.ProcessDescriptionType;
import org.junit.*;
import org.junit.runner.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.servlet.ServletRequestWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CalvalusDescribeProcessOperation.class, CalvalusFacade.class, ProcessorNameParser.class})
public class CalvalusDescribeProcessOperationTest {

    private CalvalusDescribeProcessOperation describeProcessOperation;

    private CalvalusFacade mockCalvalusFacade;
    private WpsRequestContext mockRequestContext;

    @Before
    public void setUp() throws Exception {
        mockCalvalusFacade = mock(CalvalusFacade.class);
        mockRequestContext = mock(WpsRequestContext.class);
    }

    @Test
    public void canGetSingleProcess() throws Exception {
        ProductSet mockProductSet = mock(ProductSet.class);
        when(mockProductSet.getProductType()).thenReturn("inputProductType");
        ProductSet[] mockProductSets = new ProductSet[]{mockProductSet};
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);
        CalvalusProcessor calvalusProcessor1 = getProcess1();
        when(mockCalvalusFacade.getProcessor(any(ProcessorNameParser.class))).thenReturn(calvalusProcessor1);
        when(mockCalvalusFacade.getProductSets()).thenReturn(mockProductSets);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);

        describeProcessOperation = new CalvalusDescribeProcessOperation(mockRequestContext);
        List<ProcessDescriptionType> processes = describeProcessOperation.getProcesses("process1");

        assertThat(processes.size(), equalTo(1));
        assertThat(processes.get(0).getIdentifier().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processes.get(0).getTitle().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processes.get(0).getAbstract().getValue(), equalTo("Some description"));
    }

    @Test
    public void canGetMultipleProcesses() throws Exception {
        ProductSet mockProductSet = mock(ProductSet.class);
        when(mockProductSet.getProductType()).thenReturn("inputProductType");
        ProductSet[] mockProductSets = new ProductSet[]{mockProductSet};
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);
        CalvalusProcessor calvalusProcessor1 = getProcess1();
        CalvalusProcessor calvalusProcessor2 = getProcess2();
        when(mockCalvalusFacade.getProcessor(any(ProcessorNameParser.class))).thenReturn(calvalusProcessor1, calvalusProcessor2);
        when(mockCalvalusFacade.getProductSets()).thenReturn(mockProductSets);

        describeProcessOperation = new CalvalusDescribeProcessOperation(mockRequestContext);
        List<ProcessDescriptionType> processes = describeProcessOperation.getProcesses("process1,process2");

        assertThat(processes.size(), equalTo(2));

        assertThat(processes.get(0).getIdentifier().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processes.get(0).getTitle().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processes.get(0).getAbstract().getValue(), equalTo("Some description"));

        assertThat(processes.get(1).getIdentifier().getValue(), equalTo("beam-buildin~1.0~urban-tep-indices"));
        assertThat(processes.get(1).getTitle().getValue(), equalTo("beam-buildin~1.0~urban-tep-indices"));
        assertThat(processes.get(1).getAbstract().getValue(), equalTo("Some description"));
    }

    @Test
    public void canGetAllProcesses() throws Exception {
        ProductSet mockProductSet = mock(ProductSet.class);
        when(mockProductSet.getProductType()).thenReturn("inputProductType");
        ProductSet[] mockProductSets = new ProductSet[]{mockProductSet};
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);
        List<IWpsProcess> mockProcessorList = getMockProcessList();
        when(mockCalvalusFacade.getProcessors()).thenReturn(mockProcessorList);
        when(mockCalvalusFacade.getProductSets()).thenReturn(mockProductSets);

        describeProcessOperation = new CalvalusDescribeProcessOperation(mockRequestContext);
        List<ProcessDescriptionType> processes = describeProcessOperation.getProcesses("all");

        assertThat(processes.size(), equalTo(2));

        assertThat(processes.get(0).getIdentifier().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processes.get(0).getTitle().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processes.get(0).getAbstract().getValue(), equalTo("Some description"));

        assertThat(processes.get(1).getIdentifier().getValue(), equalTo("beam-buildin~1.0~urban-tep-indices"));
        assertThat(processes.get(1).getTitle().getValue(), equalTo("beam-buildin~1.0~urban-tep-indices"));
        assertThat(processes.get(1).getAbstract().getValue(), equalTo("Some description"));
    }

    @Test(expected = ProcessesNotAvailableException.class)
    public void canThrowExceptionWhenProcessorIsNull() throws Exception {
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);
        when(mockCalvalusFacade.getProcessor(any(ProcessorNameParser.class))).thenReturn(null);

        describeProcessOperation = new CalvalusDescribeProcessOperation(mockRequestContext);
        describeProcessOperation.getProcesses("process1");
    }

    @Test(expected = ProcessesNotAvailableException.class)
    public void canThrowExceptionWhenOneOfProcessorsNotAvailable() throws Exception {
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);
        CalvalusProcessor calvalusProcessor1 = getProcess1();
        CalvalusProcessor calvalusProcessor2 = getProcess2();
        when(mockCalvalusFacade.getProcessor(any(ProcessorNameParser.class))).thenReturn(calvalusProcessor1, null, calvalusProcessor2);

        describeProcessOperation = new CalvalusDescribeProcessOperation(mockRequestContext);
        describeProcessOperation.getProcesses("process1,invalidProcessId,process2");
    }

    @Test(expected = ProcessesNotAvailableException.class)
    public void canCatchInvalidProcessorIdException() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenThrow(new InvalidProcessorIdException("processorId not found"));

        describeProcessOperation = new CalvalusDescribeProcessOperation(mockRequestContext);
        describeProcessOperation.getProcesses("process1");
    }

    @Test(expected = ProcessesNotAvailableException.class)
    public void canCatchIOException() throws Exception {
        ProductSet mockProductSet = mock(ProductSet.class);
        when(mockProductSet.getProductType()).thenReturn("inputProductType");
        ProcessorNameParser mockProcessorNameParser = mock(ProcessorNameParser.class);
        CalvalusProcessor calvalusProcessor1 = getProcess1();
        when(mockCalvalusFacade.getProcessor(any(ProcessorNameParser.class))).thenReturn(calvalusProcessor1);
        when(mockCalvalusFacade.getProductSets()).thenThrow(new IOException("IO Exception"));
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        PowerMockito.whenNew(ProcessorNameParser.class).withAnyArguments().thenReturn(mockProcessorNameParser);

        describeProcessOperation = new CalvalusDescribeProcessOperation(mockRequestContext);
        describeProcessOperation.getProcesses("process1");
    }


    private List<IWpsProcess> getMockProcessList() {
        List<IWpsProcess> mockProcessList = new ArrayList<>();

        CalvalusProcessor process1 = getProcess1();
        CalvalusProcessor process2 = getProcess2();

        mockProcessList.add(process1);
        mockProcessList.add(process2);

        return mockProcessList;
    }

    private CalvalusProcessor getProcess2() {
        CalvalusProcessor process2 = mock(CalvalusProcessor.class);
        when(process2.getIdentifier()).thenReturn("beam-buildin~1.0~urban-tep-indices");
        when(process2.getTitle()).thenReturn("Urban TEP seasonality indices from MERIS SR");
        when(process2.getAbstractText()).thenReturn("Some description");
        when(process2.getInputProductTypes()).thenReturn(new String[]{"inputProductType"});
        return process2;
    }

    private CalvalusProcessor getProcess1() {
        CalvalusProcessor process1 = mock(CalvalusProcessor.class);
        when(process1.getIdentifier()).thenReturn("beam-buildin~1.0~BandMaths");
        when(process1.getTitle()).thenReturn("Band arythmetic processor");
        when(process1.getAbstractText()).thenReturn("Some description");
        when(process1.getInputProductTypes()).thenReturn(new String[]{"inputProductType"});
        return process1;
    }

}