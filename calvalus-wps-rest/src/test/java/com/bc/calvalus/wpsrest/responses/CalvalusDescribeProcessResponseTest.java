package com.bc.calvalus.wpsrest.responses;

import static com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.CalvalusProcessor;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.exception.WpsRuntimeException;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import org.junit.*;
import org.junit.runner.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CalvalusDescribeProcessResponseConverter.class, CalvalusFacade.class})
public class CalvalusDescribeProcessResponseTest {

    private CalvalusDescribeProcessResponseConverter describeProcessResponse;

    private WpsMetadata mockWpsMetadata;

    @Before
    public void setUp() throws Exception {
        mockWpsMetadata = mock(WpsMetadata.class);
        describeProcessResponse = new CalvalusDescribeProcessResponseConverter(mockWpsMetadata);
    }

    @Test
    public void canGetMultipleDescribeProcessResponse() throws Exception {
        List<IWpsProcess> mockProcessList = getMockProcessList();
        configureCalvalusHelperMock();

        describeProcessResponse = new CalvalusDescribeProcessResponseConverter(mockWpsMetadata);
        ProcessDescriptions processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(mockProcessList);

        assertThat(processDescriptions.getProcessDescription().size(), equalTo(2));
        assertThat(processDescriptions.getProcessDescription().get(0).isStoreSupported(), equalTo(true));
        assertThat(processDescriptions.getProcessDescription().get(0).isStatusSupported(), equalTo(true));
        assertThat(processDescriptions.getProcessDescription().get(0).getProcessVersion(), equalTo("1.0"));
        assertThat(processDescriptions.getProcessDescription().get(0).getIdentifier().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processDescriptions.getProcessDescription().get(0).getTitle().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processDescriptions.getProcessDescription().get(0).getAbstract().getValue(), equalTo("Some description"));

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput().size(), equalTo(8));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(0).getIdentifier().getValue(), equalTo("productionName"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(0).getTitle().getValue(), equalTo("Production name"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(0).getLiteralData().getDataType().getValue(), equalTo("string"));

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getIdentifier().getValue(), equalTo("inputDataSetName"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getTitle().getValue(), equalTo("Input data set name"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getLiteralData().getDataType().getValue(), equalTo("string"));

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(2).getIdentifier().getValue(), equalTo("minDate"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(2).getTitle().getValue(), equalTo("Date from"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(2).getLiteralData().getDataType().getValue(), equalTo("string"));

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(3).getIdentifier().getValue(), equalTo("maxDate"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(3).getTitle().getValue(), equalTo("Date to"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(3).getLiteralData().getDataType().getValue(), equalTo("string"));

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(4).getIdentifier().getValue(), equalTo("periodLength"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(4).getTitle().getValue(), equalTo("Period length"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(4).getLiteralData().getDataType().getValue(), equalTo("string"));

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(5).getIdentifier().getValue(), equalTo("regionWkt"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(5).getTitle().getValue(), equalTo("Region WKT"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(5).getLiteralData().getDataType().getValue(), equalTo("string"));

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(6).getIdentifier().getValue(), equalTo("calvalus.l3.parameters"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(6).getComplexData().getDefault().getFormat().getSchema()
                    , equalTo("http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd"));

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(7).getIdentifier().getValue(), equalTo("calvalus.output.format"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(7).getTitle().getValue(), equalTo("Calvalus output format"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(7).getLiteralData().getDataType().getValue(), equalTo("string"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(7).getLiteralData().getAllowedValues().getValueOrRange().size(), equalTo(3));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(7).getLiteralData().getAllowedValues().getValueOrRange().get(0), equalTo("NetCDF"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(7).getLiteralData().getAllowedValues().getValueOrRange().get(1), equalTo("BEAM-DIMAP"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(7).getLiteralData().getAllowedValues().getValueOrRange().get(2), equalTo("GeoTIFF"));

    }

    @Test
    public void canGetSingleDescribeProcessResponse() throws Exception {
        CalvalusProcessor process1 = getSingleProcessorWithDescriptor();
        configureCalvalusHelperMock();

        describeProcessResponse = new CalvalusDescribeProcessResponseConverter(mockWpsMetadata);
        ProcessDescriptions processDescriptions = describeProcessResponse.getSingleDescribeProcessResponse(process1);

        assertThat(processDescriptions.getProcessDescription().size(), equalTo(1));
        assertThat(processDescriptions.getProcessDescription().get(0).isStoreSupported(), equalTo(true));
        assertThat(processDescriptions.getProcessDescription().get(0).isStatusSupported(), equalTo(true));
        assertThat(processDescriptions.getProcessDescription().get(0).getProcessVersion(), equalTo("1.0"));
        assertThat(processDescriptions.getProcessDescription().get(0).getIdentifier().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processDescriptions.getProcessDescription().get(0).getTitle().getValue(), equalTo("beam-buildin~1.0~BandMaths"));
        assertThat(processDescriptions.getProcessDescription().get(0).getAbstract().getValue(), equalTo("Some description"));
    }

    @Test
    public void canGetSingleDescribeProcessResponseWithParameterDescriptor() throws Exception {
        CalvalusProcessor process1 = getSingleProcessorWithDescriptor();
        configureCalvalusHelperMock();

        describeProcessResponse = new CalvalusDescribeProcessResponseConverter(mockWpsMetadata);
        ProcessDescriptions processDescriptions = describeProcessResponse.getSingleDescribeProcessResponse(process1);

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput().size(), equalTo(9));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getIdentifier().getValue(), equalTo("descriptor"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getTitle().getValue(), equalTo("description of descriptor"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getLiteralData().getDefaultValue(), equalTo("1"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getLiteralData().getDataType().getValue(), equalTo("string"));
    }

    @Test
    public void canGetSingleDescribeProcessResponseWithDefaultProcessorParameters() throws Exception {
        CalvalusProcessor process1 = getSingleProcessorWithDefaultParameters();
        configureCalvalusHelperMock();

        describeProcessResponse = new CalvalusDescribeProcessResponseConverter(mockWpsMetadata);
        ProcessDescriptions processDescriptions = describeProcessResponse.getSingleDescribeProcessResponse(process1);

        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput().size(), equalTo(9));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getIdentifier().getValue(), equalTo("processorParameters"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getTitle().getValue(), equalTo("Processor parameters"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getLiteralData().getDefaultValue(), equalTo("Default processor parameters"));
        assertThat(processDescriptions.getProcessDescription().get(0).getDataInputs().getInput()
                               .get(1).getLiteralData().getDataType().getValue(), equalTo("string"));
    }

    @Test
    public void canCreateDefaultProcessDescriptions() throws Exception {
        describeProcessResponse = new CalvalusDescribeProcessResponseConverter(mockWpsMetadata);
        ProcessDescriptions processDescriptions = describeProcessResponse.createBasicProcessDescriptions();

        assertThat(processDescriptions.getProcessDescription().size(), equalTo(0));
        assertThat(processDescriptions.getService(), equalTo("WPS"));
        assertThat(processDescriptions.getVersion(), equalTo("1.0.0"));
        assertThat(processDescriptions.getLang(), equalTo("en"));

    }

    @Test(expected = WpsRuntimeException.class)
    public void canThrowWpsException() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsMetadata.class)).thenThrow(new ProductionException("Error when creating CalvalusFacade"));
        IWpsProcess mockProcess = mock(IWpsProcess.class);

        describeProcessResponse = new CalvalusDescribeProcessResponseConverter(mockWpsMetadata);
        describeProcessResponse.getSingleProcessDescription(mockProcess, mockWpsMetadata);
    }

    private void configureCalvalusHelperMock() throws Exception {
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        ProductSet mockProductSet1 = mock(ProductSet.class);
        when(mockProductSet1.getProductType()).thenReturn("Product1");
        ProductSet mockProductSet2 = mock(ProductSet.class);
        when(mockProductSet2.getProductType()).thenReturn("Product2");
        ProductSet[] mockProductSets = new ProductSet[]{mockProductSet1, mockProductSet2};
        when(mockCalvalusFacade.getProductSets()).thenReturn(mockProductSets);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsMetadata.class)).thenReturn(mockCalvalusFacade);
    }

    private List<IWpsProcess> getMockProcessList() {
        List<IWpsProcess> mockProcessList = new ArrayList<>();

        CalvalusProcessor process1 = mock(CalvalusProcessor.class);
        when(process1.getIdentifier()).thenReturn("beam-buildin~1.0~BandMaths");
        when(process1.getTitle()).thenReturn("Band arythmetic processor");
        when(process1.getAbstractText()).thenReturn("Some description");
        when(process1.getVersion()).thenReturn("1.0");
        when(process1.getInputProductTypes()).thenReturn(new String[]{"Product1"});

        CalvalusProcessor process2 = mock(CalvalusProcessor.class);
        when(process2.getIdentifier()).thenReturn("beam-buildin~1.0~urban-tep-indices");
        when(process2.getTitle()).thenReturn("Urban TEP seasonality indices from MERIS SR");
        when(process2.getAbstractText()).thenReturn("Some description");
        when(process2.getInputProductTypes()).thenReturn(new String[]{"Product2"});

        mockProcessList.add(process1);
        mockProcessList.add(process2);
        return mockProcessList;
    }

    private CalvalusProcessor getSingleProcessorWithDescriptor() {
        CalvalusProcessor process = mock(CalvalusProcessor.class);
        when(process.getIdentifier()).thenReturn("beam-buildin~1.0~BandMaths");
        when(process.getTitle()).thenReturn("Band arythmetic processor");
        when(process.getAbstractText()).thenReturn("Some description");
        when(process.getVersion()).thenReturn("1.0");
        when(process.getInputProductTypes()).thenReturn(new String[]{"Product1"});

        ParameterDescriptor mockParameterDescriptor = getSingleParameterDescriptor();
        ParameterDescriptor[] mockParameterDescriptors = new ParameterDescriptor[]{mockParameterDescriptor};
        when(process.getParameterDescriptors()).thenReturn(mockParameterDescriptors);
        return process;
    }

    private CalvalusProcessor getSingleProcessorWithDefaultParameters() {
        CalvalusProcessor process = mock(CalvalusProcessor.class);
        when(process.getIdentifier()).thenReturn("beam-buildin~1.0~BandMaths");
        when(process.getTitle()).thenReturn("Band arythmetic processor");
        when(process.getAbstractText()).thenReturn("Some description");
        when(process.getVersion()).thenReturn("1.0");
        when(process.getInputProductTypes()).thenReturn(new String[]{"Product1"});
        when(process.getDefaultParameters()).thenReturn("Default processor parameters");

        return process;
    }

    private ParameterDescriptor getSingleParameterDescriptor() {
        ParameterDescriptor mockParameterDescriptor = mock(ParameterDescriptor.class);
        when(mockParameterDescriptor.getName()).thenReturn("descriptor");
        when(mockParameterDescriptor.getDescription()).thenReturn("description of descriptor");
        when(mockParameterDescriptor.getDefaultValue()).thenReturn("1");
        when(mockParameterDescriptor.getType()).thenReturn("string");
        return mockParameterDescriptor;
    }
}