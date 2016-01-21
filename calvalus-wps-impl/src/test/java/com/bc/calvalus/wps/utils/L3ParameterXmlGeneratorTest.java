package com.bc.calvalus.wps.utils;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bc.wps.api.schema.AggregatorConfig;
import com.bc.wps.api.schema.Aggregators;
import com.bc.wps.api.schema.CellProcessorConfig;
import com.bc.wps.api.schema.CompositingType;
import com.bc.wps.api.schema.L3Parameters;
import com.bc.wps.api.schema.TimeFilterMethod;
import com.bc.wps.api.schema.VariableConfig;
import com.bc.wps.api.schema.Variables;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class L3ParameterXmlGeneratorTest {

    private L3Parameters mockL3Parameters;

    /**
     * Class under test.
     */
    private L3ParameterXmlGenerator l3ParameterXmlGenerator;

    @Before
    public void setUp() throws Exception {
        mockL3Parameters = mock(L3Parameters.class);
    }

    @Test
    public void canCreateXmlWithAllElements() throws Exception {
        constructL3ParametersMocking();

        l3ParameterXmlGenerator = new L3ParameterXmlGenerator(mockL3Parameters);

        assertThat(l3ParameterXmlGenerator.createXml(), equalTo("<parameters>\n" +
                                                                "<planetaryGrid>mockPlanetaryGrid</planetaryGrid>\n" +
                                                                "<numRows>1</numRows>\n" +
                                                                "<compositingType>BINNING</compositingType>\n" +
                                                                "<superSampling>1</superSampling>\n" +
                                                                "<maskExpr>mockMaskExpr</maskExpr>\n" +
                                                                "<minDataHour>1.0</minDataHour>\n" +
                                                                "<metadataAggregatorName>mockAggregatorName</metadataAggregatorName>\n" +
                                                                "<startDateTime>mockDateTime</startDateTime>\n" +
                                                                "<periodDuration>1.0</periodDuration>\n" +
                                                                "<timeFilterMethod>SPATIOTEMPORAL_DATA_DAY</timeFilterMethod>\n" +
                                                                "<outputFile>mockOutputFile</outputFile>\n" +
                                                                "<variables>\n" +
                                                                "<variable>\n" +
                                                                "<name>mockVar</name>\n" +
                                                                "<expr>mockExpr</expr>\n" +
                                                                "</variable>\n</variables>\n" +
                                                                "<aggregators>\n" +
                                                                "<aggregator>\n" +
                                                                "<type>AVG</type>\n" +
                                                                "<varName>tsm</varName>\n" +
                                                                "</aggregator>\n" +
                                                                "</aggregators>\n" +
                                                                "<postProcessor>\n" +
                                                                "<type>mockProcessorType</type>\n" +
                                                                "<varName>mockVarName</varName>\n" +
                                                                "</postProcessor>\n" +
                                                                "</parameters>"));
    }

    @Test
    public void canCreateXmlWithVariables() throws Exception {
        Variables mockVariables = getVariables();
        L3Parameters l3Parameters = new L3Parameters();
        l3Parameters.setVariables(mockVariables);

        l3ParameterXmlGenerator = new L3ParameterXmlGenerator(l3Parameters);

        assertThat(l3ParameterXmlGenerator.createXml(), equalTo("<parameters>\n" +
                                                                "<variables>\n" +
                                                                "<variable>\n" +
                                                                "<name>mockVar</name>\n" +
                                                                "<expr>mockExpr</expr>\n" +
                                                                "</variable>\n" +
                                                                "</variables>\n" +
                                                                "</parameters>"));
    }

    @Test
    public void canCreateXmlWithAggregators() throws Exception {
        Aggregators mockAggregators = getAggregators();
        L3Parameters l3Parameters = new L3Parameters();
        l3Parameters.setAggregators(mockAggregators);

        l3ParameterXmlGenerator = new L3ParameterXmlGenerator(l3Parameters);

        assertThat(l3ParameterXmlGenerator.createXml(), equalTo("<parameters>\n" +
                                                                "<aggregators>\n" +
                                                                "<aggregator>\n" +
                                                                "<type>AVG</type>\n" +
                                                                "<varName>tsm</varName>\n" +
                                                                "</aggregator>\n" +
                                                                "</aggregators>\n" +
                                                                "</parameters>"));

    }

    @Test
    public void canCreateXmlWithProcessors() throws Exception {
        CellProcessorConfig mockProcessorConfig = getMockCellProcessorConfig();

        L3Parameters l3Parameters = new L3Parameters();
        l3Parameters.setPostProcessorConfig(mockProcessorConfig);

        l3ParameterXmlGenerator = new L3ParameterXmlGenerator(l3Parameters);

        assertThat(l3ParameterXmlGenerator.createXml(), equalTo("<parameters>\n" +
                                                                "<postProcessor>\n" +
                                                                "<type>mockProcessorType</type>\n" +
                                                                "<varName>mockVarName</varName>\n" +
                                                                "</postProcessor>\n" +
                                                                "</parameters>"));
    }

    @Test
    public void testCreateEmptyXml() throws Exception {
        L3Parameters l3Parameters = new L3Parameters();
        l3ParameterXmlGenerator = new L3ParameterXmlGenerator(l3Parameters);

        assertThat(l3ParameterXmlGenerator.createXml(), equalTo("<parameters>\n" +
                                                                "</parameters>"));
    }

    private void constructL3ParametersMocking() {
        when(mockL3Parameters.getPlanetaryGrid()).thenReturn("mockPlanetaryGrid");
        when(mockL3Parameters.getNumRows()).thenReturn(1);
        when(mockL3Parameters.getCompositingType()).thenReturn(CompositingType.BINNING);
        when(mockL3Parameters.getSuperSampling()).thenReturn(1);
        when(mockL3Parameters.getMaskExpr()).thenReturn("mockMaskExpr");
        when(mockL3Parameters.getMinDataHour()).thenReturn(1.0);
        when(mockL3Parameters.getMetadataAggregatorName()).thenReturn("mockAggregatorName");
        when(mockL3Parameters.getStartDateTime()).thenReturn("mockDateTime");
        when(mockL3Parameters.getPeriodDuration()).thenReturn(1.0);
        when(mockL3Parameters.getTimeFilterMethod()).thenReturn(TimeFilterMethod.SPATIOTEMPORAL_DATA_DAY);
        when(mockL3Parameters.getOutputFile()).thenReturn("mockOutputFile");

        Variables mockVariables = getVariables();
        when(mockL3Parameters.getVariables()).thenReturn(mockVariables);

        Aggregators mockAggregators = getAggregators();
        when(mockL3Parameters.getAggregators()).thenReturn(mockAggregators);

        CellProcessorConfig mockProcessorConfig = getMockCellProcessorConfig();
        when(mockL3Parameters.getPostProcessorConfig()).thenReturn(mockProcessorConfig);
    }

    private Variables getVariables() {
        Variables mockVariables = mock(Variables.class);
        VariableConfig mockVariableConfig = mock(VariableConfig.class);
        when(mockVariableConfig.getName()).thenReturn("mockVar");
        when(mockVariableConfig.getExpr()).thenReturn("mockExpr");

        List<VariableConfig> mockVariable = new ArrayList<>();
        mockVariable.add(mockVariableConfig);
        when(mockVariables.getVariable()).thenReturn(mockVariable);

        return mockVariables;
    }

    private Aggregators getAggregators() {
        Aggregators mockAggregators = mock(Aggregators.class);
        AggregatorConfig mockAggregatorConfig = mock(AggregatorConfig.class);
        when(mockAggregatorConfig.getType()).thenReturn("AVG");
        when(mockAggregatorConfig.getVarName()).thenReturn("tsm");

        List<AggregatorConfig> mockAggregatorList = new ArrayList<>();
        mockAggregatorList.add(mockAggregatorConfig);
        when(mockAggregators.getAggregator()).thenReturn(mockAggregatorList);

        return mockAggregators;
    }

    private CellProcessorConfig getMockCellProcessorConfig() {
        CellProcessorConfig mockProcessorConfig = mock(CellProcessorConfig.class);
        when(mockProcessorConfig.getType()).thenReturn("mockProcessorType");
        when(mockProcessorConfig.getVarName()).thenReturn("mockVarName");
        return mockProcessorConfig;
    }

}