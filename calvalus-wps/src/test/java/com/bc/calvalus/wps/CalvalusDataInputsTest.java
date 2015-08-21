package com.bc.calvalus.wps;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs;
import org.deegree.commons.tom.ows.CodeType;
import org.deegree.services.wps.ProcessletInputs;
import org.deegree.services.wps.input.ComplexInput;
import org.deegree.services.wps.input.LiteralInput;
import org.deegree.services.wps.input.ProcessletInput;
import org.junit.*;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hans on 31/07/2015.
 */
public class CalvalusDataInputsTest {

    /**
     * Class under test.
     */
    private CalvalusDataInputs calvalusDataInputs;

    @Test
    public void canGetValueForLiteralInputsOnly() throws Exception {
        List<ProcessletInput> mockProcessletInputList = new ArrayList<>();
        mockProcessletInputList.add(getMockLiteralInput("productionName", "Ocean Colour test"));
        mockProcessletInputList.add(getMockLiteralInput("processorBundleName", "case2-regional"));
        ProcessletInputs processletInputs = new ProcessletInputs(mockProcessletInputList);

        calvalusDataInputs = new CalvalusDataInputs(processletInputs);

        assertThat(calvalusDataInputs.getValue("productionName"), equalTo("Ocean Colour test"));
        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo("case2-regional"));
    }

    @Test
    public void canGetValueForComplexInputsOnly() throws Exception {
        List<ProcessletInput> mockProcessletInputList = new ArrayList<>();
        mockProcessletInputList.add(getMockComplexInput("calvalus.l3.parameters", getCalvalusL3Parameters()));
        mockProcessletInputList.add(getMockComplexInput("processorParameters", getSampleProcessorParameters()));
        ProcessletInputs processletInputs = new ProcessletInputs(mockProcessletInputList);

        calvalusDataInputs = new CalvalusDataInputs(processletInputs);

        assertThat(calvalusDataInputs.getValue("processorParameters"),
                   equalTo("<parameters>\n" +
                           "\t\t\t  <doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
                           "\t\t\t  <doSmileCorrection>true</doSmileCorrection>\n" +
                           "\t\t\t  <outputTosa>false</outputTosa>\n" +
                           "\t\t</parameters>"));
        assertThat(calvalusDataInputs.getValue("calvalus.l3.parameters"),
                   equalTo("<parameters>\n" +
                           "            <compositingType>MOSAICKING</compositingType>\n" +
                           "            <planetaryGrid>org.esa.beam.binning.support.PlateCarreeGrid</planetaryGrid>\n" +
                           "            <numRows>21600</numRows>\n" +
                           "            <superSampling>1</superSampling>\n" +
                           "            <maskExpr>!case2_flags.INVALID</maskExpr>\n" +
                           "            <aggregators>\n" +
                           "              <aggregator>\n" +
                           "                <type>AVG</type>\n" +
                           "                <varName>tsm</varName>\n" +
                           "              </aggregator>\n" +
                           "            </aggregators>\n" +
                           "          </parameters>"));
    }

    @Test
    public void canGetValueForLiteralAndComplexInputs() throws Exception {
        List<ProcessletInput> mockProcessletInputList = new ArrayList<>();
        mockProcessletInputList.add(getMockLiteralInput("productionName", "Ocean Colour test"));
        mockProcessletInputList.add(getMockLiteralInput("processorBundleName", "case2-regional"));
        mockProcessletInputList.add(getMockComplexInput("processorParameters", getSampleProcessorParameters()));
        ProcessletInputs processletInputs = new ProcessletInputs(mockProcessletInputList);

        calvalusDataInputs = new CalvalusDataInputs(processletInputs);

        assertThat(calvalusDataInputs.getValue("productionName"), equalTo("Ocean Colour test"));
        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo("case2-regional"));
        assertThat(calvalusDataInputs.getValue("processorParameters"),
                   equalTo("<parameters>\n" +
                           "\t\t\t  <doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
                           "\t\t\t  <doSmileCorrection>true</doSmileCorrection>\n" +
                           "\t\t\t  <outputTosa>false</outputTosa>\n" +
                           "\t\t</parameters>"));
    }

    @Test
    public void returnsNullWhenValueIsNotAvailable() throws Exception {
        List<ProcessletInput> mockProcessletInputList = new ArrayList<>();
        mockProcessletInputList.add(getMockLiteralInput("productionName", "Ocean Colour test"));
        ProcessletInputs processletInputs = new ProcessletInputs(mockProcessletInputList);

        calvalusDataInputs = new CalvalusDataInputs(processletInputs);

        assertThat(calvalusDataInputs.getValue("notExist"), is(nullValue()));
    }

    @Test
    public void canReturnAllValues() throws Exception {
        List<ProcessletInput> mockProcessletInputList = new ArrayList<>();
        mockProcessletInputList.add(getMockLiteralInput("productionName", "Ocean Colour test"));
        mockProcessletInputList.add(getMockLiteralInput("processorBundleName", "case2-regional"));
        mockProcessletInputList.add(getMockComplexInput("processorParameters", getSampleProcessorParameters()));
        mockProcessletInputList.add(getMockComplexInput("calvalus.l3.parameters", getCalvalusL3Parameters()));
        ProcessletInputs processletInputs = new ProcessletInputs(mockProcessletInputList);

        calvalusDataInputs = new CalvalusDataInputs(processletInputs);

        assertThat(calvalusDataInputs.toString(), equalTo(
                    "processorBundleName : case2-regional\nprocessorParameters : <parameters>\n" +
                    "\t\t\t  <doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
                    "\t\t\t  <doSmileCorrection>true</doSmileCorrection>\n" +
                    "\t\t\t  <outputTosa>false</outputTosa>\n" +
                    "\t\t</parameters>\n" +
                    "productionName : Ocean Colour test\n" +
                    "calvalus.l3.parameters : <parameters>\n" +
                    "            <compositingType>MOSAICKING</compositingType>\n" +
                    "            <planetaryGrid>org.esa.beam.binning.support.PlateCarreeGrid</planetaryGrid>\n" +
                    "            <numRows>21600</numRows>\n" +
                    "            <superSampling>1</superSampling>\n" +
                    "            <maskExpr>!case2_flags.INVALID</maskExpr>\n" +
                    "            <aggregators>\n" +
                    "              <aggregator>\n" +
                    "                <type>AVG</type>\n" +
                    "                <varName>tsm</varName>\n" +
                    "              </aggregator>\n" +
                    "            </aggregators>\n" +
                    "          </parameters>\n"));
    }

    @Test
    public void canGetInputMap() throws Exception {
        List<ProcessletInput> mockProcessletInputList = new ArrayList<>();
        Map<String, String> expectedInputMap = new HashMap<>();
        mockProcessletInputList.add(getMockLiteralInput("productionName", "Ocean Colour test"));
        expectedInputMap.put("productionName", "Ocean Colour test");
        mockProcessletInputList.add(getMockLiteralInput("processorBundleName", "case2-regional"));
        expectedInputMap.put("processorBundleName", "case2-regional");
        mockProcessletInputList.add(getMockComplexInput("processorParameters", getSampleProcessorParameters()));
        expectedInputMap.put("processorParameters", getSampleProcessorParameters());
        ProcessletInputs processletInputs = new ProcessletInputs(mockProcessletInputList);

        calvalusDataInputs = new CalvalusDataInputs(processletInputs);

        assertThat(calvalusDataInputs.getInputMap(), equalTo(expectedInputMap));
    }

    @Test
    public void canCatchXMLStreamException() throws Exception {
        List<ProcessletInput> mockProcessletInputList = new ArrayList<>();
        mockProcessletInputList.add(getMockComplexInputWithException("processorParameters", new XMLStreamException()));
        ProcessletInputs processletInputs = new ProcessletInputs(mockProcessletInputList);

        calvalusDataInputs = new CalvalusDataInputs(processletInputs);

        assertThat(calvalusDataInputs.getValue("processorParameters"), equalTo(""));
    }

    @Test
    public void canCatchIOException() throws Exception {
        List<ProcessletInput> mockProcessletInputList = new ArrayList<>();
        mockProcessletInputList.add(getMockComplexInputWithException("processorParameters", new IOException()));
        ProcessletInputs processletInputs = new ProcessletInputs(mockProcessletInputList);

        calvalusDataInputs = new CalvalusDataInputs(processletInputs);

        assertThat(calvalusDataInputs.getValue("processorParameters"), equalTo(""));
    }

    private LiteralInput getMockLiteralInput(String parameterName, String value) {
        LiteralInput mockLiteralInput = mock(LiteralInput.class);
        CodeType mockCodeType = mock(CodeType.class);
        when(mockCodeType.toString()).thenReturn(parameterName);
        when(mockLiteralInput.getIdentifier()).thenReturn(mockCodeType);
        when(mockLiteralInput.getValue()).thenReturn(value);
        return mockLiteralInput;
    }

    private ComplexInput getMockComplexInput(String parameterName, String xmlValue) throws IOException, XMLStreamException {
        ComplexInput mockComplexInput = mock(ComplexInput.class);
        CodeType mockCodeType = mock(CodeType.class);
        when(mockCodeType.toString()).thenReturn(parameterName);
        when(mockComplexInput.getIdentifier()).thenReturn(mockCodeType);
        when(mockComplexInput.getValueAsXMLStream()).thenReturn(getXmlStreamReader(xmlValue));
        return mockComplexInput;
    }

    private ComplexInput getMockComplexInputWithException(String parameterName, Exception exception) throws IOException, XMLStreamException {
        ComplexInput mockComplexInput = mock(ComplexInput.class);
        CodeType mockCodeType = mock(CodeType.class);
        when(mockCodeType.toString()).thenReturn(parameterName);
        when(mockComplexInput.getIdentifier()).thenReturn(mockCodeType);
        when(mockComplexInput.getValueAsXMLStream()).thenThrow(exception);
        return mockComplexInput;
    }

    private XMLStreamReader getXmlStreamReader(String xmlString) throws XMLStreamException {
        Reader reader = new StringReader(xmlString);
        XMLInputFactory factory = XMLInputFactory.newInstance();
        return factory.createXMLStreamReader(reader);
    }

    private String getInvalidXml() {
        return "<parameters>\n" +
               "\t\t\t  <doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
               "\t\t\t  <doSmileCorrection>true</doSmileCorrection>\n" +
               "\t\t\t  <outputTosa>false\n" +
               "\t\t</parameters>";
    }

    private String getSampleProcessorParameters() {
        return "<parameters>\n" +
               "\t\t\t  <doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
               "\t\t\t  <doSmileCorrection>true</doSmileCorrection>\n" +
               "\t\t\t  <outputTosa>false</outputTosa>\n" +
               "\t\t</parameters>";
    }

    private String getCalvalusL3Parameters() {
        return "<parameters>\n" +
               "            <compositingType>MOSAICKING</compositingType>\n" +
               "            <planetaryGrid>org.esa.beam.binning.support.PlateCarreeGrid</planetaryGrid>\n" +
               "            <numRows>21600</numRows>\n" +
               "            <superSampling>1</superSampling>\n" +
               "            <maskExpr>!case2_flags.INVALID</maskExpr>\n" +
               "            <aggregators>\n" +
               "              <aggregator>\n" +
               "                <type>AVG</type>\n" +
               "                <varName>tsm</varName>\n" +
               "              </aggregator>\n" +
               "            </aggregators>\n" +
               "          </parameters>";
    }
}