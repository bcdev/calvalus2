package com.bc.calvalus.wps.utility;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.*;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.StringReader;

/**
 * Created by hans on 22.07.2015.
 */
public class XmlProcessorTest {

    @Test
    public void testGetXmlStringWithoutNestedElements() throws Exception {
        String requestXml = getRequestXml();
        XMLStreamReader xmlStreamReader = readXMLFromString(requestXml);
        XmlProcessor xmlProcessor = new XmlProcessor(xmlStreamReader);

        String xmlString = xmlProcessor.getXmlString();

        assertThat(xmlString, equalTo("<parameters>\n" +
                                      "\t\t\t  <spectrumOutOfScopeThreshold>4.0</spectrumOutOfScopeThreshold>\n" +
                                      "\t\t\t  <invalidPixelExpression>agc_flags.INVALID</invalidPixelExpression>\n" +
                                      "</parameters>"));
    }

    @Test
    public void testGetXmlStringWithNestedElements() throws Exception {
        String requestXml = getRequestNestedXml();
        XMLStreamReader xmlStreamReader = readXMLFromString(requestXml);
        XmlProcessor xmlProcessor = new XmlProcessor(xmlStreamReader);

        String xmlString = xmlProcessor.getXmlString();

        assertThat(xmlString, equalTo("<parameters>\n" +
                                      "\t\t\t  <spectrumOutOfScopeThreshold>4.0</spectrumOutOfScopeThreshold>\n" +
                                      "\t\t\t  <invalidPixelExpression>agc_flags.INVALID</invalidPixelExpression>\n" +
                                      "\t\t\t  <nested>\n" +
                                      "\t\t\t\t\t  <test1>agc_flags.INVALID</test1>\n" +
                                      "\t\t\t\t\t  <test2>agc_flags.INVALID</test2>\n" +
                                      "\t\t\t  </nested>\n" +
                                      "</parameters>"));
    }

    @Ignore
    @Test
    public void testGetXmlStringWithNoStartingParametersElement() throws Exception {
        String requestXml = getRequestXmlNoStartingParametersElement();
        XMLStreamReader xmlStreamReader = readXMLFromString(requestXml);
        XmlProcessor xmlProcessor = new XmlProcessor(xmlStreamReader);

        String xmlString = xmlProcessor.getXmlString();

        assertThat(xmlString, equalTo("<parameters>\n" +
                                      "\t\t\t  <spectrumOutOfScopeThreshold></spectrumOutOfScopeThreshold>\n" +
                                      "</parameters>"));
    }

    @Test
    public void testGetXmlStringWithEmptyElements() throws Exception {
        String requestXml = getRequestXmlEmptyElement();
        XMLStreamReader xmlStreamReader = readXMLFromString(requestXml);
        XmlProcessor xmlProcessor = new XmlProcessor(xmlStreamReader);

        String xmlString = xmlProcessor.getXmlString();

        assertThat(xmlString, equalTo("<parameters>\n" +
                                      "\t\t\t  <spectrumOutOfScopeThreshold></spectrumOutOfScopeThreshold>\n" +
                                      "</parameters>"));
    }

    @Test
    public void testGetXmlStringWithEmptyInput() throws Exception {
        String requestXml = "";
        XMLStreamReader xmlStreamReader = readXMLFromString(requestXml);
        XmlProcessor xmlProcessor = new XmlProcessor(xmlStreamReader);

        String xmlString = xmlProcessor.getXmlString();

        assertThat(xmlString, equalTo(""));
    }

    @Test
    public void testGetXmlStringWithNullXmlStreamReader() throws Exception {
        XmlProcessor xmlProcessor = new XmlProcessor(null);
        String xmlString = xmlProcessor.getXmlString();

        assertThat(xmlString, equalTo(""));
    }

    public XMLStreamReader readXMLFromString(final String xmlContent) throws XMLStreamException {
        final XMLInputFactory inputFactory = XMLInputFactory.newInstance();

        final StringReader reader = new StringReader(xmlContent);
        return inputFactory.createXMLStreamReader(reader);
    }

    private String getRequestNestedXml() {
        return "<parameters>\n" +
               "\t\t\t  <spectrumOutOfScopeThreshold>4.0</spectrumOutOfScopeThreshold>\n" +
               "\t\t\t  <invalidPixelExpression>agc_flags.INVALID</invalidPixelExpression>\n" +
               "\t\t\t  <nested>\n" +
               "\t\t\t\t\t  <test1>agc_flags.INVALID</test1>\n" +
               "\t\t\t\t\t  <test2>agc_flags.INVALID</test2>\n" +
               "\t\t\t  </nested>\n" +
               "</parameters>";
    }

    private String getRequestXml() {
        return "<parameters>\n" +
               "\t\t\t  <spectrumOutOfScopeThreshold>4.0</spectrumOutOfScopeThreshold>\n" +
               "\t\t\t  <invalidPixelExpression>agc_flags.INVALID</invalidPixelExpression>\n" +
               "</parameters>";
    }

    private String getRequestXmlNoStartingParametersElement() {
        return "<ComplexData>\n" +
               "\t\t<parameters>\n" +
               "\t\t\t  <doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
               "\t\t\t  <doSmileCorrection>true</doSmileCorrection>\n" +
               "\t\t\t  <outputTosa>false</outputTosa>\n" +
               "\t\t\t  <outputReflec>true</outputReflec>\n" +
               "\t\t\t  <outputReflecAs>RADIANCE_REFLECTANCES</outputReflecAs>\n" +
               "\t\t\t  <outputPath>true</outputPath>\n" +
               "\t\t\t  <outputTransmittance>false</outputTransmittance>\n" +
               "\t\t\t  <outputNormReflec>false</outputNormReflec>\n" +
               "\t\t\t  <landExpression>toa_reflec_10 &gt; toa_reflec_6 AND toa_reflec_13 &gt; 0.0475</landExpression>\n" +
               "\t\t\t  <cloudIceExpression>toa_reflec_14 &gt; 0.2</cloudIceExpression>\n" +
               "\t\t\t  <algorithm>REGIONAL</algorithm>\n" +
               "\t\t\t  <tsmConversionExponent>1.0</tsmConversionExponent>\n" +
               "\t\t\t  <tsmConversionFactor>1.73</tsmConversionFactor>\n" +
               "\t\t\t  <chlConversionExponent>1.04</chlConversionExponent>\n" +
               "\t\t\t  <chlConversionFactor>21.0</chlConversionFactor>\n" +
               "\t\t\t  <spectrumOutOfScopeThreshold>4.0</spectrumOutOfScopeThreshold>\n" +
               "\t\t\t  <invalidPixelExpression>agc_flags.INVALID</invalidPixelExpression>\n" +
               "\t\t</parameters>\n" +
               "\t\t</ComplexData>";
    }

    private String getRequestXmlEmptyElement() {
        return "<parameters>\n" +
               "\t\t\t  <spectrumOutOfScopeThreshold/>\n" +
               "</parameters>";
    }
}