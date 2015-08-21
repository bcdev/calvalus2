package com.bc.calvalus.wps.utility;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Created by hans on 22.07.2015.
 */
public class XmlProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(XmlProcessor.class);
    private static final String COMPLEX_DATA_ELEMENT_NAME = "ComplexData";

    private final XMLStreamReader xmlStreamReader;

    /**
     * Constructor.
     *
     * @param xmlStreamReader XMLStreamReader object that contains an XML-type parameter value.
     */
    public XmlProcessor(XMLStreamReader xmlStreamReader) {
        this.xmlStreamReader = xmlStreamReader;
    }

    /**
     * Converts XMLStreamReader to an XML string, preserving all the whitespaces.
     *
     * @return XML String.
     */
    public String getXmlString() {
        String xmlString = "";
        if (xmlStreamReader == null) {
            logError("xmlStreamReader is null.");
            return xmlString;
        }
        return constructXmlString(xmlString);
    }

    private String constructXmlString(String xmlString) {
        try {
            while (xmlStreamReader.hasNext()) {
                if (xmlStreamReader.isStartElement()) {
                    xmlString += getStartElement();
                }
                if (xmlStreamReader.isCharacters()) {
                    xmlString += getElementContent();
                }
                if (xmlStreamReader.isEndElement()) {
                    if (xmlStreamReader.getLocalName().equals(COMPLEX_DATA_ELEMENT_NAME)) {
                        break;
                    }
                    xmlString += getEndElement();
                }
                xmlStreamReader.next();
            }
        } catch (XMLStreamException exception) {
            logError("There is an error when constructing the XML String from xmlStreamReader: " + exception.getMessage());
            return "";
        }
        return xmlString;
    }

    private void logError(String errorMessage) {
        LOG.error(errorMessage);
    }

    private String getElementContent() {
        return xmlStreamReader.getText();
    }

    private String getEndElement() {
        return "</" + xmlStreamReader.getLocalName() + ">";
    }

    private String getStartElement() throws XMLStreamException {
        return "<" + xmlStreamReader.getLocalName() + ">";
    }

}
