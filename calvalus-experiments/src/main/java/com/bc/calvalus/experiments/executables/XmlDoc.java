package com.bc.calvalus.experiments.executables;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.StringReader;

/**
 * Wrapper of an XML document for XPath access to values and nodes.
 *
 * @author Martin Boettcher
 */
public class XmlDoc {

    final Document doc;
    final XPath xpath = XPathFactory.newInstance().newXPath();

    public XmlDoc(String content) throws ParserConfigurationException, SAXException, IOException {
        doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new InputSource(new StringReader(content)))';
    }

    public Document getDocument() { return doc; }

    public String getString(String path) throws IllegalArgumentException, XPathExpressionException {
        String result = (String) xpath.evaluate(path, doc);
        if (result == null) throw new IllegalArgumentException("xpath " + path + " not found");
        return result;
    }

    public String getString(String path, Node node) throws IllegalArgumentException, XPathExpressionException {
        String result = (String) xpath.evaluate(path, node);
        if (result == null) throw new IllegalArgumentException("xpath " + path + " not found");
        return result;
    }

    public NodeList getNodes(String path) throws XPathExpressionException {
        return (NodeList) xpath.evaluate(path, doc, XPathConstants.NODESET);
    }
}
