package com.bc.calvalus.processing.shellexec;

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
 * The Document is built from a String provided as constructor parameter.
 * The getX methods handle missing values of required parameters by
 * throwing IllegalArgumentException with a proper error message.
 *
 * @author Martin Boettcher
 */
public class XmlDoc {

    final Document doc;
    final XPath xpath = XPathFactory.newInstance().newXPath();

    /**
     * Creates Document from String
     * @param content  XML string to be parsed
     * @throws ParserConfigurationException  if application should have configured
     *             non-default document builder and factory fails
     * @throws SAXException  if XML string is not well-formed
     * @throws IOException  should never occur when reading from String
     */
    public XmlDoc(String content) throws ParserConfigurationException, SAXException, IOException {
        doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new InputSource(new StringReader(content)));
    }

    /** Returns Document node */
    public Document getDocument() { return doc; }

    /**
     * Returns XPath value for document as string, verifies that path exists
     * @param path  XPath expression, e.g. "//someelement[@someattribute='somevalue']"
     * @return  string content of addressed element or attribute
     * @throws IllegalArgumentException  path does not point to value in document, value would be null
     * @throws XPathExpressionException  XPath expression not well formed
     */
    public String getString(String path) throws IllegalArgumentException, XPathExpressionException {
        String result = xpath.evaluate(path, doc);
        if (result == null) throw new IllegalArgumentException("xpath " + path + " not found");
        return result;
    }

    /**
     * Returns XPath value for document as string,
     * @param path  XPath expression, e.g. "//someelement[@someattribute='somevalue']"
     * @return  string content of addressed element or attribute, null if path does not exist in document
     * @throws XPathExpressionException  XPath expression not well formed
     */
    public String getString(String path, String defaultValue) throws XPathExpressionException {
        String result = xpath.evaluate(path, doc);
        if (result == null) {
            return defaultValue;
        } else {
            return result;
        }
    }

    /**
     * Returns XPath value starting at node as string, verifies that path exists
     * @param path  XPath expression, e.g. "//someelement[@someattribute='somevalue']"
     * @param node  start node to apply path to
     * @return  string content of addressed element or attribute
     * @throws IllegalArgumentException  path does not point to value below node, value would be null
     * @throws XPathExpressionException  XPath expression not well formed
     */
    public String getString(String path, Node node) throws IllegalArgumentException, XPathExpressionException {
        String result = xpath.evaluate(path, node);
        if (result == null) {
            throw new IllegalArgumentException("xpath " + path + " not found");
        }
        return result;
    }

    /**
     * Returns XPath value starting at node as string.
     * @param path  XPath expression, e.g. "//someelement[@someattribute='somevalue']"
     * @param node  start node to apply path to
     * @return  string content of addressed element or attribute, null if path does not exist
     * @throws XPathExpressionException  XPath expression not well formed
     */
    public String getString(String path, Node node, String defaultValue) throws IllegalArgumentException, XPathExpressionException {
        String result = xpath.evaluate(path, node);
        if (result == null) {
            return defaultValue;
        } else {
            return result;
        }
    }


    /**
     * Returns list of nodes at XPath expression in document
     * @param path  XPath expression, e.g. "//someelement[@someattribute='somevalue']"
     * @return  NodeList of nodes found
     * @throws XPathExpressionException  XPath expression not well formed
     */
    public NodeList getNodes(String path) throws XPathExpressionException {
        return (NodeList) xpath.evaluate(path, doc, XPathConstants.NODESET);
    }

    /**
     * Returns node at XPath expression in document
     * @param path  XPath expression, e.g. "//someelement[@someattribute='somevalue']"
     * @return  node of addressed element
     * @throws XPathExpressionException  XPath expression not well formed
     */
    public Node getNode(String path) throws XPathExpressionException {
        return (Node) xpath.evaluate(path, doc, XPathConstants.NODE);
    }
}
