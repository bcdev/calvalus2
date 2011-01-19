package com.bc.calvalus.processing.shellexec;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Wrapper of an XSL Transformer configured by an XSL file.
 * It transforms Documents into Strings.
 * It supports XSLT 2.0 by using Saxon (8.8).
 *
 * @author Martin Boettcher
 */
public class XslTransformer {

    private final Transformer transformer;

    /**
     * Creates XSL Transformer from XSL file
     * @param  xsl  File with XSL definition
     * @throws IOException  if file cannot be ProcessUtil
     * @throws SAXException   if file is not well formed XML
     * @throws TransformerException  if Transformer factory (saxon) is not found
     *             or fails to create transformer from XSL
     */
    public XslTransformer(File xsl) throws IOException, SAXException, TransformerException {
        final Reader in = new FileReader(xsl);
        final TransformerFactory transformerFactory = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
        transformer = transformerFactory.newTransformer(new StreamSource(in));
        in.close();
    }

    /**
     * Applies transformation to Document
     * @param doc  parsed XML document
     * @return  String result of transformation
     * @throws TransformerException  if transformation fails
     */
    public String transform(Document doc) throws TransformerException {
        final Writer out = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(out));
        return out.toString();
    }

    /**
     * Sets transformation parameter that may be referenced in XSL document
     * @param key  parameter name
     * @param value  parameter value
     */
    public void setParameter(String key, Object value) {
        transformer.setParameter(key, value);
    }
}
