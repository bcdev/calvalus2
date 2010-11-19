package com.bc.calvalus.experiments.executables;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.transform.Transformer;
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
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class XslTransformer {

    final Transformer transformer;

    public XslTransformer(File xsl) throws IOException, SAXException {
        Reader in = new FileReader(xsl);
        transformer = TransformerFactory.newInstance().newTransformer(new StreamSource(in));
        in.close();
    }

    public String transform(Document doc) {
        Writer out = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(out));
        return out.toString();
    }

    public void setParameter(String key, Object value) {
        transformer.setParameter(key, value);
    }
}
