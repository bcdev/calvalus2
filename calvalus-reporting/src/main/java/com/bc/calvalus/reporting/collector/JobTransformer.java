package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.reporting.collector.exception.JobTransformerException;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author hans
 */
public class JobTransformer {

    private static final String CONF_XSL = "conf.xsl";
    private static final String COUNTER_XSL = "counter.xsl";

    private Transformer confTransformer;
    private Transformer counterTransformer;

    public JobTransformer() throws JobTransformerException {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        try {
            this.confTransformer = getTransformer(transformerFactory, CONF_XSL);
            this.counterTransformer = getTransformer(transformerFactory, COUNTER_XSL);
        } catch (IOException | TransformerConfigurationException exception) {
            throw new JobTransformerException(exception);
        }
    }

    public StringReader applyConfXslt(InputStream stream) throws JobTransformerException {
        StringWriter stringWriter = new StringWriter();
        try {
            this.confTransformer.transform(new StreamSource(stream), new StreamResult(stringWriter));
        } catch (TransformerException exception) {
            throw new JobTransformerException(exception);
        }
        return new StringReader(stringWriter.toString());
    }

    public StringReader applyCountersXslt(InputStream stream) throws JobTransformerException {
        StringWriter stringWriter = new StringWriter();
        try {
            this.counterTransformer.transform(new StreamSource(stream), new StreamResult(stringWriter));
        } catch (TransformerException exception) {
            throw new JobTransformerException(exception);
        }
        return new StringReader(stringWriter.toString());
    }

    private Transformer getTransformer(TransformerFactory transformerFactory, String xslFileName)
                throws IOException, TransformerConfigurationException {
        BufferedReader reader = Files.newBufferedReader(Paths.get(xslFileName));
        StreamSource xsltSource = new StreamSource(reader);
        return transformerFactory.newTransformer(xsltSource);
    }

}
