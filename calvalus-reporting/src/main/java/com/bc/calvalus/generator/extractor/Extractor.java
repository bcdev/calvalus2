package com.bc.calvalus.generator.extractor;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.extractor.jobs.JobsType;
import com.bc.wps.utilities.PropertiesWrapper;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;

/**
 * @author muhammad.bc.
 */
public abstract class Extractor {

    private Properties properties;

    public abstract <T> HashMap<String, T> extractInfo(int from, int to, JobsType jobsType) throws GenerateLogException, JAXBException;

    public abstract <T> T getType(String jobId) throws JAXBException;

    public abstract String getXsltAsString();


    public Properties getProperties() {
        if (properties == null) {
            properties = createProperties();
        }
        return properties;
    }

    public static Properties createProperties() {
        try (InputStream resourceAsStream = Extractor.class.getClassLoader().getResourceAsStream("./conf/calvalus-reporting.properties")) {
            Properties properties = new Properties();
            properties.load(resourceAsStream);
            return properties;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getXSLTAsString(File file) throws FileNotFoundException {
        if (!file.exists()) {
            throw new FileNotFoundException("The file doest exist !!!");
        }
        StringBuilder stringBuilder = new StringBuilder();
        try (FileInputStream inputStream = new FileInputStream(file)) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }
        } catch (IOException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
        }
        return stringBuilder.toString();
    }

    protected String getFilterXMLWithXSLT(Reader reader, StreamSource xslSource) {
        try (StringWriter stringWriter = new StringWriter()) {
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer(xslSource);
            transformer.transform(new StreamSource(reader), new StreamResult(stringWriter));
            return stringWriter.toString();
        } catch (TransformerException | IOException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
        }
        return null;
    }

    protected Reader getReadFromSource(String sourceUrl) {
        //// todo: 28.12.2017 mba/ for test purpose only
        if (!sourceUrl.contains("master00")) {
            return new StringReader(sourceUrl);
        }
        ReadHistory readHistory = new ReadHistory(sourceUrl, FormatType.XML);
        String rawSource = readHistory.getRawSource();
        return new StringReader(rawSource);
    }

    protected <T> T extractInfo(String sourceURL, StreamSource xsltSource, Class<T> typeClass) throws JAXBException {
        Reader reader = getReadFromSource(sourceURL);
        String filterString = getFilterXMLWithXSLT(reader, xsltSource);
        JAXBContext jaxbContext = JAXBContext.newInstance(typeClass);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        return (T) unmarshaller.unmarshal(new StringReader(filterString));
    }

    protected String loadXSLTFile(String confXsl) {
        String xsltAsString = null;
        try {
            URL xsltFileUrl = Extractor.class.getClassLoader().getResource(PropertiesWrapper.get("cli.resource.directory") + "/" + confXsl);
            File xsltFile;
            if (xsltFileUrl != null) {
                xsltFile = new File(xsltFileUrl.toURI());
            } else {
                throw new FileNotFoundException("XSLT xsltFile does not exit.");
            }
            xsltAsString = getXSLTAsString(xsltFile);
        } catch (FileNotFoundException | URISyntaxException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
        }

        return xsltAsString;
    }

}
