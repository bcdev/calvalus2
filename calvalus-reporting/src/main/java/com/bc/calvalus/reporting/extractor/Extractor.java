package com.bc.calvalus.reporting.extractor;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.wps.utilities.PropertiesWrapper;
import java.util.List;
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
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.logging.Level;

/**
 * @author muhammad.bc.
 */
public abstract class Extractor {

    public abstract <T> HashMap<String, T> extractInfo(int from, int to, List<com.bc.calvalus.reporting.extractor.jobs.JobType> jobTypes) throws ExtractCalvalusReportException;

    public abstract <T> T getType(String jobId) throws JAXBException;

    public abstract String getXsltAsString();

    private String getXSLTAsString(File file) throws FileNotFoundException {
        if (!file.exists()) {
            throw new FileNotFoundException("The file does not exist !!!");
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

    private String getFilterXMLWithXSLT(Reader reader, StreamSource xslSource) {
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

    public Reader getReadFromSource(String sourceUrl, ReadFormatType readFormatType) {
        //// todo: 28.12.2017 mba/ for test purpose only
        if (!sourceUrl.contains("master00")) {
            return new StringReader(sourceUrl);
        }
        String response = null;
        try {
            ClientConnectionToHistory extractHistory = new ClientConnectionToHistory(sourceUrl, readFormatType);
            response = extractHistory.getResponse();
            extractHistory.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new StringReader(response);
    }

    public <T> T extractInfo(String sourceURL, StreamSource xsltSource, Class<T> typeClass) throws JAXBException {
        Reader reader = getReadFromSource(sourceURL, ReadFormatType.XML);
        String filterString = getFilterXMLWithXSLT(reader, xsltSource);
        assert filterString != null;
        JAXBContext jaxbContext = JAXBContext.newInstance(typeClass);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        return (T) unmarshaller.unmarshal(new StringReader(filterString));
    }

    public String loadXSLTFile(String confXsl) {
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
