package com.bc.calvalus.generator.extractor;


import com.bc.calvalus.generator.GenerateLogException;
import com.bc.calvalus.generator.extractor.jobs.JobsType;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.util.HashMap;
import java.util.Objects;

/**
 * @author muhammad.bc.
 */
public abstract class Extractor {


    private static JobsType jobsType;

    public Extractor() {
    }

    public Extractor(String sourceUrl) {
        if (Objects.isNull(jobsType)) {
            jobsType = getJobDetail(sourceUrl);
        }
    }

    private JobsType getJobDetail(String sourceUrl) {
        try {
            Reader reader = getReadFromSource(sourceUrl);
            JAXBContext jaxbContext = JAXBContext.newInstance(JobsType.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            return jobsType = (JobsType) unmarshaller.unmarshal(reader);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }


    protected String getFilterXMLWithXSLT(Reader reader, StreamSource xslSource) {
        try (StringWriter stringWriter = new StringWriter()) {
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer(xslSource);
            transformer.transform(new StreamSource(reader), new StreamResult(stringWriter));
            return stringWriter.toString();
        } catch (TransformerException | IOException e) {
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
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }

    protected Reader getReadFromSource(String sourceUrl) {
        //// todo: 28.12.2017 mba/ for test purpose only
        if (!sourceUrl.contains("master00")) {
            return new StringReader(sourceUrl);
        }
        LogReader logReader = new LogReader(sourceUrl, FormatType.XML);
        String rawSource = logReader.getRawSource();
        return new StringReader(rawSource);
    }

    protected <T> T extractInfo(String sourceURL, StreamSource xsltSource, Class<T> typeClass) throws JAXBException {
        Reader reader = getReadFromSource(sourceURL);
        String filterString = getFilterXMLWithXSLT(reader, xsltSource);
        JAXBContext jaxbContext = JAXBContext.newInstance(typeClass);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        return (T) unmarshaller.unmarshal(new StringReader(filterString));
    }

    //note mba for test purpose only
    public <T> T extractInfoFromXML(String sourceXML, String xsltAsString, Class<T> typeClass) throws JAXBException {
        Reader reader = getReadFromSource(sourceXML);
        StreamSource xsltSource = new StreamSource(new StringReader(xsltAsString));

        String filterString = getFilterXMLWithXSLT(reader, xsltSource);
        JAXBContext jaxbContext = JAXBContext.newInstance(typeClass);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        return (T) unmarshaller.unmarshal(new StringReader(filterString));
    }

    protected String loadXSLTFile(String confXsl) {
        String xsltAsString = null;
        try {
            File xsltFile = new File(getClass().getResource(confXsl).getFile());
            if (!xsltFile.exists()) {
                throw new FileNotFoundException("XSLT xsltFile does not exit.");
            }
            xsltAsString = getXSLTAsString(xsltFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return xsltAsString;
    }

    public JobsType getJobsType() {
        return jobsType;
    }

    public abstract <T> HashMap<String, T> extractInfo(JobsType jobsType) throws JAXBException;

    public abstract <T> HashMap<String, T> extractInfo(int from, int to, JobsType jobsType) throws GenerateLogException, JAXBException;

    public abstract <T> T getType(String jobId) throws JAXBException;

    public abstract String getXsltAsString();

}
