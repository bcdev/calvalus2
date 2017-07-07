package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.collector.exception.JobReportsException;
import com.bc.calvalus.reporting.collector.exception.ServerConnectionException;
import com.bc.calvalus.reporting.collector.types.ConfType;
import com.bc.calvalus.reporting.collector.types.CountersType;
import com.bc.calvalus.reporting.collector.types.JobDetailType;
import com.bc.calvalus.reporting.collector.types.JobType;
import com.bc.calvalus.reporting.collector.types.JobsType;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class ReportingCollector<T> {

    private static final Logger LOGGER = CalvalusLogger.getLogger();
    private static final int HISTORY_SERVER_POLL_INTERVAL = PropertiesWrapper.getInteger("history.server.poll.interval");
    private static final String REPORT_FILE_PATH = PropertiesWrapper.get("report.file.path");
    private static final String RETRIEVE_ALL_JOBS_URL = PropertiesWrapper.get("retrieve.all.jobs.url");
    private static final String RETRIEVE_CONF_URL = PropertiesWrapper.get("retrieve.configuration.url");
    private static final String RETRIEVE_COUNTERS_URL = PropertiesWrapper.get("retrieve.counters.url");
    private static final String REPORTING_COLLECTOR_PROPERTIES = "reporting-collector.properties";
    private static final String CONF_XSL = "conf.xsl";
    private static final String COUNTER_XSL = "counter.xsl";
    private static final JobReports jobReports = new JobReports();

    private Transformer confTransformer;
    private Transformer counterTransformer;

    private ReportingCollector() throws IOException, TransformerConfigurationException {
        PropertiesWrapper.loadConfigFile(REPORTING_COLLECTOR_PROPERTIES);
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        this.confTransformer = getTransformer(transformerFactory, CONF_XSL);
        this.counterTransformer = getTransformer(transformerFactory, COUNTER_XSL);
    }

    public static void main(String[] args) {
        try {
            new ReportingCollector().run();
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Problem when loading the configuration file.", exception);
            System.exit(1);
        } catch (JobReportsException exception) {
            LOGGER.log(Level.SEVERE, "Problem when accessing the report file.", exception);
            System.exit(1);
        } catch (ServerConnectionException exception) {
            LOGGER.log(Level.SEVERE, "Problem when connecting to history server.", exception);
            System.exit(1);
        } catch (TransformerConfigurationException exception) {
            LOGGER.log(Level.SEVERE, "Problem when creating a transformer.", exception);
            System.exit(1);
        } finally {
            jobReports.closeBufferedWriter();
        }
    }

    private void run() throws JobReportsException, ServerConnectionException {
        jobReports.init(REPORT_FILE_PATH);
        while (true) {
            try {
                JobsType jobs = retrieveAllJobs(RETRIEVE_ALL_JOBS_URL);
                for (JobType job : jobs.getJobs()) {
                    if (jobReports.contains(job.getId())) {
                        InputStream confStream = getContentInputStream(String.format(RETRIEVE_CONF_URL, job.getId()));
                        StringReader confReader = applyXslt(this.confTransformer, confStream);
                        ConfType conf = (ConfType) unmarshal(confReader, ConfType.class);
                        InputStream countersStream = getContentInputStream(String.format(RETRIEVE_COUNTERS_URL, job.getId()));
                        StringReader countersReader = applyXslt(this.counterTransformer, countersStream);
                        CountersType counters = (CountersType) unmarshal(countersReader, CountersType.class);
                        JobDetailType jobDetailType = createJobDetailType(conf, counters, job);
                        Gson gson = new Gson();
                        String jobJsonString = gson.toJson(jobDetailType);
                        jobReports.add(job.getId(), jobJsonString);
                    }
                }
                jobReports.flushBufferedWriter();
            } catch (IOException exception) {
                LOGGER.log(Level.SEVERE, "HTTP request failed.", exception);
                throw new ServerConnectionException(exception);
            } catch (JAXBException exception) {
                LOGGER.log(Level.SEVERE, "Unable to initialize JAXB.", exception);
                throw new ServerConnectionException(exception);
            } catch (TransformerException exception) {
                LOGGER.log(Level.SEVERE, "Unable to apply XSL transformation.", exception);
                throw new ServerConnectionException(exception);
            }

            try {
                Thread.sleep(HISTORY_SERVER_POLL_INTERVAL);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private JobDetailType createJobDetailType(ConfType conf, CountersType counters, JobType job) {
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setJobInfo(job);
        jobDetailType.setConfInfo(conf);
        jobDetailType.setCounterInfo(counters);
        return jobDetailType;
    }

    private StringReader applyXslt(Transformer transformer, InputStream stream) throws TransformerException {
        StringWriter stringWriter = new StringWriter();
        transformer.transform(new StreamSource(stream), new StreamResult(stringWriter));
        return new StringReader(stringWriter.toString());
    }

    private Transformer getTransformer(TransformerFactory transformerFactory, String xslFileName)
                throws IOException, TransformerConfigurationException {
        BufferedReader reader = Files.newBufferedReader(Paths.get(xslFileName));
        StreamSource xsltSource = new StreamSource(reader);
        return transformerFactory.newTransformer(xsltSource);
    }

    private JobsType retrieveAllJobs(String url) throws IOException, JAXBException {
        InputStream contentStream = getContentInputStream(url);
        JAXBContext jc = JAXBContext.newInstance(JobsType.class);
        return (JobsType) jc.createUnmarshaller().unmarshal(contentStream);
    }

    @SuppressWarnings("unchecked")
    private T unmarshal(StringReader reader, Class clazz) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(clazz);
        return (T) jc.createUnmarshaller().unmarshal(reader);
    }

    private InputStream getContentInputStream(String url) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = httpclient.execute(httpGet);
        return response.getEntity().getContent();
    }
}
