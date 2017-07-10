package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.collector.exception.JobReportsException;
import com.bc.calvalus.reporting.collector.exception.JobTransformerException;
import com.bc.calvalus.reporting.collector.exception.ServerConnectionException;
import com.bc.calvalus.reporting.collector.types.ConfType;
import com.bc.calvalus.reporting.collector.types.CountersType;
import com.bc.calvalus.reporting.collector.types.JobDetailType;
import com.bc.calvalus.reporting.collector.types.JobType;
import com.bc.calvalus.reporting.collector.types.JobsType;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class ReportingCollector<T> {

    private static final Logger LOGGER = CalvalusLogger.getLogger();
    private static final String REPORTING_COLLECTOR_PROPERTIES = "reporting-collector.properties";
    private static JobReports jobReports;

    private final HistoryServerClient historyServerClient;
    private final JobTransformer jobTransformer;

    private ReportingCollector(String propertiesName) throws IOException, JobTransformerException {
        PropertiesWrapper.loadConfigFile(propertiesName);
        this.historyServerClient = new HistoryServerClient();
        this.jobTransformer = new JobTransformer();
        jobReports = new JobReports();
    }

    public static void main(String[] args) {
        try {
            new ReportingCollector(args.length > 0 ? args[0] : REPORTING_COLLECTOR_PROPERTIES).run();
        } catch (IOException exception) {
            LOGGER.log(Level.SEVERE, "Problem when loading the configuration file.", exception);
            System.exit(1);
        } catch (JobReportsException exception) {
            LOGGER.log(Level.SEVERE, "Problem when accessing the report file.", exception);
            System.exit(1);
        } catch (ServerConnectionException exception) {
            LOGGER.log(Level.SEVERE, "Problem when connecting to history server.", exception);
            System.exit(1);
        } catch (JAXBException exception) {
            LOGGER.log(Level.SEVERE, "Problem when unmarshalling XML.", exception);
            System.exit(1);
        } catch (JobTransformerException exception) {
            LOGGER.log(Level.SEVERE, "Problem when dealing with XSL transformation.", exception);
            System.exit(1);
        } finally {
            jobReports.closeBufferedWriters();
        }
    }

    private void run() throws JobReportsException, ServerConnectionException, JAXBException, JobTransformerException {
        jobReports.init(PropertiesWrapper.get("reporting.folder.path"));
        while (true) {
            JobsType jobs = retrieveAllJobs();
            Gson gson = new Gson();
            int counter = 0;
            for (JobType job : jobs.getJob()) {
                if (!jobReports.contains(job.getId())) {
                    ConfType conf = getConf(job);
                    CountersType counters = getCounters(job);
                    JobDetailType jobDetailType = createJobDetailType(conf, counters, job);
                    String jobJsonString = gson.toJson(jobDetailType);
                    long finishTime = Long.parseLong(job.getFinishTime());
                    jobReports.add(job.getId(), finishTime, jobJsonString);
                    counter++;
                }
            }
            jobReports.flushBufferedWriters();
            if (counter > 0) {
                LOGGER.info("Successfully added " + counter + " new job(s) to the reports file.");
            } else {
                LOGGER.info("No new jobs on the history server.");
            }
            int pollInterval = PropertiesWrapper.getInteger("history.server.poll.interval");
            LOGGER.info("waiting for " + pollInterval / 1000 + " seconds for the next run...");
            try {
                Thread.sleep(pollInterval);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private CountersType getCounters(JobType job) throws JAXBException, ServerConnectionException, JobTransformerException {
        InputStream countersStream = this.historyServerClient.getCounters(job.getId());
        StringReader countersReader = this.jobTransformer.applyCountersXslt(countersStream);
        return (CountersType) unmarshal(countersReader, CountersType.class);
    }

    private ConfType getConf(JobType job) throws JAXBException, ServerConnectionException, JobTransformerException {
        InputStream confStream = this.historyServerClient.getConf(job.getId());
        StringReader confReader = this.jobTransformer.applyConfXslt(confStream);
        return (ConfType) unmarshal(confReader, ConfType.class);
    }

    private JobDetailType createJobDetailType(ConfType conf, CountersType counters, JobType job) {
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setJobInfo(job);
        jobDetailType.setConfInfo(conf);
        jobDetailType.setCounterInfo(counters);
        return jobDetailType;
    }

    private JobsType retrieveAllJobs() throws JAXBException, ServerConnectionException {
        InputStream contentStream = this.historyServerClient.getAllJobs();
        JAXBContext jc = JAXBContext.newInstance(JobsType.class);
        return (JobsType) jc.createUnmarshaller().unmarshal(contentStream);
    }

    @SuppressWarnings("unchecked")
    private T unmarshal(StringReader reader, Class clazz) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(clazz);
        return (T) jc.createUnmarshaller().unmarshal(reader);
    }
}
