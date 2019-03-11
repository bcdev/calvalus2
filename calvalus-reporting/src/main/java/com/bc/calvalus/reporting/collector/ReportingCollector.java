package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.reporting.collector.exception.JobReportsException;
import com.bc.calvalus.reporting.collector.exception.JobTransformerException;
import com.bc.calvalus.reporting.collector.exception.ServerConnectionException;
import com.bc.calvalus.reporting.collector.types.Job;
import com.bc.calvalus.reporting.collector.types.JobConf;
import com.bc.calvalus.reporting.collector.types.JobCounters;
import com.bc.calvalus.reporting.collector.types.JobDetailType;
import com.bc.calvalus.reporting.collector.types.Jobs;
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
    private final StatusHandler statusHandler;

    private ReportingCollector(String propertiesName) throws IOException, JobTransformerException {
        PropertiesWrapper.loadConfigFile(propertiesName);
        this.historyServerClient = new HistoryServerClient();
        this.jobTransformer = new JobTransformer();
        this.statusHandler = new StatusHandler();
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
        this.statusHandler.initReport(jobReports.getKnownJobIdSet().size());
        boolean firstCycle = true;
        while (true) {
            try {
                Jobs jobs = retrieveAllJobs();
                Gson gson = new Gson();
                int counter = 0;
                for (Job job : jobs.getJob()) {
                    if (!jobReports.contains(job.getId())) {
                        JobConf conf = getConf(job);
                        JobCounters counters = getCounters(job);
                        JobDetailType jobDetailType = createJobDetailType(conf, counters, job);
                        String jobJsonString = gson.toJson(jobDetailType);
                        long finishTime = Long.parseLong(job.getFinishTime());
                        jobReports.add(job.getId(), finishTime, jobJsonString);
                        counter++;
                    }
                }
                if (counter > 0) {
                    LOGGER.info("Successfully added " + counter + " new job(s) to the reports file.");
                    this.statusHandler.updateNewJobNumber(jobReports.getKnownJobIdSet().size());
                } else {
                    LOGGER.info("No new jobs on the history server.");
                }
            } catch (ServerConnectionException exception) {
                if (firstCycle) {
                    throw exception;
                }
                LOGGER.log(Level.WARNING, "Problem when connecting to history server.", exception);
            }
            firstCycle = false;
            int pollInterval = PropertiesWrapper.getInteger("history.server.poll.interval");
            LOGGER.info("waiting for " + pollInterval / 1000 + " seconds for the next run...");
            try {
                Thread.sleep(pollInterval);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private JobCounters getCounters(Job job) throws JAXBException, ServerConnectionException, JobTransformerException {
        InputStream countersStream = this.historyServerClient.getCounters(job.getId());
        StringReader countersReader = this.jobTransformer.applyCountersXslt(countersStream);
        return (JobCounters) unmarshal(countersReader, JobCounters.class);
    }

    private JobConf getConf(Job job) throws JAXBException, ServerConnectionException, JobTransformerException {
        InputStream confStream = this.historyServerClient.getConf(job.getId());
        StringReader confReader = this.jobTransformer.applyConfXslt(confStream);
        return (JobConf) unmarshal(confReader, JobConf.class);
    }

    private JobDetailType createJobDetailType(JobConf conf, JobCounters counters, Job job) {
        JobDetailType jobDetailType = new JobDetailType();
        jobDetailType.setJobInfo(job);
        jobDetailType.setConfInfo(conf);
        jobDetailType.setCounterInfo(counters);
        return jobDetailType;
    }

    private Jobs retrieveAllJobs() throws JAXBException, ServerConnectionException {
        InputStream contentStream = this.historyServerClient.getAllJobs();
        JAXBContext jc = JAXBContext.newInstance(Jobs.class);
        return (Jobs) jc.createUnmarshaller().unmarshal(contentStream);
    }

    @SuppressWarnings("unchecked")
    private T unmarshal(StringReader reader, Class clazz) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(clazz);
        return (T) jc.createUnmarshaller().unmarshal(reader);
    }
}
