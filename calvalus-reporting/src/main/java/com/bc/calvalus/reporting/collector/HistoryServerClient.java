package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.util.PropertiesWrapper;
import com.bc.calvalus.reporting.collector.exception.ServerConnectionException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * @author hans
 */
class HistoryServerClient {

    private static final String RETRIEVE_ALL_JOBS_URL = PropertiesWrapper.get("retrieve.all.jobs.url");
    private static final String RETRIEVE_CONF_URL_TEMPLATE = PropertiesWrapper.get("retrieve.configuration.url");
    private static final String RETRIEVE_COUNTERS_URL_TEMPLATE = PropertiesWrapper.get("retrieve.counters.url");

    private static final Logger LOGGER = CalvalusLogger.getLogger();

    InputStream getAllJobs() throws ServerConnectionException {
        try {
            LOGGER.info("Retrieving all jobs from '" + RETRIEVE_ALL_JOBS_URL + "'");
            return getContentInputStream(RETRIEVE_ALL_JOBS_URL);
        } catch (IOException exception) {
            throw new ServerConnectionException(exception);
        }
    }

    InputStream getConf(String jobId) throws ServerConnectionException {
        try {
            String confUrl = String.format(RETRIEVE_CONF_URL_TEMPLATE, jobId);
            LOGGER.info("Retrieving configuration of job '" + jobId + "' from '" + confUrl + "'");
            return getContentInputStream(confUrl);
        } catch (IOException exception) {
            throw new ServerConnectionException(exception);
        }
    }

    InputStream getCounters(String jobId) throws ServerConnectionException {
        try {
            String countersUrl = String.format(RETRIEVE_COUNTERS_URL_TEMPLATE, jobId);
            LOGGER.info("Retrieving counters of job '" + jobId + "' from '" + countersUrl + "'");
            return getContentInputStream(countersUrl);
        } catch (IOException exception) {
            throw new ServerConnectionException(exception);
        }
    }

    private InputStream getContentInputStream(String url) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        httpGet.setHeader("Accept", "application/xml");
        CloseableHttpResponse response = httpclient.execute(httpGet);
        return response.getEntity().getContent();
    }

}
