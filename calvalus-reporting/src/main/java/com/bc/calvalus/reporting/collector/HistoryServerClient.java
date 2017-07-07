package com.bc.calvalus.reporting.collector;

import com.bc.calvalus.reporting.collector.exception.ServerConnectionException;
import com.bc.wps.utilities.PropertiesWrapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author hans
 */
class HistoryServerClient {

    private static final String RETRIEVE_ALL_JOBS_URL = PropertiesWrapper.get("retrieve.all.jobs.url");
    private static final String RETRIEVE_CONF_URL = PropertiesWrapper.get("retrieve.configuration.url");
    private static final String RETRIEVE_COUNTERS_URL = PropertiesWrapper.get("retrieve.counters.url");

    InputStream getAllJobs() throws ServerConnectionException {
        try {
            return getContentInputStream(RETRIEVE_ALL_JOBS_URL);
        } catch (IOException exception) {
            throw new ServerConnectionException(exception);
        }
    }

    InputStream getConf(String jobId) throws ServerConnectionException {
        try {
            return getContentInputStream(String.format(RETRIEVE_CONF_URL, jobId));
        } catch (IOException exception) {
            throw new ServerConnectionException(exception);
        }
    }

    InputStream getCounters(String jobId) throws ServerConnectionException {
        try {
            return getContentInputStream(String.format(RETRIEVE_COUNTERS_URL, jobId));
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
