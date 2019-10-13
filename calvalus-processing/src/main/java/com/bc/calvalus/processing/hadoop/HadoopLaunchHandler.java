package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.processing.executable.KeywordHandler;
import com.bc.ceres.core.ProcessObserver;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class HadoopLaunchHandler {

    private static final int SHUTDOWN_DELAY = 300;
    private static final int SUPERVISOR_PERIOD = 60;

    private static Logger LOG = CalvalusLogger.getLogger();

    enum ClusterState {
        DOWN,
        STARTING,
        UP,
        IDLE,
        STOPPING,
        UNKNOWN
    }

    private HadoopProcessingService hadoopProcessingService;
    private Configuration configuration;
    private ClusterState clusterState = ClusterState.DOWN;
    private long idleSince = 0;
    private List<HadoopWorkflowItem> workflowItemQueue = Collections.synchronizedList(new ArrayList<>());

    public HadoopLaunchHandler(Configuration configuration) {
        this.configuration = configuration;
    }

    public HadoopLaunchHandler(HadoopProcessingService hadoopProcessingService, Configuration configuration) {
        this.hadoopProcessingService = hadoopProcessingService;
        this.configuration = configuration;
        hadoopProcessingService.getTimer().scheduleAtFixedRate(new ClusterSupervisorTask(), 10000, configuration.getInt("calvalus.launcher.supervisor-period", SUPERVISOR_PERIOD) * 1000);
    }

    public void queueWorkflowItem(HadoopWorkflowItem workflowItem) {
        LOG.info("queueWorkflowItem while cluster " + clusterState);
        workflowItemQueue.add(workflowItem);
        synchronized (this) {
            switch (clusterState) {
                case DOWN:
                    hadoopProcessingService.getTimer().schedule(new ClusterStartTask(), 0);
                    break;
                case IDLE:
                    clusterState = ClusterState.UP;
                    // fall through
                case UP:
                    hadoopProcessingService.getTimer().schedule(new SubmitTask(), 0);
                    break;
            }
        }
    }

    class SubmitTask extends TimerTask {
        @Override
        public void run() {
            LOG.info("SubmitTask started " + clusterState);
            try {
                synchronized (HadoopLaunchHandler.this) {
                    if (clusterState != ClusterState.UP && clusterState != ClusterState.IDLE) {
                        return;
                    }
                    while (!workflowItemQueue.isEmpty()) {
                        HadoopWorkflowItem workflowItem;
                        workflowItem = workflowItemQueue.get(0);
                        workflowItem.getJobConfig().set("yarn.resourcemanager.hostname", configuration.get("yarn.resourcemanager.hostname"));
                        workflowItem.getJobConfig().set("yarn.resourcemanager.address", configuration.get("yarn.resourcemanager.address"));
                        workflowItem.getJobConfig().set("mapreduce.jobhistory.address", configuration.get("mapreduce.jobhistory.address"));
                        workflowItem.getJobConfig().set("mapreduce.jobhistory.webapp.address", configuration.get("mapreduce.jobhistory.webapp.address"));
                        workflowItem.getJobConfig().set("yarn.log.server.url", configuration.get("yarn.log.server.url"));
                        workflowItem.getJobConfig().unset("yarn.resourcemanager.scheduler.address");
                        workflowItem.getJobConfig().unset("yarn.resourcemanager.resource-tracker.address");
                        workflowItem.getJobConfig().unset("yarn.resourcemanager.admin.address");
                        workflowItem.submitInternal();
                        workflowItemQueue.remove(0);
                        if (workflowItemQueue.isEmpty()) {
                            break;
                        }
                    }
                    clusterState = ClusterState.UP;
                }
            } catch (WorkflowException e) {
                LOG.warning("SubmitTask error: " + e);
                synchronized (HadoopLaunchHandler.this) {
                    clusterState = ClusterState.UNKNOWN;
                    hadoopProcessingService.getTimer().schedule(new ClusterStopTask(), 0);
                }
            } finally {
                LOG.info("SubmitTask finished " + clusterState);
            }
        }
    }

    class ClusterStartTask extends TimerTask {
        @Override
        public void run() {
            LOG.info("StartTask started " + clusterState);
            try {
                synchronized (HadoopLaunchHandler.this) {
                    if (clusterState != ClusterState.DOWN) {
                        return;
                    }
                    clusterState = ClusterState.STARTING;
                }
                startCluster();
                synchronized (HadoopLaunchHandler.this) {
                    clusterState = ClusterState.UP;
                    hadoopProcessingService.getTimer().schedule(new SubmitTask(), 0);
                }
            } catch (Exception e) {
                e.printStackTrace();
                synchronized (HadoopLaunchHandler.this) {
                    clusterState = ClusterState.UNKNOWN;
                    hadoopProcessingService.getTimer().schedule(new ClusterStopTask(), 0);
                }
            } finally {
                LOG.info("StartTask finished " + clusterState);
            }
        }
    }

    class ClusterStopTask extends TimerTask {
        @Override
        public void run() {
            try {
                LOG.info("StopTask started " + clusterState);
                synchronized (HadoopLaunchHandler.this) {
                    if (clusterState != ClusterState.IDLE && clusterState != ClusterState.UNKNOWN) {
                        return;
                    }
                    clusterState = clusterState.STOPPING;
                }
                stopCluster();
                synchronized (HadoopLaunchHandler.this) {
                    clusterState = ClusterState.DOWN;
                }
            } catch (Exception e) {
                clusterState = ClusterState.UNKNOWN;
            } finally {
                LOG.info("StopTask finished " + clusterState);
            }
        }
    }

    class ClusterSupervisorTask extends TimerTask {

        @Override
        public void run() {
            try {
                LOG.info("ClusterSupervisorTask started " + clusterState);
                synchronized (HadoopLaunchHandler.this) {
                    switch (clusterState) {
                        case UNKNOWN:
                            hadoopProcessingService.getTimer().schedule(new ClusterStopTask(), 0);
                            break;
                        case DOWN:
                            if (!workflowItemQueue.isEmpty()) {
                                hadoopProcessingService.getTimer().schedule(new ClusterStartTask(), 0);
                            }
                            break;
                        case IDLE:
                            if (! workflowItemQueue.isEmpty()) {
                                // should never happen ...
                                clusterState = ClusterState.UP;
                                hadoopProcessingService.getTimer().schedule(new SubmitTask(), 0);
                            } else if (System.currentTimeMillis() > idleSince + configuration.getInt("calvalus.launcher.shutdown-delay", SHUTDOWN_DELAY) * 1000) {
                                hadoopProcessingService.getTimer().schedule(new ClusterStopTask(), 0);
                            }
                            break;
                        case UP:
                            try {
                                if (workflowItemQueue.isEmpty() && retrieveNoOfRunningTasks() == 0) {
                                    clusterState = ClusterState.IDLE;
                                    idleSince = System.currentTimeMillis();
                                    LOG.info("idle since " + DateUtils.ISO_FORMAT.format(new Date(idleSince)));
                                }
                            } catch (Exception e) {
                                LOG.warning("failed to retrieve hadoop metrics: " + e);
                                clusterState = ClusterState.IDLE;
                                idleSince = System.currentTimeMillis();
                            }
                    }
                }
            } finally {
                LOG.info("ClusterSupervisorTask finished " + clusterState);
            }
        }
    }

    int retrieveNoOfRunningTasks() throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        final HttpClient httpClient = new HttpClient();
        final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        final XPathFactory xPathfactory = XPathFactory.newInstance();
        String masterHost = configuration.get("yarn.resourcemanager.hostname", "cdt1");
        final String searchUrl = "http://" + masterHost + ":8088/ws/v1/cluster/metrics";
        final GetMethod metricsRequest = new GetMethod(searchUrl);
        metricsRequest.setRequestHeader("Accept", "application/xml");
        final int statusCode = httpClient.executeMethod(metricsRequest);
        if (statusCode > 299) {
            throw new IOException("search error: " + metricsRequest.getResponseBodyAsString() + " query: " + metricsRequest.getQueryString());
        }
        InputStream response = metricsRequest.getResponseBodyAsStream();
        DocumentBuilder builder = docFactory.newDocumentBuilder();
        Document doc = builder.parse(response);
        XPath xpath = xPathfactory.newXPath();
        XPathExpression expr = xpath.compile("/clusterMetrics/allocatedVirtualCores");
        String coresString = (String) expr.evaluate(doc, XPathConstants.STRING);
        return Integer.parseInt(coresString);
    }

    static class ExternalIpHandler extends KeywordHandler {
        public ExternalIpHandler(String programName, Configuration configuration) {
            super(programName, new MapContextImpl(configuration, new TaskAttemptID("", 0, false, 0, 0), null, null, null, null, null) { public void progress() {} });
            //# ip 80.158.3.197 is assigned to vm cdt1
            outputProductPattern = Pattern.compile("# ip (.*) is assigned to vm cdt1");
        }

        @Override
        public void onStdoutLineReceived(ProcessObserver.ObservedProcess process, String line, ProgressMonitor pm) {
            //super.onStdoutLineReceived(process, line, pm);
            LOG.info(line);
            Matcher outputProductMatcher = outputProductPattern.matcher(line);
            if (outputProductMatcher.find()) {
                outputFiles.add(outputProductMatcher.group(1).trim());
            }
        }

        @Override
        public void onStderrLineReceived(ProcessObserver.ObservedProcess process, String line, ProgressMonitor pm) {
            //super.onStderrLineReceived(process, line, pm);
            LOG.warning(line);
        }
    }

    void startCluster() throws IOException, InterruptedException {
        String startCmd = configuration.get("calvalus.openstack.startcmd");
        LOG.info(startCmd);
        Process process = Runtime.getRuntime().exec(startCmd, null, new File(configuration.get("calvalus.openstack.workdir")));
        KeywordHandler keywordHandler = new ExternalIpHandler(startCmd, configuration);
        new ProcessObserver(process).setHandler(keywordHandler).start();
        if (process.waitFor() != 0) {
            throw new IOException(startCmd + " failed");
        }
        String[] outputFiles = keywordHandler.getOutputFiles();
        if (outputFiles.length < 1) {
            throw new IOException("external IP of master not found");
        }
        configuration.set("yarn.resourcemanager.hostname", outputFiles[0]);
        configuration.set("yarn.resourcemanager.address", outputFiles[0] + ":8032");
        configuration.set("mapreduce.jobhistory.address", outputFiles[0] + ":10020");
        configuration.set("mapreduce.jobhistory.webapp.address", outputFiles[0] + ":19888");
        configuration.set("yarn.log.server.url", outputFiles[0] + ":19888/jobhistory/logs");
        hadoopProcessingService.clearCache();
    }

    void stopCluster() throws IOException, InterruptedException {
        String stopCmd = configuration.get("calvalus.openstack.startcmd").replaceAll("startup", "shutdown");
        LOG.info(stopCmd);
        Process process = Runtime.getRuntime().exec(stopCmd,null, new File(configuration.get("calvalus.openstack.workdir")));
        new ProcessObserver(process).start();
        if (process.waitFor() != 0) {
            throw new IOException(stopCmd + " failed");
        }
        configuration.unset("yarn.resourcemanager.hostname");
        configuration.unset("yarn.resourcemanager.address");
        configuration.unset("mapreduce.jobhistory.address");
        configuration.unset("mapreduce.jobhistory.webapp.address");
        configuration.unset("yarn.log.server.url");
    }
}
