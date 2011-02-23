package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.net.NetUtils;

import javax.servlet.ServletContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;

/**
 * An BackendService implementation that delegates to a Hadoop cluster.
 */
public class HadoopBackendService implements BackendService {

    private final ServletContext servletContext;
    private final Configuration hadoopConf;
    private final JobClient jobClient;

    public HadoopBackendService(ServletContext servletContext) throws IOException {
        this.servletContext = servletContext;
        this.hadoopConf = new Configuration();
        initHadoopConf(servletContext);
        String target = hadoopConf.get("mapred.job.tracker", "localhost:9001");
        InetSocketAddress trackerAddr = NetUtils.createSocketAddr(target);
        this.jobClient = new JobClient(trackerAddr, hadoopConf);
    }

    private void initHadoopConf(ServletContext servletContext) {
        hadoopConf.reloadConfiguration();
        Enumeration elements = servletContext.getInitParameterNames();
        while (elements.hasMoreElements()) {
            String name = (String) elements.nextElement();
            if (name.startsWith("calvalus.hadoop.")) {
                String hadoopName = name.substring("calvalus.hadoop.".length());
                String hadoopValue = servletContext.getInitParameter(name);
                hadoopConf.set(hadoopName, hadoopValue);
                servletContext.log("Using Hadoop configuration: " + hadoopName + " = " + hadoopValue);
            }
        }
    }

    @Override
    public PortalProductSet[] getProductSets(String type) throws BackendServiceException {
        return new PortalProductSet[]{
                new PortalProductSet("id1", "MERIS_RR__1P", "All MERIS RR L1b"),
                new PortalProductSet("id2", "MERIS_RR__1P", "MERIS RR L1b 2004"),
        };
    }

    @Override
    public PortalProcessor[] getProcessors(String type) throws BackendServiceException {
        return new PortalProcessor[]{
                new PortalProcessor("id1", "MERIS_RR__1P", "MERIS Case2R", new String[]{"1.0"}),
        };
    }

    @Override
    public PortalProduction[] getProductions(String type) throws BackendServiceException {
        try {
            // todo - this is still wrong, must first load all (persistent) productions,
            //        then retrieve its Hadoop queue-ID and then call jobClient.getJobsFromQueue();
            JobStatus[] jobStatuses = jobClient.getAllJobs();
            ArrayList<PortalProduction> productions = new ArrayList<PortalProduction>();
            for (int i = 0; i < jobStatuses.length; i++) {
                JobStatus jobStatus = jobStatuses[i];
                RunningJob runningJob = jobClient.getJob(jobStatus.getJobID());
                String productionId = jobStatus.getJobID().toString();
                System.out.printf("Production %d: %s (id=%s)%n", (i + 1), runningJob.getJobName(), productionId);
                productions.add(new PortalProduction(productionId,
                                                     runningJob.getJobName(),
                                                     createWorkStatus(productionId, jobStatus)));
            }
            return productions.toArray(new PortalProduction[productions.size()]);
        } catch (IOException e) {
            throw new BackendServiceException("Failed to retrieve job list from Hadoop.", e);
        }
    }

    @Override
    public WorkStatus getProductionStatus(String productionId) throws BackendServiceException {
        try {
            JobStatus jobStatus = getJobStatus(productionId);
            return createWorkStatus(productionId, jobStatus);
        } catch (IOException e) {
            throw new BackendServiceException("Failed to retrieve job status from Hadoop.", e);
        }
    }

    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest productionRequest) throws BackendServiceException {
        throw new BackendServiceException("Method 'orderProduction' not implemented");
    }

    @Override
    public boolean[] cancelProductions(String[] productionIds) throws BackendServiceException {
        try {
            boolean[] result = new boolean[productionIds.length];
            JobStatus[] jobsStatuses = jobClient.getAllJobs();
            for (int i = 0; i < productionIds.length; i++) {
                JobStatus jobStatus = findJobStatus(jobsStatuses, productionIds[i]);
                if (jobsStatuses != null) {
                    RunningJob job = jobClient.getJob(jobStatus.getJobID());
                    job.killJob();
                    result[i] = true;
                }
            }
            return result;
        } catch (IOException e) {
            throw new  BackendServiceException("Failed to cancel jobs", e);
        }
    }

    @Override
    public boolean[] deleteProductions(String[] productionIds) throws BackendServiceException {
        // todo - delete productions as well.
        return cancelProductions(productionIds);
    }

    private JobStatus getJobStatus(String productionId) throws IOException {
        JobStatus[] jobsStatuses = jobClient.getAllJobs();
        return findJobStatus(jobsStatuses, productionId);
    }

    private static JobStatus findJobStatus(JobStatus[] jobsStatuses, String productionId) {
        JobID jobID = JobID.forName(productionId);
        for (JobStatus jobStatus : jobsStatuses) {
            if (jobStatus.getJobID().equals(jobID)) {
                return jobStatus;
            }
        }
        return null;
    }

    private WorkStatus createWorkStatus(String productionId, JobStatus job) throws IOException {
        if (job != null) {
            float progress = (job.setupProgress() + job.cleanupProgress()
                    + job.mapProgress() + job.reduceProgress()) / 4.0f;
            if (job.getRunState() == JobStatus.FAILED) {
                return new WorkStatus(WorkStatus.State.ERROR, "Job failed.", progress);
            } else if (job.getRunState() == JobStatus.KILLED) {
                return new WorkStatus(WorkStatus.State.CANCELLED, "", progress);
            } else if (job.getRunState() == JobStatus.PREP) {
                return new WorkStatus(WorkStatus.State.WAITING, "", progress);
            } else if (job.getRunState() == JobStatus.RUNNING) {
                return new WorkStatus(WorkStatus.State.IN_PROGRESS, "", progress);
            } else if (job.getRunState() == JobStatus.SUCCEEDED) {
                return new WorkStatus(WorkStatus.State.COMPLETED, "", 1.0);
            }
        }
        return new WorkStatus(WorkStatus.State.UNKNOWN, "", 0.0);
    }
}
