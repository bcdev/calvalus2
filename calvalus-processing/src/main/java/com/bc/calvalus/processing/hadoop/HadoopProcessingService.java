package com.bc.calvalus.processing.hadoop;


import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.esa.beam.framework.gpf.annotations.ParameterBlockConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.logging.Logger;

public class HadoopProcessingService implements ProcessingService<JobID> {

    public static final String CALVALUS_SOFTWARE_PATH = "/calvalus/software/1.0";
    public static final String DEFAULT_CALVALUS_BUNDLE = "calvalus-1.5-SNAPSHOT";
    public static final String DEFAULT_BEAM_BUNDLE = "beam-4.10.4-SNAPSHOT";

    private static final boolean DEBUG = Boolean.getBoolean("calvalus.debug");

    private final JobClient jobClient;
    private final FileSystem fileSystem;
    private final Map<JobID, ProcessStatus> jobStatusMap;
    private final Logger logger;

    public HadoopProcessingService(JobClient jobClient) throws IOException {
        this.jobClient = jobClient;
        this.fileSystem = FileSystem.get(jobClient.getConf());
        this.jobStatusMap = new WeakHashMap<JobID, ProcessStatus>();
        this.logger = Logger.getLogger("com.bc.calvalus");
    }

    @Override
    public BundleDescriptor[] getBundles(String filter) throws IOException {
        ArrayList<BundleDescriptor> descriptors = new ArrayList<BundleDescriptor>();

        try {
            Path softwarePath = fileSystem.makeQualified(new Path(CALVALUS_SOFTWARE_PATH));
            FileStatus[] paths = fileSystem.listStatus(softwarePath);
            for (FileStatus path : paths) {
                FileStatus[] subPaths = fileSystem.listStatus(path.getPath());
                for (FileStatus subPath : subPaths) {
                    if (subPath.getPath().toString().endsWith("bundle-descriptor.xml")) {
                        try {
                            BundleDescriptor bd = new BundleDescriptor();
                            new ParameterBlockConverter().convertXmlToObject(readFile(subPath), bd);
                            descriptors.add(bd);
                        } catch (Exception e) {
                            logger.warning(e.getMessage());
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.warning(e.getMessage());
        }

        return descriptors.toArray(new BundleDescriptor[descriptors.size()]);
    }

    // this code exists somewhere else already
    private String readFile(FileStatus subPath) throws IOException {
        InputStream is = fileSystem.open(subPath.getPath());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(is, baos);
        return baos.toString();
    }


    public static void addBundleToClassPath(String bundle, Configuration configuration) throws IOException {
        final FileSystem fileSystem = FileSystem.get(configuration);
        final Path bundlePath = new Path(CALVALUS_SOFTWARE_PATH, bundle);
        final FileStatus[] fileStatuses = fileSystem.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(".jar");
            }
        });
        for (FileStatus fileStatus : fileStatuses) {
            // For hadoops sake, skip protocol from path because it contains ':' and that is used
            // as separator in the job configuration!
            final Path path = fileStatus.getPath();
            final Path pathWithoutProtocol = new Path(path.toUri().getPath());
            DistributedCache.addFileToClassPath(pathWithoutProtocol, configuration);
        }
    }

    public final Configuration createJobConfig() {
        Configuration jobConfig = new Configuration(getJobClient().getConf());
        initCommonJobConfig(jobConfig);
        return jobConfig;
    }

    protected void initCommonJobConfig(Configuration jobConfig) {
        // Make user hadoop owns the outputs, required by "fuse"
        jobConfig.set("hadoop.job.ugi", "hadoop,hadoop");

        jobConfig.set("mapred.map.tasks.speculative.execution", "false");
        jobConfig.set("mapred.reduce.tasks.speculative.execution", "false");

        if (DEBUG) {
            // For debugging uncomment following line:
            jobConfig.set("mapred.child.java.opts",
                          "-Xmx1024M -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8009");
        } else {
            // Set VM maximum heap size
            jobConfig.set("mapred.child.java.opts",
                          "-Xmx1024M");
        }
    }

    public Job createJob(String jobName, Configuration jobConfig) throws IOException {
        return new Job(jobConfig, jobName);
    }

    public JobClient getJobClient() {
        return jobClient;
    }

    @Override
    public JobIdFormat<JobID> getJobIdFormat() {
        return new HadoopJobIdFormat();
    }

    @Override
    public void updateStatuses() throws IOException {
        JobStatus[] jobStatuses = jobClient.getAllJobs();
        synchronized (jobStatusMap) {
            if (jobStatuses != null) {
                for (JobStatus jobStatus : jobStatuses) {
                    jobStatusMap.put(jobStatus.getJobID(), convertStatus(jobStatus));
                }
            }
        }
    }

    @Override
    public ProcessStatus getJobStatus(JobID jobId) {
        synchronized (jobStatusMap) {
            ProcessStatus jobStatus = jobStatusMap.get(jobId);
            return jobStatus != null ? jobStatus : ProcessStatus.UNKNOWN;
        }
    }

    @Override
    public boolean killJob(JobID jobId) throws IOException {
        org.apache.hadoop.mapred.JobID oldJobId = org.apache.hadoop.mapred.JobID.downgrade(jobId);
        RunningJob runningJob = jobClient.getJob(oldJobId);
        if (runningJob != null) {
            runningJob.killJob();
            return true;
        }
        return false;
    }


    @Override
    public void close() throws IOException {
        jobClient.close();
    }

    /**
     * Updates the status. This method is called periodically after a fixed delay period.
     *
     * @param jobStatus The hadoop job status. May be null, which is interpreted as the job is being done.
     * @return The process status.
     */
    static ProcessStatus convertStatus(JobStatus jobStatus) {
        if (jobStatus != null) {
            float progress = (9.0F * jobStatus.mapProgress() + jobStatus.reduceProgress()) / 10.0F;
            if (jobStatus.getRunState() == JobStatus.FAILED) {
                return new ProcessStatus(ProcessState.ERROR, progress, "Hadoop job '" + jobStatus.getJobID() + "' failed, see logs for details");
            } else if (jobStatus.getRunState() == JobStatus.KILLED) {
                return new ProcessStatus(ProcessState.CANCELLED, progress);
            } else if (jobStatus.getRunState() == JobStatus.PREP) {
                return new ProcessStatus(ProcessState.SCHEDULED, progress);
            } else if (jobStatus.getRunState() == JobStatus.RUNNING) {
                return new ProcessStatus(ProcessState.RUNNING, progress);
            } else if (jobStatus.getRunState() == JobStatus.SUCCEEDED) {
                return new ProcessStatus(ProcessState.COMPLETED, 1.0f);
            }
        }
        return ProcessStatus.UNKNOWN;
    }
}
