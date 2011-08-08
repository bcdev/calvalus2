package com.bc.calvalus.processing.hadoop;


import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.beam.BeamUtils;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class HadoopProcessingService implements ProcessingService<JobID> {

    public static final String CALVALUS_EODATA_PATH = "/calvalus/eodata";
    public static final String CALVALUS_OUTPUTS_PATH = "/calvalus/outputs";
    public static final String CALVALUS_SOFTWARE_PATH = "/calvalus/software/0.5";
    public static final String DEFAULT_CALVALUS_BUNDLE = "calvalus-0.3-SNAPSHOT";
    public static final String DEFAULT_BEAM_BUNDLE = "beam-4.10-SNAPSHOT";

    private final JobClient jobClient;
    private final FileSystem fileSystem;
    private final Map<JobID, ProcessStatus> jobStatusMap;
    private final Logger logger;

    public HadoopProcessingService(JobClient jobClient) throws IOException {
        this.jobClient = jobClient;
        this.fileSystem = FileSystem.get(jobClient.getConf());
        this.jobStatusMap = new HashMap<JobID, ProcessStatus>();
        this.logger = Logger.getLogger("com.bc.calvalus");
    }

    @Override
    public ProcessorDescriptor[] getProcessors(String filter) throws IOException {
        ArrayList<ProcessorDescriptor> descriptors = new ArrayList<ProcessorDescriptor>();

        try {
            Path softwarePath = fileSystem.makeQualified(new Path(CALVALUS_SOFTWARE_PATH));
            FileStatus[] paths = fileSystem.listStatus(softwarePath);
            for (FileStatus path : paths) {
                FileStatus[] subPaths = fileSystem.listStatus(path.getPath());
                for (FileStatus subPath : subPaths) {
                    if (subPath.getPath().toString().endsWith("processor-descriptor.xml")) {
                        try {
                            InputStream is = fileSystem.open(subPath.getPath());
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            IOUtils.copyBytes(is, baos);
                            String xmlContent = baos.toString();
                            ProcessorDescriptor pd = new ProcessorDescriptor();
                            BeamUtils.convertXmlToObject(xmlContent, pd);
                            descriptors.add(pd);
                        } catch (Exception e) {
                            logger.warning(e.getMessage());
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.warning(e.getMessage());
        }

        return descriptors.toArray(new ProcessorDescriptor[descriptors.size()]);
    }


    public static void addBundleToClassPath(String bundle, Configuration configuration) throws IOException {
        final FileSystem fileSystem = FileSystem.get(configuration);
        final Path bundlePath = new Path(CALVALUS_SOFTWARE_PATH + "/" + bundle);

        final FileStatus[] beamJars = fileSystem.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith("jar");
            }
        });
        for (FileStatus beamJar : beamJars) {
            final Path path = beamJar.getPath();
            final Path pathWithoutProtocol = new Path(path.toUri().getPath());  // for hadoops sake!
            DistributedCache.addFileToClassPath(pathWithoutProtocol, configuration);
        }
    }

    public Job createJob(String jobName) throws IOException {
        Job job = new Job(getJobClient().getConf(), jobName);
        Configuration configuration = job.getConfiguration();
        // Make user hadoop owns the outputs
        configuration.set("hadoop.job.ugi", "hadoop,hadoop");
        configuration.set("mapred.map.tasks.speculative.execution", "false");
        configuration.set("mapred.reduce.tasks.speculative.execution", "false");
        boolean debug = false;
        if (debug) {
            // For debugging uncomment following line:
            configuration.set("mapred.child.java.opts",
                              "-Xmx2000m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8009");
        } else {
            // Set VM maximum heap size
            configuration.set("mapred.child.java.opts",
                              "-Xmx2000m");
        }
        return job;
    }

    public JobClient getJobClient() {
        return jobClient;
    }

    @Override
    public JobIdFormat<JobID> getJobIdFormat() {
        return new HadoopJobIdFormat();
    }

    public String[] globFilePaths(String dirPathGlob) throws IOException {
        FileStatus[] fileStatuses = fileSystem.globStatus(new Path(dirPathGlob));
        String[] paths = new String[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            paths[i] = fileStatuses[i].getPath().toString();
        }
        return paths;
    }

    @Override
    public String getDataInputPath(String inputPath) {
        return makeQualified(CALVALUS_EODATA_PATH, inputPath);
    }

    @Override
    public String getDataOutputPath(String outputPath) {
        return makeQualified(CALVALUS_OUTPUTS_PATH, outputPath);
    }


    @Override
    public void updateStatuses() throws IOException {
        JobStatus[] jobStatuses = jobClient.getAllJobs();
        synchronized (jobStatusMap) {
            jobStatusMap.clear();
            if (jobStatuses != null) {
                for (JobStatus jobStatus : jobStatuses) {
                    jobStatusMap.put(jobStatus.getJobID(), convertStatus(jobStatus));
                }
            }
        }
    }

    @Override
    public ProcessStatus getJobStatus(JobID jobID) {
        synchronized (jobStatusMap) {
            return jobStatusMap.get(jobID);
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
     */
    static ProcessStatus convertStatus(JobStatus jobStatus) {
        if (jobStatus != null) {
            float progress = (jobStatus.mapProgress() + jobStatus.reduceProgress()) / 2;
            if (jobStatus.getRunState() == JobStatus.FAILED) {
                return new ProcessStatus(ProcessState.ERROR, progress, "Hadoop job '" + jobStatus.getJobID() + "' failed");
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

    private String makeQualified(String parent, String child) {
        Path path = new Path(child);
        if (!path.isAbsolute()) {
            path = new Path(parent, path);
        }
        return fileSystem.makeQualified(path).toString();
    }

}
