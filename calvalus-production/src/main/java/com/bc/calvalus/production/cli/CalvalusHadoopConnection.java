package com.bc.calvalus.production.cli;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.security.UserGroupInformation;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

public class CalvalusHadoopConnection {
    private static final String CALVALUS_SOFTWARE_PATH = "/calvalus/software/1.0";
    private static Logger LOG = CalvalusLogger.getLogger();

    private final UserGroupInformation remoteUser;
    private JobClient jobClient;

    public CalvalusHadoopConnection(String userName) {
        remoteUser = UserGroupInformation.createRemoteUser(userName);
    }

    public void createJobClient(Configuration hadoopParameters) throws IOException, InterruptedException {
        jobClient = remoteUser.doAs((PrivilegedExceptionAction<JobClient>) () -> new JobClient(new JobConf(hadoopParameters)));
    }

    public RunningJob submitJob(JobConf jobConf) throws IOException {
        return jobClient.submitJob(jobConf);
    }

    public JobStatus getJobStatus(JobID id) throws IOException {
        try {
            return jobClient.getJobStatus(id);
        } catch (NullPointerException _) {  // risky, shall handle case where request got lost
            return null;
        }
    }

    public JobStatus[] getAllJobs() throws IOException {
        return jobClient.getAllJobs();
    }

    public RunningJob getJob(JobID jobId) throws IOException {
        return jobClient.getJob(jobId);
    }

    public String getDiagnostics(JobID id) throws IOException {
        RunningJob job = getJob(id);
        if (job != null) {
            return getDiagnostics(job);
        } else {
            return "job diagnostics not found";
        }
    }

    /** Read first exception message of some failed task */

    public String getDiagnostics(RunningJob runningJob) throws IOException {
        int eventCounter = 0;
        while (true) {
            TaskCompletionEvent[] taskCompletionEvents = runningJob.getTaskCompletionEvents(eventCounter);
            if (taskCompletionEvents.length == 0) {
                return null;
            }
            eventCounter += taskCompletionEvents.length;
            for (TaskCompletionEvent taskCompletionEvent : taskCompletionEvents) {
                if (taskCompletionEvent.getTaskStatus().equals(TaskCompletionEvent.Status.FAILED)) {
                    String[] taskDiagnostics = runningJob.getTaskDiagnostics(taskCompletionEvent.getTaskAttemptId());
                    if (taskDiagnostics.length > 0) {
                        String firstMessage = taskDiagnostics[0];
                        String firstLine = firstMessage.split("\\n")[0];
                        String[] firstLineSplit = firstLine.split("Exception: ");
                        return firstLineSplit[firstLineSplit.length - 1];
                    }
                }
            }
        }
    }

    public Map<String, String> getProcessorDescriptorParameters(String bundles, String processor, String userName)
            throws IOException, InterruptedException {
        return remoteUser.doAs((PrivilegedExceptionAction<Map<String, String>>) () -> {
            Path path = new Path("/calvalus/software/1.0/" + bundles.split(",")[0] + "/bundle-descriptor.xml");
            if (!jobClient.getFs().exists(path)) {
                path = new Path("/calvalus/home/" + userName + "/software/" + bundles.split(",")[0] + "/bundle-descriptor.xml");
                if (!jobClient.getFs().exists(path)) {
                    LOG.fine("no bundle-descriptor.xml in bundle " + bundles.split(",")[0]);
                    return Collections.emptyMap();
                }
            }
            BundleDescriptor bd = readBundleDescriptor(path, jobClient.getFs());
            for (ProcessorDescriptor pd : bd.getProcessorDescriptors()) {
                if (processor.equals(pd.getExecutableName())) {
                    LOG.fine("adding bundle descriptor default parameters from " + path);
                    return pd.getJobConfiguration();
                }
            }
            LOG.fine("no processor descriptor for " + processor + " in bundle " + bundles.split(",")[0]);
            return Collections.emptyMap();
        });
    }

    /**
     * Add parameters for classpath and dist cache to Hadoop job
     */

    public void installBundle(String calvalusBundle, JobConf jobConf) throws IOException {
        Path bundlePath = new Path(CALVALUS_SOFTWARE_PATH, calvalusBundle);
        HadoopProcessingService.addBundleToClassPathStatic(bundlePath, jobConf, jobClient.getFs());
        HadoopProcessingService.addBundleArchives(bundlePath, jobClient.getFs(), jobConf);
        HadoopProcessingService.addBundleLibs(bundlePath, jobClient.getFs(), jobConf);
    }

    /**
     * Read bundle descriptor
     */

    public static BundleDescriptor readBundleDescriptor(Path path, FileSystem fs) throws IOException, BindingException {
        final ParameterBlockConverter parameterBlockConverter = new ParameterBlockConverter();
        final BundleDescriptor bd = new BundleDescriptor();
        parameterBlockConverter.convertXmlToObject(readFile(fs, path), bd);
        return bd;
    }

    /**
     * Read file content into string
     */

    public static String readFile(FileSystem fileSystem, Path path) throws IOException {
        try (InputStream is = fileSystem.open(path);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(is, baos, 8096);
            return baos.toString();
        }
    }


    public void installProcessorBundles(String userName, JobConf jobConf) throws IOException {
        ProcessorFactory.installProcessorBundles(userName, jobConf, jobClient.getFs());

    }

    public void deleteOutputDir(JobConf jobConf) throws IOException, InterruptedException {
        final String outputDir = jobConf.get("calvalus.output.dir");
        final Path dirPath = new Path(outputDir);
        remoteUser.doAs(new PrivilegedExceptionAction<Path>() {
            public Path run() throws IOException, InterruptedException {
                FileSystem fileSystem = dirPath.getFileSystem(jobConf);
                if (fileSystem.exists(dirPath)) {
                    LOG.info("clearing output dir " + outputDir);
                    fileSystem.delete(dirPath, true);
                }
                return dirPath;
            }});
    }
}