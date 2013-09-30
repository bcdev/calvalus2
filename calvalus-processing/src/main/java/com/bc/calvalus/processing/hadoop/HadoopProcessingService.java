/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.hadoop;


import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.ProcessorDescriptor;
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
    public static final String DEFAULT_CALVALUS_BUNDLE = "calvalus-1.7-SNAPSHOT";
    public static final String DEFAULT_BEAM_BUNDLE = "beam-4.10.4";
    public static final String BUNDLE_DESCRIPTOR_XML_FILENAME = "bundle-descriptor.xml";
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
    public BundleDescriptor[] getBundles(BundleFilter filter) throws IOException {
        ArrayList<BundleDescriptor> descriptors = new ArrayList<BundleDescriptor>();
        String bundleDirName = "*";
        if (filter.getBundleName() != null) {
            bundleDirName = filter.getBundleName() + "-" + filter.getBundleVersion();
        }
        try {
            if (filter.getNumSupportedProvider() == 0) {
                logger.warning("No bundle provider set in filter. Using SYSTEM as provider.");
                filter.withProvider(BundleFilter.PROVIDER_SYSTEM);
            }
            if (filter.isProviderSupported(BundleFilter.PROVIDER_USER)) {
                String bundleLocationPattern = String.format("/calvalus/home/%s/software/%s/%s", filter.getUserName().toLowerCase(), bundleDirName,
                                                             BUNDLE_DESCRIPTOR_XML_FILENAME);
                collectBundleDescriptors(bundleLocationPattern, filter, descriptors);
            }
            if (filter.isProviderSupported(BundleFilter.PROVIDER_ALL_USERS)) {
                String bundleLocationPattern = String.format("/calvalus/home/%s/software/%s/%s", "*", bundleDirName, BUNDLE_DESCRIPTOR_XML_FILENAME);
                collectBundleDescriptors(bundleLocationPattern, filter, descriptors);
            }
            if (filter.isProviderSupported(BundleFilter.PROVIDER_SYSTEM)) {
                String bundleLocationPattern = String.format("%s/%s/%s", CALVALUS_SOFTWARE_PATH, bundleDirName, BUNDLE_DESCRIPTOR_XML_FILENAME);
                collectBundleDescriptors(bundleLocationPattern, filter, descriptors);
            }
        } catch (IOException e) {
            logger.warning(e.getMessage());
            throw e;
        }

        return descriptors.toArray(new BundleDescriptor[descriptors.size()]);
    }

    private void collectBundleDescriptors(String bundlePathsGlob, BundleFilter filter, ArrayList<BundleDescriptor> descriptors) throws IOException {
        final Path qualifiedPath = fileSystem.makeQualified(new Path(bundlePathsGlob));
        final FileStatus[] fileStatuses = fileSystem.globStatus(qualifiedPath);
        final ParameterBlockConverter parameterBlockConverter = new ParameterBlockConverter();

        for (FileStatus file : fileStatuses) {
            try {
                BundleDescriptor bd = new BundleDescriptor();
                parameterBlockConverter.convertXmlToObject(readFile(file), bd);
                bd.setBundleLocation(file.getPath().getParent().toString());
                if (filter.getProcessorName() != null) {
                    final ProcessorDescriptor[] processorDescriptors = bd.getProcessorDescriptors();
                    for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                        if (processorDescriptor.getProcessorName().equals(filter.getProcessorName()) &&
                            processorDescriptor.getProcessorVersion().equals(filter.getProcessorVersion())) {
                            parameterBlockConverter.convertXmlToObject(readFile(file), bd);
                            descriptors.add(bd);
                        }
                    }
                } else {
                    descriptors.add(bd);
                }
            } catch (Exception e) {
                logger.warning(e.getMessage());
            }
        }
    }

    // this code exists somewhere else already
    private String readFile(FileStatus subPath) throws IOException {
        InputStream is = fileSystem.open(subPath.getPath());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(is, baos);
        return baos.toString();
    }


    public static void addBundleToClassPath(Path bundlePath, Configuration configuration) throws IOException {
        final FileSystem fileSystem = FileSystem.get(configuration);
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
            DistributedCache.addFileToClassPath(pathWithoutProtocol, configuration, fileSystem);
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
     *
     * @return The process status.
     */
    static ProcessStatus convertStatus(JobStatus jobStatus) {
        if (jobStatus != null) {
            float progress = (9.0F * jobStatus.mapProgress() + jobStatus.reduceProgress()) / 10.0F;
            if (jobStatus.getRunState() == JobStatus.FAILED) {
                return new ProcessStatus(ProcessState.ERROR, progress,
                                         "Hadoop job '" + jobStatus.getJobID() + "' failed, see logs for details");
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
