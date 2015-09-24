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


import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

public class HadoopProcessingService implements ProcessingService<JobID> {

    public static final String CALVALUS_SOFTWARE_PATH = "/calvalus/software/1.0";
    public static final String DEFAULT_CALVALUS_BUNDLE = "calvalus-2.7-SNAPSHOT";
    public static final String DEFAULT_BEAM_BUNDLE = "beam-5.0.1";
    public static final String BUNDLE_DESCRIPTOR_XML_FILENAME = "bundle-descriptor.xml";
    private static final long CAHCE_RETENTION = 30 * 1000;

    private final JobClientsMap jobClientsMap;
    private final Map<JobID, ProcessStatus> jobStatusMap;
    private final List<BundleQueryCacheEntry> bundleQueryCache;
    private final Timer bundlesQueryCleaner;
    private final Map<String, BundleCacheEntry> bundleCache;
    private final Logger logger;
    private final ExecutorService executorService = Executors.newFixedThreadPool(3);


    public HadoopProcessingService(JobClientsMap jobClientsMap) throws IOException {
        this.jobClientsMap = jobClientsMap;
        this.jobStatusMap = new WeakHashMap<>();

        this.bundleQueryCache = new ArrayList<>();
        this.bundlesQueryCleaner = new Timer("bundlesQueryCleaner");
        TimerTask bundlesQueryCleanTask = new TimerTask() {
            @Override
            public void run() {
                synchronized (bundleQueryCache) {
                    long now = System.currentTimeMillis();
                    long clearIfOlder = now - CAHCE_RETENTION;
                    Iterator<BundleQueryCacheEntry> iterator = bundleQueryCache.iterator();
                    while (iterator.hasNext()) {
                        BundleQueryCacheEntry cacheEntry = iterator.next();
                        if (cacheEntry.time < clearIfOlder) {
                            iterator.remove();
                        }
                    }
                }

            }
        };
        this.bundlesQueryCleaner.scheduleAtFixedRate(bundlesQueryCleanTask, CAHCE_RETENTION, CAHCE_RETENTION);

        this.bundleCache = new HashMap<>();
        this.logger = Logger.getLogger("com.bc.calvalus");
    }

    @Override
    public BundleDescriptor[] getBundles(final String username, final BundleFilter filter) throws IOException {
        String bundleFilterString = filter.toString();
        synchronized (bundleQueryCache) {
            for (BundleQueryCacheEntry entry : bundleQueryCache) {
                if (entry.bundleFilter.equals(bundleFilterString) && entry.userName.equals(username)) {
                    try {
                        return entry.bundles.get();
                    } catch (InterruptedException | ExecutionException e) {
                        logger.warning(e.getMessage());
                    }
                }
            }
            Future<BundleDescriptor[]> future = executorService.submit(new Callable<BundleDescriptor[]>() {
                @Override
                public BundleDescriptor[] call() throws Exception {
                    return HadoopProcessingService.this.getBundleDescriptorsImpl(username, filter);
                }
            });
            bundleQueryCache.add(new BundleQueryCacheEntry(username, bundleFilterString, future));
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.warning(e.getMessage());
            }
        }
        throw new IOException("Failed to load BundleDescriptor");
    }

    public BundleDescriptor[] getBundleDescriptorsImpl(String username, BundleFilter filter) throws IOException {
        ArrayList<BundleDescriptor> descriptors = new ArrayList<>();
        String bundleDirName = "*";
        if (filter.getBundleName() != null) {
            bundleDirName = filter.getBundleName() + "-" + filter.getBundleVersion();
        }
        try {
            if (filter.getNumSupportedProvider() == 0) {
                logger.warning("No bundle provider set in filter. Using SYSTEM as provider.");
                filter.withProvider(BundleFilter.PROVIDER_SYSTEM);
            }
            if (filter.isProviderSupported(BundleFilter.PROVIDER_USER) && filter.getUserName() != null) {
                String bundleLocationPattern = String.format("/calvalus/home/%s/software/%s/%s", filter.getUserName().toLowerCase(), bundleDirName,
                                                             BUNDLE_DESCRIPTOR_XML_FILENAME);
                FileSystem fileSystem = getFileSystem(username, bundleLocationPattern);
                collectBundleDescriptors(fileSystem, bundleLocationPattern, filter, descriptors);
            }
            if (filter.isProviderSupported(BundleFilter.PROVIDER_ALL_USERS)) {
                String bundleLocationPattern = String.format("/calvalus/home/%s/software/%s/%s", "*", bundleDirName, BUNDLE_DESCRIPTOR_XML_FILENAME);
                FileSystem fileSystem = getFileSystem(username, bundleLocationPattern);
                collectBundleDescriptors(fileSystem, bundleLocationPattern, filter, descriptors);
            }
            if (filter.isProviderSupported(BundleFilter.PROVIDER_SYSTEM)) {
                final String calvalusSoftwarePath = (jobClientsMap.getConfiguration().get("calvalus.portal.softwareDir", CALVALUS_SOFTWARE_PATH));
                String bundleLocationPattern = String.format("%s/%s/%s", calvalusSoftwarePath, bundleDirName, BUNDLE_DESCRIPTOR_XML_FILENAME);
                FileSystem fileSystem = getFileSystem(username, bundleLocationPattern);
                collectBundleDescriptors(fileSystem, bundleLocationPattern, filter, descriptors);
            }
        } catch (IOException e) {
            logger.warning(e.getMessage());
            throw e;
        }
        return descriptors.toArray(new BundleDescriptor[descriptors.size()]);
    }

    private void collectBundleDescriptors(FileSystem fileSystem, String bundlePathsGlob, BundleFilter filter, ArrayList<BundleDescriptor> descriptors) throws IOException {
        final Path qualifiedPath = fileSystem.makeQualified(new Path(bundlePathsGlob));
        final FileStatus[] fileStatuses = fileSystem.globStatus(qualifiedPath);
        if (fileStatuses == null) {
            return;
        }
        for (FileStatus file : fileStatuses) {
            try {
                final BundleDescriptor bd;
                BundleCacheEntry bundleCacheEntry = bundleCache.get(file.getPath().toString());
                if (bundleCacheEntry != null && bundleCacheEntry.modificationTime == file.getModificationTime()) {
                    bd = bundleCacheEntry.bundleDescriptor;
                } else {
                    bd = readBundleDescriptor(fileSystem, file.getPath());
                    bd.setBundleLocation(file.getPath().getParent().toString());
                    bundleCache.put(file.getPath().toString(), new BundleCacheEntry(file.getModificationTime(), bd));
                }
                if (filter.getProcessorName() != null) {
                    final ProcessorDescriptor[] processorDescriptors = bd.getProcessorDescriptors();
                    if (processorDescriptors != null) {
                        for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                            if (processorDescriptor.getProcessorName().equals(filter.getProcessorName()) &&
                                    processorDescriptor.getProcessorVersion().equals(filter.getProcessorVersion())) {
                                descriptors.add(bd);
                            }
                        }
                    }
                } else {
                    descriptors.add(bd);
                }
            } catch (Exception e) {
                logger.warning("error reading bundle-descriptor (" + file.getPath() +") : "+ e.getMessage());
            }
        }
    }

    // this code exists somewhere else already
    private static String readFile(FileSystem fileSystem, Path path) throws IOException {
        try (InputStream is = fileSystem.open(path);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(is, baos, 8092);
            return baos.toString();
        }
    }

    public static BundleDescriptor readBundleDescriptor(FileSystem fileSystem, Path path) throws IOException, BindingException {
        final ParameterBlockConverter parameterBlockConverter = new ParameterBlockConverter();
        final BundleDescriptor bd = new BundleDescriptor();
        parameterBlockConverter.convertXmlToObject(readFile(fileSystem, path), bd);
        return bd;
    }

    public static void addBundleToClassPath(Path bundlePath, Configuration configuration) throws IOException {
        final FileSystem fileSystem = bundlePath.getFileSystem(configuration);
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

    public final Configuration createJobConfig(String userName) throws IOException {
        return new Configuration(getJobClient(userName).getConf());
    }

    public Job createJob(String jobName, Configuration jobConfig) throws IOException {
        return new Job(jobConfig, jobName);
    }

    public JobClient getJobClient(String username) throws IOException {
        return jobClientsMap.getJobClient(username);
    }

    @Override
    public JobIdFormat<JobID> getJobIdFormat() {
        return new HadoopJobIdFormat();
    }

    @Override
    public void updateStatuses(String username) throws IOException {
        JobClient jobClient = jobClientsMap.getJobClient(username);
        JobStatus[] jobStatuses = jobClient.getAllJobs();
        synchronized (jobStatusMap) {
            if (jobStatuses != null && jobStatuses.length > 0) {
                Set<JobID> allJobs = new HashSet<>(jobStatusMap.keySet());
                for (JobStatus jobStatus : jobStatuses) {
                    org.apache.hadoop.mapred.JobID jobID = jobStatus.getJobID();
                    jobStatusMap.put(jobID, convertStatus(jobID, jobStatus, jobClient));
                    allJobs.remove(jobID);
                }
                for (JobID jobID : allJobs) {
                    float progress = jobStatusMap.get(jobID).getProgress();
                    ProcessStatus processStatus = new ProcessStatus(ProcessState.ERROR, progress, "Hadoop job '" + jobID + "' cancelled by backend");
                    jobStatusMap.put(jobID, processStatus);
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
    public boolean killJob(String username, JobID jobId) throws IOException {
        JobClient jobClient = jobClientsMap.getJobClient(username);
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
        jobClientsMap.close();
        bundlesQueryCleaner.cancel();
        executorService.shutdown();
    }

    /**
     * Updates the status. This method is called periodically after a fixed delay period.
     *
     * @param jobID
     * @param jobStatus The hadoop job status. May be null, which is interpreted as the job is being done.
     * @param jobClient
     * @return The process status.
     */
    private ProcessStatus convertStatus(org.apache.hadoop.mapred.JobID jobID, JobStatus jobStatus, JobClient jobClient) {
        ProcessStatus oldProcessStatus = jobStatusMap.get(jobID);
        float oldProgress = oldProcessStatus != null ? oldProcessStatus.getProgress() : 0f;

        if (jobStatus.getRunState() == JobStatus.FAILED) {
            return new ProcessStatus(ProcessState.ERROR, oldProgress,
                                     "Hadoop job '" + jobStatus.getJobID() + "' failed, see logs for details");
        } else if (jobStatus.getRunState() == JobStatus.KILLED) {
            return new ProcessStatus(ProcessState.CANCELLED, oldProgress);
        } else if (jobStatus.getRunState() == JobStatus.PREP) {
            return new ProcessStatus(ProcessState.SCHEDULED, 0f);
        } else if (jobStatus.getRunState() == JobStatus.RUNNING) {
            float progress = getMapReduceProgress(jobStatus, jobClient);
            return new ProcessStatus(ProcessState.RUNNING, progress);
        } else if (jobStatus.getRunState() == JobStatus.SUCCEEDED) {
            return new ProcessStatus(ProcessState.COMPLETED, 1.0f);
        } else {
            return ProcessStatus.UNKNOWN;
        }
    }

    private float getMapReduceProgress(JobStatus jobStatus, JobClient jobClient) {
        try {
            Job job = jobClient.getClusterHandle().getJob(jobStatus.getJobID());
            if (job != null) {
                return calculateProgress(job.getStatus(), job.getNumReduceTasks() > 0);
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return 0f;
    }

    static float calculateProgress(org.apache.hadoop.mapreduce.JobStatus jobStatus, boolean hasReducer) {
        if (hasReducer) {
            return (9.0F * jobStatus.getMapProgress() + jobStatus.getReduceProgress()) / 10.0F;
        } else {
            return jobStatus.getMapProgress();
        }
    }

    public FileSystem getFileSystem(String userName, String path) throws IOException {
        return jobClientsMap.getFileSystem(userName, path);
    }

    private static class BundleQueryCacheEntry {
        private final String userName;
        private final String bundleFilter;
        private final long time;
        private final Future<BundleDescriptor[]> bundles;

        public BundleQueryCacheEntry(String userName, String bundleFilter, Future<BundleDescriptor[]> bundles) {
            this.userName = userName;
            this.bundleFilter = bundleFilter;
            this.bundles = bundles;
            this.time = System.currentTimeMillis();
        }
    }

    private class BundleCacheEntry {
        private final BundleDescriptor bundleDescriptor;
        private final long modificationTime;

        public BundleCacheEntry(long modificationTime, BundleDescriptor bundleDescriptor) {
            this.bundleDescriptor = bundleDescriptor;
            this.modificationTime = modificationTime;
        }
    }
}
