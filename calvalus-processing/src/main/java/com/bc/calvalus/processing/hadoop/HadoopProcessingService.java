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
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.MaskDescriptor;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.ra.RARegions;
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
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
    public static final String DEFAULT_CALVALUS_BUNDLE = "calvalus-2.17-SNAPSHOT";
    public static final String DEFAULT_SNAP_BUNDLE = "snap-5.0";
    public static final String BUNDLE_DESCRIPTOR_XML_FILENAME = "bundle-descriptor.xml";
    private static final long CACHE_RETENTION = 30 * 1000;

    private final JobClientsMap jobClientsMap;
    private final String softwareDir;
    private final Map<JobID, ProcessStatus> jobStatusMap;
    private final List<BundleQueryCacheEntry> bundleQueryCache;
    private final Timer bundlesQueryCleaner;
    private final Map<String, BundleCacheEntry> bundleCache;
    private final Map<String, ShapefileCacheEntry> shapeAttributeCache;
    private final Logger logger;
    private final ExecutorService executorService = Executors.newFixedThreadPool(3);
    private boolean withExternalAccessControl;

    private static JobClientsMap jobClientsMapSingleton = null;

    public HadoopProcessingService(JobClientsMap jobClientsMap) throws IOException {
        this(jobClientsMap, CALVALUS_SOFTWARE_PATH);
    }

    public HadoopProcessingService(JobClientsMap jobClientsMap, String softwareDir) throws IOException {
        this.jobClientsMap = jobClientsMap;
        jobClientsMapSingleton = jobClientsMap;
        this.softwareDir = softwareDir;
        this.jobStatusMap = new WeakHashMap<>();
        this.withExternalAccessControl = Boolean.getBoolean("calvalus.accesscontrol.external");
        this.bundleQueryCache = new ArrayList<>();
        // TODO there should be one Timer for a process that is used for all timer tasks
        this.bundlesQueryCleaner = new Timer("bundlesQueryCleaner", true);
        TimerTask bundlesQueryCleanTask = new TimerTask() {
            @Override
            public void run() {
                synchronized (bundleQueryCache) {
                    long now = System.currentTimeMillis();
                    long clearIfOlder = now - CACHE_RETENTION;
                    bundleQueryCache.removeIf(cacheEntry -> cacheEntry.time < clearIfOlder);
                }

            }
        };
        this.bundlesQueryCleaner.scheduleAtFixedRate(bundlesQueryCleanTask, CACHE_RETENTION, CACHE_RETENTION);
        this.bundleCache = new HashMap<>();
        this.shapeAttributeCache = new HashMap<>();
        this.logger = Logger.getLogger("com.bc.calvalus");
    }

    @Override
    public BundleDescriptor[] getBundles(final String username, final BundleFilter filter) throws IOException {
        logger.fine("HadoopProcessingService.getBundles.start username = [" + username + "], filter = [" + filter + "]");
        long t1 = System.currentTimeMillis();
        try {
            String bundleFilterString = filter.toString();
            Future<BundleDescriptor[]> bundleFuture = null;
            synchronized (bundleQueryCache) {
                for (BundleQueryCacheEntry entry : bundleQueryCache) {
                    if (entry.bundleFilter.equals(bundleFilterString) && entry.userName.equals(username)) {
                        logger.fine("HadoopProcessingService.getBundles cacheHIT");
                        bundleFuture = entry.bundles;
                        break;
                    }
                }
                if (bundleFuture == null) {
                    logger.fine("HadoopProcessingService.getBundles cacheMISS");
                    bundleFuture = executorService.submit(new Callable<BundleDescriptor[]>() {
                        @Override
                        public BundleDescriptor[] call() throws Exception {
                            return HadoopProcessingService.this.getBundleDescriptorsImpl(username, filter);
                        }
                    });
                    bundleQueryCache.add(new BundleQueryCacheEntry(username, bundleFilterString, bundleFuture));
                }
            }
            try {
                return bundleFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                logger.warning(e.getMessage());
                throw new IOException("Failed to load BundleDescriptor", e);
            }
        } finally {
            long t2 = System.currentTimeMillis();
            long delta = t2 - t1;
            logger.fine("HadoopProcessingService.getBundles.end username = [" + username + "], filter = [" + filter + "] ==> " + delta + "ms");
        }
    }

    @Override
    public MaskDescriptor[] getMasks(final String username) throws IOException {
        Future<MaskDescriptor[]> future = executorService.submit(new Callable<MaskDescriptor[]>() {
            @Override
            public MaskDescriptor[] call() throws Exception {
                return HadoopProcessingService.this.getMaskDescriptorsImpl(username);
            }
        });
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            logger.warning(e.getMessage());
        }
        throw new IOException("Failed to load MaskDescriptor");
    }

    public MaskDescriptor[] getMaskDescriptorsImpl(String username) throws IOException {
        try {
            String maskLocationPattern = String.format("/calvalus/home/%s/masks/*", username);
            FileSystem fileSystem = getFileSystem(username, maskLocationPattern);
            List<MaskDescriptor> maskDescriptors = getMaskDescriptors(fileSystem, maskLocationPattern);
            for (MaskDescriptor maskDescriptor : maskDescriptors) {
                maskDescriptor.setOwner(username);
            }
            return maskDescriptors.toArray(new MaskDescriptor[0]);
        } catch (IOException e) {
            logger.warning(e.getMessage());
            throw e;
        }
    }

    public BundleDescriptor[] getBundleDescriptorsImpl(String username, BundleFilter filter) throws IOException {
        try {
            UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(username);
            return remoteUser.doAs((PrivilegedExceptionAction<BundleDescriptor[]>) () -> {
                ArrayList<BundleDescriptor> descriptors = new ArrayList<>();
                String bundleDirName = "*";
                if (filter.getBundleName() != null) {
                    bundleDirName = filter.getBundleName() + "-" + filter.getBundleVersion();
                }
                if (filter.getNumSupportedProvider() == 0) {
                    logger.warning("No bundle provider set in filter. Using SYSTEM as provider.");
                    filter.withProvider(BundleFilter.PROVIDER_SYSTEM);
                }
                if (filter.isProviderSupported(BundleFilter.PROVIDER_USER) && filter.getUserName() != null) {
                    String bundleLocationPattern = String.format("/calvalus/home/%s/software/%s/%s", username, bundleDirName,
                                                                 BUNDLE_DESCRIPTOR_XML_FILENAME);
                    FileSystem fileSystem = getFileSystem(username, bundleLocationPattern);
                    List<BundleDescriptor> singleUserDescriptors = getBundleDescriptors(fileSystem, bundleLocationPattern, filter);
                    for (BundleDescriptor bundleDescriptor : singleUserDescriptors) {
                        bundleDescriptor.setOwner(filter.getUserName());
                    }
                    descriptors.addAll(singleUserDescriptors);
                }
                if (filter.isProviderSupported(BundleFilter.PROVIDER_ALL_USERS)) {
                    String bundleLocationPattern = String.format("/calvalus/home/%s/software/%s/%s", "*", bundleDirName, BUNDLE_DESCRIPTOR_XML_FILENAME);
                    FileSystem fileSystem = getFileSystem(username, bundleLocationPattern);
                    List<BundleDescriptor> allUserDescriptors = getBundleDescriptors(fileSystem, bundleLocationPattern, filter);
                    String userPathPattern = String.format("/calvalus/home/%s/software", username);
                    for (BundleDescriptor bundleDescriptor : allUserDescriptors) {
                        String bundleLocation = bundleDescriptor.getBundleLocation();
                        if (bundleLocation.contains(userPathPattern)) {
                            continue;
                        }
                        String[] pathElems = bundleLocation.split("/");
                        for (int i = 0; i < pathElems.length; i++) {
                            if (pathElems[i].equals("home")) {
                                bundleDescriptor.setOwner(pathElems[i + 1]);
                                break;
                            }
                        }
                        descriptors.add(bundleDescriptor);
                    }
                }
                if (filter.isProviderSupported(BundleFilter.PROVIDER_SYSTEM)) {
                    String bundleLocationPattern = String.format("%s/%s/%s", softwareDir, bundleDirName, BUNDLE_DESCRIPTOR_XML_FILENAME);
                    FileSystem fileSystem = getFileSystem(username, bundleLocationPattern);
                    List<BundleDescriptor> systemDescriptors = getBundleDescriptors(fileSystem, bundleLocationPattern, filter);
                    descriptors.addAll(systemDescriptors);
                }
                return descriptors.toArray(new BundleDescriptor[descriptors.size()]);
            });
        } catch (IOException e) {
            logger.warning(e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            logger.warning(e.getMessage());
            throw new IOException("failed to retrieve bundle descriptors for user " + username, e);
        }
    }

    private List<BundleDescriptor> getBundleDescriptors(FileSystem fileSystem, String bundlePathsGlob, BundleFilter filter) throws IOException {
        final Path qualifiedPath = fileSystem.makeQualified(new Path(bundlePathsGlob));
        final FileStatus[] fileStatuses;
        if (jobClientsMap.getConfiguration().getBoolean("calvalus.acl", true) && !withExternalAccessControl) {
            final ArrayList<FileStatus> accu = new ArrayList<>();
            collectAccessibleFiles(fileSystem, bundlePathsGlob, 1, new Path("/"), accu);
            fileStatuses = accu.toArray(new FileStatus[accu.size()]);
        } else {
            fileStatuses = fileSystem.globStatus(qualifiedPath);
        }
        if (fileStatuses == null) {
            return Collections.EMPTY_LIST;
        }
        List<BundleDescriptor> descriptors = new ArrayList<>();
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
                logger.warning("error reading bundle-descriptor (" + file.getPath() + ") : " + e.getMessage());
            }
        }
        return descriptors;
    }

    private List<MaskDescriptor> getMaskDescriptors(FileSystem fileSystem, String maskLocationPattern) throws IOException {
        final Path qualifiedPath = fileSystem.makeQualified(new Path(maskLocationPattern));
        final FileStatus[] fileStatuses;
        if (jobClientsMap.getConfiguration().getBoolean("calvalus.acl", true) && !withExternalAccessControl) {
            final List<FileStatus> accu = new ArrayList<>();
            collectAccessibleFiles(fileSystem, maskLocationPattern, 1, new Path("/"), accu);
            fileStatuses = accu.toArray(new FileStatus[accu.size()]);
        } else {
            fileStatuses = fileSystem.globStatus(qualifiedPath);
        }
        if (fileStatuses == null) {
            return Collections.EMPTY_LIST;
        }
        List<MaskDescriptor> descriptors = new ArrayList<>();
        for (FileStatus file : fileStatuses) {
            try {
                final MaskDescriptor md = new MaskDescriptor();
                md.setMaskName(file.getPath().getName());
                md.setMaskLocation(file.getPath().getParent().toString());
                descriptors.add(md);
            } catch (Exception e) {
                logger.warning("error reading bundle-descriptor (" + file.getPath() + ") : " + e.getMessage());
            }
        }
        return descriptors;
    }

    private void collectAccessibleFiles(FileSystem fileSystem, String pathPattern, int pos, Path path, List<FileStatus> accu) throws IOException {
        try {
            final int pos1 = pathPattern.indexOf('/', pos);
            if (pos1 > -1) {
                final FileStatus[] stati = fileSystem.globStatus(new Path(path, pathPattern.substring(pos, pos1)));
                if (stati != null) {
                    for (FileStatus status : stati) {
                        if (status.isDirectory()) {
                            collectAccessibleFiles(fileSystem, pathPattern, pos1 + 1, status.getPath(), accu);
                        }
                    }
                }
            } else {
                try {
                    final FileStatus status = fileSystem.getFileStatus(new Path(path, pathPattern.substring(pos)));
                    if (status.isFile()) {
                        accu.add(status);
                    }
                } catch (FileNotFoundException ignore) {
                    // ok, try to glob as an alternative (to avoid duplicates)
                    final FileStatus[] stati = fileSystem.globStatus(new Path(path, pathPattern.substring(pos)));
                    if (stati != null) {
                        for (FileStatus globStatus : stati) {
                            if (globStatus.isFile()) {
                                accu.add(globStatus);
                            }
                        }
                    }
                }
            }
        } catch (FileNotFoundException | AccessControlException ignore) {
            // ok
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

    public void addBundleToClassPath(Path bundlePath, Configuration configuration) throws IOException {
        FileSystem fileSystem = jobClientsMap.getFileSystem(configuration.get(JobConfigNames.CALVALUS_USER), bundlePath.toString());
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

    public static void addBundleToClassPathStatic(Path bundlePath, Configuration configuration, FileSystem fileSystem) throws IOException {
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

    @Override
    public String[][] loadRegionDataInfo(String username, String url) throws IOException {
        Path path = new Path(url);
        Configuration conf = createJobConfig(username);
        //FileStatus fileStatus = path.getFileSystem(conf).getFileStatus(path);
        FileSystem fileSystem = jobClientsMap.getFileSystem(username);
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        if (fileStatus != null) {
            ShapefileCacheEntry cacheEntry = shapeAttributeCache.get(path.toString());
            if (cacheEntry == null || cacheEntry.modificationTime != fileStatus.getModificationTime() || fileStatus.getModificationTime() == 0) {
                String[][] data = RARegions.loadStringAttributes(url, conf);
                cacheEntry = new ShapefileCacheEntry(fileStatus.getModificationTime(), data);
                shapeAttributeCache.put(path.toString(), cacheEntry);
            }
            return cacheEntry.data;
        }
        throw new FileNotFoundException(url);
    }

    /**
     * Updates the status. This method is called periodically after a fixed delay period.
     *
     * @param jobID
     * @param jobStatus The hadoop job status. May be null, which is interpreted as the job is being done.
     * @param jobClient
     *
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

    public FileSystem getFileSystem(String userName) throws IOException {
        return jobClientsMap.getFileSystem(userName);
    }

    public FileSystem getFileSystem(String userName, String path) throws IOException {
        return jobClientsMap.getFileSystem(userName, path);
    }

    public FileSystem getFileSystem(String userName, Configuration conf, Path path) throws IOException {
        return jobClientsMap.getFileSystem(userName, conf, path);
    }

    void addBundleToDistributedCache(Path bundlePath, String username, Configuration conf) throws IOException {
        final FileSystem fs = jobClientsMap.getFileSystem(username, conf, bundlePath);

        addBundleToClassPath(bundlePath, conf);
        addBundleArchives(bundlePath, fs, conf);
        addBundleLibs(bundlePath, fs, conf);

    }

    public static void addBundleArchives(Path bundlePath, FileSystem fs, Configuration conf) throws IOException {
        final FileStatus[] archives = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return isArchive(path);
            }
        });
        for (FileStatus archive : archives) {
            URI uri = convertPathToURI(fs.makeQualified(archive.getPath()));
            DistributedCache.addCacheArchive(uri, conf);
        }
    }

    private static URI convertPathToURI(Path path) {
        URI uri = path.toUri();
        String linkName = stripArchiveExtension(path.getName());
        try {
            return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), null, linkName);
        } catch (URISyntaxException ignore) {
            throw new IllegalArgumentException("could not add fragment to URI for Path: " + path);
        }
    }

    /**
     * Hadoop can handle archives with the following extensions: zip, tar, tar.gz, tgz
     */
    public static boolean isArchive(Path archivePath) {
        String filename = archivePath.getName();
        return filename.endsWith(".tgz") || filename.endsWith(".tar.gz") ||
               filename.endsWith(".tar") || filename.endsWith(".zip");
    }

    static String stripArchiveExtension(String archiveName) {
        if (archiveName.endsWith(".tgz") || archiveName.endsWith(".tar") || archiveName.endsWith(".zip")) {
            return archiveName.substring(0, archiveName.length() - 4);
        } else if (archiveName.endsWith(".tar.gz")) {
            return archiveName.substring(0, archiveName.length() - 7);
        }
        return null;
    }

    public static void addBundleLibs(Path bundlePath, FileSystem fs, Configuration conf) throws IOException {
        final FileStatus[] libs = fs.listStatus(bundlePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return isLib(path);
            }
        });
        for (FileStatus lib : libs) {
            URI uri = fs.makeQualified(lib.getPath()).toUri();
            DistributedCache.addCacheFile(uri, conf);
        }
    }

    public static boolean isLib(Path libPath) {
        String filename = libPath.getName();
        return filename.endsWith(".so");
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

    private static class BundleCacheEntry {

        private final BundleDescriptor bundleDescriptor;
        private final long modificationTime;

        public BundleCacheEntry(long modificationTime, BundleDescriptor bundleDescriptor) {
            this.bundleDescriptor = bundleDescriptor;
            this.modificationTime = modificationTime;
        }
    }

    private static class ShapefileCacheEntry {

        private final String[][] data;
        private final long modificationTime;

        public ShapefileCacheEntry(long modificationTime, String[][] data) {
            this.data = data;
            this.modificationTime = modificationTime;
        }
    }

    /**
     * Opens an InputStream from a given URL
     */
    public static InputStream openUrlAsStream(String url, Configuration conf) throws IOException {
        InputStream inputStream;
        if (Boolean.getBoolean("calvalus.accesscontrol.external") && jobClientsMapSingleton != null) {
            String userName = UserGroupInformation.getCurrentUser().getShortUserName();
            final Path path = new Path(url);
            FileSystem fs = jobClientsMapSingleton.getFileSystem(userName, conf, path);
            System.out.println("HadoopProcessingService.openUrlAsStream user " + userName + " path " + url + " fileSystem " + fs);
            inputStream = fs.open(path);
        } else if (url.startsWith("hdfs:")) {
            final Path path = new Path(url);
            inputStream = path.getFileSystem(conf).open(path);
        } else {
            inputStream = new URL(url).openStream();
        }
        return inputStream;
    }

    @Override
    public void invalidateBundleCache() {
        synchronized (bundleQueryCache) {
            bundleQueryCache.clear();
        }
    }
}
