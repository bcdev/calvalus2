/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.inventory.hadoop.CalvalusShFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

/**
 * Creates a JobClient for a given User
 */
public class JobClientsMap {

    private static final Logger LOG = CalvalusLogger.getLogger();
     // 10 minutes in milliseconds
    private static final long T_10_MINUTES = 10 * 60 * 1000;
    // 3 hours in milliseconds
    private static final long T_3_HOURS = 3 * 60 * 60 * 1000;

    private final JobConf jobConfTemplate;
    private final Map<String, CacheEntry> jobClientsCache = new HashMap<>();
    private final Timer cacheCleaner;
    private final Map<String, FileSystem> fileSystemMap = new HashMap<>();
    private boolean withExternalAccessControl;

    public JobClientsMap(JobConf jobConf) {
        this.jobConfTemplate = jobConf;
        withExternalAccessControl = Boolean.getBoolean("calvalus.accesscontrol.external");
        // TODO there should be one Timer for a process that is used for all timer tasks
        this.cacheCleaner = new Timer("jobClientsCacheCleaner", true);
        TimerTask jobClientsCacheRemoveUnused = new TimerTask() {
            @Override
            public void run() {
                removeUnusedEntries();
            }
        };
        this.cacheCleaner.scheduleAtFixedRate(jobClientsCacheRemoveUnused, T_10_MINUTES, T_10_MINUTES);
        TimerTask jobClientsCacheRemoveAll = new TimerTask() {
            @Override
            public void run() {
                removeAllEntries();
            }
        };
        this.cacheCleaner.scheduleAtFixedRate(jobClientsCacheRemoveAll, T_3_HOURS, T_3_HOURS);
    }

    public Configuration getConfiguration() {
        return jobConfTemplate;
    }

    public synchronized JobClient getJobClient(String userName) throws IOException {
        if (jobClientsCache.get(userName) == null) {
            System.out.println("CREATING new JobClient for: " + userName);
//            if ("anonymous".equals(userName)) {
//                new Exception("Where is anonymous created=").printStackTrace();
//            }
            UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(userName);
            JobClient jobClient = null;
            try {
                jobClient = remoteUser.doAs((PrivilegedExceptionAction<JobClient>) () -> new JobClient(new JobConf(jobConfTemplate)));
            } catch (InterruptedException e) {
                throw new IOException(e);   
            }
            CacheEntry cacheEntry = new CacheEntry(jobClient);
            jobClientsCache.put(userName, cacheEntry);
            if (withExternalAccessControl) {
                CalvalusShFileSystem.createOrRegister(userName, cacheEntry, fileSystemMap);
            }
            return jobClient;
        }
        return jobClientsCache.get(userName).getJobClient();
    }

    public FileSystem getFileSystem(String username) throws IOException {
        FileSystem fileSystem;
        if (withExternalAccessControl) {
            fileSystem = fileSystemMap.get(username);
            if (fileSystem == null) {
                getJobClient(username);
                fileSystem = fileSystemMap.get(username);
            }
        } else {
            fileSystem = getJobClient(username).getFs();
        }
        //LOG.info("JobClientsMap user " + username + " fs " + fileSystem + " jcm " + this);
        return fileSystem;
    }

    public FileSystem getFileSystem(String username, String path) throws IOException {
        FileSystem fileSystem;
        if (withExternalAccessControl) {
            fileSystem = fileSystemMap.get(username);
            if (fileSystem == null) {
                getJobClient(username);
                fileSystem = fileSystemMap.get(username);
            }
        } else {
            URI uri = new Path(path).toUri();
            try {
                fileSystem = FileSystem.get(uri, jobConfTemplate, username);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        //LOG.info("JobClientsMap user " + username + " fs " + fileSystem + " jcm " + this);
        return fileSystem;
    }

    public FileSystem getFileSystem(String username, Configuration conf, Path path) throws IOException {
        FileSystem fileSystem;
        if (withExternalAccessControl) {
            fileSystem = fileSystemMap.get(username);
            if (fileSystem == null) {
                getJobClient(username);
                fileSystem = fileSystemMap.get(username);
            }
        } else {
            try {
                fileSystem = FileSystem.get(path.toUri(), conf, username);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        //LOG.info("JobClientsMap user " + username + " fs " + fileSystem + " jcm " + this);
        return fileSystem;
    }

    public synchronized void close() throws IOException {
        cacheCleaner.cancel();
        for (CacheEntry cacheEntry : jobClientsCache.values()) {
            cacheEntry.jobClient.close();
        }
        jobClientsCache.clear();
        fileSystemMap.clear();
    }

    /**
     * We remove JobClients after being unused for 10 minutes.
     * Inside it contains a "org.apache.hadoop.mapred.ClientCache" that has a forever growing cache.
     */
    private synchronized void removeUnusedEntries() {
        long now = System.currentTimeMillis();
        long clearIfOlder = now - T_10_MINUTES;
        if (withExternalAccessControl) {
            fileSystemMap.values().removeIf(fileSystem -> ((fileSystem instanceof CalvalusShFileSystem)
                    && ((CalvalusShFileSystem) fileSystem).getCacheEntry().accessTime < clearIfOlder));
        }
        jobClientsCache.values().removeIf(cacheEntry -> cacheEntry.accessTime < clearIfOlder);
    }

    /**
     * We remove JobClients after 3 hours. IN ALL CASES. Because the update thread always triggers the accessTime
     * Inside it contains a "org.apache.hadoop.mapred.ClientCache" that has a forever growing cache.
     */
    public synchronized void removeAllEntries() {
        if (withExternalAccessControl) {
            fileSystemMap.clear();
        }
        jobClientsCache.clear();
    }

    public static class CacheEntry  {
        private final JobClient jobClient;
        private long accessTime;

        public CacheEntry(JobClient jobClient) {
            this.jobClient = jobClient;
            accessTime = System.currentTimeMillis();
        }

        JobClient getJobClient() {
            accessTime = System.currentTimeMillis();
            return jobClient;
        }

        public JobClient getJobClientInternal() {
            return jobClient;
        }
        public void setAccessTime() {
            accessTime = System.currentTimeMillis();
        }
    }
}
