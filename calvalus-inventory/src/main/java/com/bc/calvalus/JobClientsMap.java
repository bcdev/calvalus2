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

/**
 * Creates a JobClient for a given User
 */
public class JobClientsMap {
     // 10 minutes in milliseconds
    private static final long CACHE_RETENTION = 10 * 60 * 1000;

    private final JobConf jobConfTemplate;
    private final Map<String, CacheEntry> jobClientsCache;
    private final Timer cacheCleaner;

    public JobClientsMap(JobConf jobConf) {
        this.jobConfTemplate = jobConf;
        jobClientsCache = new HashMap<>();
        this.cacheCleaner = new Timer("jobClientsCacheCleaner");
        TimerTask bundlesQueryCleanTask = new TimerTask() {
            @Override
            public void run() {
                removeUnusedEntries();
            }
        };
        this.cacheCleaner.scheduleAtFixedRate(bundlesQueryCleanTask, CACHE_RETENTION, CACHE_RETENTION);
    }

    public Configuration getConfiguration() {
        return jobConfTemplate;
    }

    public synchronized JobClient getJobClient(String userName) throws IOException {
        if (jobClientsCache.get(userName) == null) {
            System.out.println("CREATING new JobClient for: " + userName);
            UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(userName);
            try {
                JobClient jobClient = remoteUser.doAs((PrivilegedExceptionAction<JobClient>) () -> new JobClient(new JobConf(jobConfTemplate)));
                jobClientsCache.put(userName, new CacheEntry(jobClient));
                return jobClient;
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return jobClientsCache.get(userName).getJobClient();
    }

    public FileSystem getFileSystem(String userName, String path) throws IOException {
        URI uri = new Path(path).toUri();
        try {
            return FileSystem.get(uri, jobConfTemplate, userName);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public synchronized void close() throws IOException {
        cacheCleaner.cancel();
        for (CacheEntry cacheEntry : jobClientsCache.values()) {
            cacheEntry.jobClient.close();
        }
        jobClientsCache.clear();
    }

    /**
     * We remove JobClients after being unused for 10 minutes.
     * Inside it contains a "org.apache.hadoop.mapred.ClientCache" that has a forever growing cache.
     */
    private synchronized void removeUnusedEntries() {
        long now = System.currentTimeMillis();
        long clearIfOlder = now - CACHE_RETENTION;
        jobClientsCache.values().removeIf(cacheEntry -> cacheEntry.accessTime < clearIfOlder);
    }

    private static class CacheEntry  {
        private final JobClient jobClient;
        private long accessTime;

        CacheEntry(JobClient jobClient) {
            this.jobClient = jobClient;
            accessTime = System.currentTimeMillis();
        }

        JobClient getJobClient() {
            accessTime = System.currentTimeMillis();
            return jobClient;
        }
    }
}
