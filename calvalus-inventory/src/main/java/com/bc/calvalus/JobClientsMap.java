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

/**
 * Creates a JobClient for a given User
 */
public class JobClientsMap {

    private final JobConf jobConfTemplate;
    private final Map<String, JobClient> jobClients;

    public JobClientsMap(JobConf jobConf) {
        this.jobConfTemplate = jobConf;
        jobClients = new HashMap<>();
    }

    public Configuration getConfiguration() {
        return jobConfTemplate;
    }

    public synchronized JobClient getJobClient(String userName) throws IOException {
        if (jobClients.get(userName) == null) {
            System.out.println("CREATING new JobClient for: " + userName);
            UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(userName);
            try {
                JobClient jobClient = remoteUser.doAs(new PrivilegedExceptionAction<JobClient>() {
                    @Override
                    public JobClient run() throws Exception {
                        JobConf jobConfClone = new JobConf(jobConfTemplate);
                        return new JobClient(jobConfClone);
                    }
                });
                jobClients.put(userName, jobClient);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return jobClients.get(userName);
    }

    public FileSystem getFileSystem(String userName, String path) throws IOException {
        URI uri = new Path(path).toUri();
        try {
            return FileSystem.get(uri, jobConfTemplate, userName);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public void close() throws IOException {
        for (JobClient jobClient : jobClients.values()) {
            jobClient.close();
        }
        jobClients.clear();
    }
}
