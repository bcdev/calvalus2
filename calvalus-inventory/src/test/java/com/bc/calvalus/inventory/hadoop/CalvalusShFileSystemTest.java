/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.JobClientsMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import static org.junit.Assert.*;


/**
 * @author MarcoZ
 * @author Norman
 */
public class CalvalusShFileSystemTest {

    @Test
    @Ignore
    public void testListStatus() throws Exception {
        System.setProperty("calvalus.accesscontrol.external", "true");
        String user = "boe";
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf());
        JobClient jobClient = jobClientsMap.getJobClient(user);
        FileSystem fileSystem = jobClientsMap.getFileSystem(user);
        for (FileStatus fileStatus : fileSystem.listStatus(new Path("/calvalus/testdir"))) {
            System.out.println(fileStatus.getPath() + " " + fileStatus.isDirectory());
        }
    }

    @Test
    @Ignore
    public void testGlob() throws Exception {
        System.setProperty("calvalus.accesscontrol.external", "true");
        String user = "dhus";
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf());
        JobClient jobClient = jobClientsMap.getJobClient(user);
        FileSystem fileSystem = jobClientsMap.getFileSystem(user);
        for (FileStatus fileStatus : fileSystem.globStatus(new Path("/calvalus/testdir/t*"))) {
            System.out.println(fileStatus.getPath() + " " + fileStatus.isDirectory());
        }
    }

    @Test
    @Ignore
    public void testCat() throws Exception {
        System.setProperty("calvalus.accesscontrol.external", "true");
        String user = "boe";
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf());
        JobClient jobClient = jobClientsMap.getJobClient(user);
        FileSystem fileSystem = jobClientsMap.getFileSystem(user);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(fileSystem.open(new Path("/calvalus/testdir/testfile"))))) {
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("|" + line + "|");
            }
        };
    }
}
