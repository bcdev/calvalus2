/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bc.calvalus.experiments;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Test case to run a MapReduce job.
 * <p/>
 * It runs a 2-node cluster with a 2-node DFS.
 * <p/>
 * The {@code JobConf} to use must be obtained via the {@code createJobConf()} method.
 * <p/>
 * It creates a temporary directory -accessible via  {@code getTestRootDir()} -
 * for both input and output.
 * <p/>
 * The input directory is accessible via  {@code getInputDir()} and the output
 * directory via  {@code getOutputDir() }.
 * <p/>
 * The DFS filesystem is formatted before the test case starts and after it ends.
 *
 * @author Marco Zuehlke
 * @since 0.1
 */
public class MapReduceTestCluster {
    private static final String TEST_LOGS = "target/hadoop-test-logs";
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";

    private MiniDFSCluster dfsCluster = null;
    private MiniMRCluster mrCluster = null;

    /**
     * Creates Hadoop Cluster and DFS before a test case is run.
     *
     * @throws Exception
     */
    public void setUpCluster() throws Exception {
        startCluster(true, null);
    }

    /**
     * Starts the cluster within a testcase.
     * <p/>
     * Note that the cluster is already started when the testcase method
     * is invoked. This method is useful if as part of the testcase the
     * cluster has to be shutdown and restarted again.
     * <p/>
     * If the cluster is already running this method does nothing.
     *
     * @param reformatDFS indicates if DFS has to be reformated
     * @param props       configuration properties to inject to the mini cluster
     * @throws Exception if the cluster could not be started
     */
    public synchronized void startCluster(boolean reformatDFS, Properties props)
            throws Exception {
        // make sure the log folder exists,
        // otherwise the test fill fail
        final File testLogs = new File(TEST_LOGS);
        testLogs.mkdirs();
        testLogs.deleteOnExit();

        System.setProperty("hadoop.log.dir", TEST_LOGS);
        System.setProperty("javax.xml.parsers.SAXParserFactory",
                           "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        if (dfsCluster == null) {
            JobConf conf = new JobConf();
            if (props != null) {
                for (Map.Entry entry : props.entrySet()) {
                    conf.set((String) entry.getKey(), (String) entry.getValue());
                }
            }
            dfsCluster = new MiniDFSCluster(conf, 4, reformatDFS,
                                            new String[]{"/rack0", "/rack0", "/rack1", "/rack1"},
                                            new String[]{"host0", "host1", "host2", "host3"});

            ConfigurableMiniMRCluster.setConfiguration(props);
            dfsCluster.getFileSystem().makeQualified(getInputDir());
            dfsCluster.getFileSystem().makeQualified(getOutputDir());
            //noinspection deprecation
            mrCluster = new ConfigurableMiniMRCluster(2, getFileSystem().getName(), 1);
        }
    }

    private static class ConfigurableMiniMRCluster extends MiniMRCluster {
        private static Properties config;

        public static void setConfiguration(Properties props) {
            config = props;
        }

        public ConfigurableMiniMRCluster(int numTaskTrackers, String namenode,
                                         int numDir) throws Exception {
            super(numTaskTrackers, namenode, numDir);
        }

        public JobConf createJobConf() {
            JobConf conf = super.createJobConf();
            if (config != null) {
                for (Map.Entry entry : config.entrySet()) {
                    conf.set((String) entry.getKey(), (String) entry.getValue());
                }
            }
            return conf;
        }
    }

    /**
     * Stops the cluster within a testcase.
     * <p/>
     * Note that the cluster is already started when the testcase method
     * is invoked. This method is useful if as part of the testcase the
     * cluster has to be shutdown.
     * <p/>
     * If the cluster is already stopped this method does nothing.
     *
     * @throws Exception if the cluster could not be stopped
     */
    public void stopCluster() throws Exception {
        if (mrCluster != null) {
            mrCluster.shutdown();
            mrCluster = null;
        }
        if (dfsCluster != null) {
            dfsCluster.shutdown();
            dfsCluster = null;
        }
    }

    /**
     * Destroys Hadoop Cluster and DFS after a test case is run.
     *
     * @throws Exception
     */
    public void tearDownCluster() throws Exception {
        stopCluster();
    }

    /**
     * Returns a preconfigured Filesystem instance for test cases to read and
     * write files to it.
     * <p/>
     * TestCases should use this Filesystem instance.
     *
     * @return the filesystem used by Hadoop.
     * @throws IOException
     */
    public FileSystem getFileSystem() throws IOException {
        return dfsCluster.getFileSystem();
    }

    public MiniMRCluster getMRCluster() {
        return mrCluster;
    }

    /**
     * Returns the path to the root directory for the testcase.
     *
     * @return path to the root directory for the testcase.
     */
    public Path getTestRootDir() {
        return new Path("x").getParent();
    }

    /**
     * Returns a path to the input directory for the testcase.
     *
     * @return path to the input directory for the tescase.
     */
    public Path getInputDir() {
        return new Path(INPUT);
    }

    /**
     * Returns a path to the output directory for the testcase.
     *
     * @return path to the output directory for the tescase.
     */
    public Path getOutputDir() {
        return new Path(OUTPUT);
    }

    /**
     * Returns a job configuration preconfigured to run against the Hadoop
     * managed by the testcase.
     *
     * @return configuration that works on the testcase Hadoop instance
     */
    public JobConf createJobConf() {
        return mrCluster.createJobConf();
    }

}
