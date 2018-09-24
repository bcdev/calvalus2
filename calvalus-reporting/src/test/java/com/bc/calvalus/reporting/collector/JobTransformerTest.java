package com.bc.calvalus.reporting.collector;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.commons.util.PropertiesWrapper;
import org.apache.commons.io.IOUtils;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;

/**
 * @author hans
 */
public class JobTransformerTest {

    private JobTransformer jobTransformer;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("reporting-collector-test.properties");
        jobTransformer = new JobTransformer();
    }

    @Test
    public void canTransformConf() throws Exception {
        StringReader reader = jobTransformer.applyConfXslt(getSampleConfInputStream());
        String transformedString = IOUtils.toString(reader);

        assertThat(transformedString, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                                              "<conf>" +
                                                "<path>hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/06/29/000000/job_1498650116199_0092_conf.xml</path>" +
                                                "<workflowType>L2</workflowType>" +
                                              "</conf>"));
        reader.close();
    }

    @Test
    public void canTransformCounters() throws Exception {
        StringReader reader = jobTransformer.applyCountersXslt(getSampleCountersInputStream());
        String transformedString = IOUtils.toString(reader);

        assertThat(transformedString, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                                              "<jobCounters>" +
                                                  "<id>job_1498650116199_0092</id>" +
                                                  "<counterGroup>" +
                                                      "<counterGroupName>org.apache.hadoop.mapreduce.FileSystemCounter</counterGroupName>" +
                                                      "<counter>" +
                                                          "<name>FILE_BYTES_READ</name>" +
                                                          "<totalCounterValue>0</totalCounterValue>" +
                                                          "<mapCounterValue>0</mapCounterValue>" +
                                                          "<reduceCounterValue>0</reduceCounterValue>" +
                                                      "</counter>" +
                                                      "<counter>" +
                                                          "<name>FILE_BYTES_WRITTEN</name>" +
                                                          "<totalCounterValue>266437</totalCounterValue>" +
                                                          "<mapCounterValue>266437</mapCounterValue>" +
                                                          "<reduceCounterValue>0</reduceCounterValue>" +
                                                      "</counter>" +
                                                      "<counter>" +
                                                          "<name>HDFS_BYTES_READ</name>" +
                                                          "<totalCounterValue>266437</totalCounterValue>" +
                                                          "<mapCounterValue>266437</mapCounterValue>" +
                                                          "<reduceCounterValue>0</reduceCounterValue>" +
                                                      "</counter>" +
                                                      "<counter>" +
                                                          "<name>HDFS_BYTES_WRITTEN</name>" +
                                                          "<totalCounterValue>266437</totalCounterValue>" +
                                                          "<mapCounterValue>266437</mapCounterValue>" +
                                                          "<reduceCounterValue>0</reduceCounterValue>" +
                                                      "</counter>" +
                                                      "<counter>" +
                                                          "<name>MB_MILLIS_MAPS</name>" +
                                                          "<totalCounterValue>1</totalCounterValue>" +
                                                          "<mapCounterValue>0</mapCounterValue>" +
                                                          "<reduceCounterValue>0</reduceCounterValue>" +
                                                      "</counter>" +
                                                      "<counter>" +
                                                          "<name>MB_MILLIS_REDUCES</name>" +
                                                          "<totalCounterValue>1</totalCounterValue>" +
                                                          "<mapCounterValue>0</mapCounterValue>" +
                                                          "<reduceCounterValue>0</reduceCounterValue>" +
                                                      "</counter>" +
                                                      "<counter>" +
                                                          "<name>VCORES_MILLIS_MAPS</name>" +
                                                          "<totalCounterValue>160210</totalCounterValue>" +
                                                          "<mapCounterValue>0</mapCounterValue>" +
                                                          "<reduceCounterValue>0</reduceCounterValue>" +
                                                      "</counter>" +
                                                      "<counter>" +
                                                          "<name>VCORES_MILLIS_REDUCES</name>" +
                                                          "<totalCounterValue>160210</totalCounterValue>" +
                                                          "<mapCounterValue>0</mapCounterValue>" +
                                                          "<reduceCounterValue>0</reduceCounterValue>" +
                                                      "</counter>" +
                                                      "<counter>" +
                                                          "<name>CPU_MILLISECONDS</name>" +
                                                          "<totalCounterValue>0</totalCounterValue>" +
                                                          "<mapCounterValue>0</mapCounterValue>" +
                                                          "<reduceCounterValue>0</reduceCounterValue>" +
                                                      "</counter>" +
                                                  "</counterGroup>" +
                                              "</jobCounters>"));
        reader.close();
    }

    private InputStream getSampleCountersInputStream() {
        String sampleCounters = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                "<jobCounters>" +
                                "   <id>job_1498650116199_0092</id>" +
                                "   <counterGroup>" +
                                "       <counterGroupName>org.apache.hadoop.mapreduce.FileSystemCounter</counterGroupName>" +
                                "       <counter>" +
                                "           <name>FILE_BYTES_READ</name>" +
                                "           <totalCounterValue>0</totalCounterValue>" +
                                "           <mapCounterValue>0</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "       <counter>" +
                                "           <name>FILE_BYTES_WRITTEN</name>" +
                                "           <totalCounterValue>266437</totalCounterValue>" +
                                "           <mapCounterValue>266437</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "       <counter>" +
                                "           <name>HDFS_BYTES_READ</name>" +
                                "           <totalCounterValue>266437</totalCounterValue>" +
                                "           <mapCounterValue>266437</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "       <counter>" +
                                "           <name>HDFS_BYTES_WRITTEN</name>" +
                                "           <totalCounterValue>266437</totalCounterValue>" +
                                "           <mapCounterValue>266437</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "   </counterGroup>" +
                                "   <counterGroup>" +
                                "       <counterGroupName>org.apache.hadoop.mapreduce.JobCounter</counterGroupName>" +
                                "       <counter>" +
                                "           <name>MB_MILLIS_MAPS</name>" +
                                "           <totalCounterValue>1</totalCounterValue>" +
                                "           <mapCounterValue>0</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "       <counter>" +
                                "           <name>MB_MILLIS_REDUCES</name>" +
                                "           <totalCounterValue>1</totalCounterValue>" +
                                "           <mapCounterValue>0</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "       <counter>" +
                                "           <name>VCORES_MILLIS_MAPS</name>" +
                                "           <totalCounterValue>160210</totalCounterValue>" +
                                "           <mapCounterValue>0</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "       <counter>" +
                                "           <name>VCORES_MILLIS_REDUCES</name>" +
                                "           <totalCounterValue>160210</totalCounterValue>" +
                                "           <mapCounterValue>0</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "   </counterGroup>" +
                                "   <counterGroup>" +
                                "       <counterGroupName>org.apache.hadoop.mapreduce.TaskCounter</counterGroupName>" +
                                "       <counter>" +
                                "           <name>CPU_MILLISECONDS</name>" +
                                "           <totalCounterValue>0</totalCounterValue>" +
                                "           <mapCounterValue>0</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "   </counterGroup>" +
                                "   <counterGroup>" +
                                "       <counterGroupName>Products</counterGroupName>" +
                                "       <counter>" +
                                "           <name>Product processed</name>" +
                                "           <totalCounterValue>1</totalCounterValue>" +
                                "           <mapCounterValue>1</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "   </counterGroup>" +
                                "   <counterGroup>" +
                                "       <counterGroupName>org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter</counterGroupName>" +
                                "       <counter>" +
                                "           <name>BYTES_READ</name>" +
                                "           <totalCounterValue>0</totalCounterValue>" +
                                "           <mapCounterValue>0</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "   </counterGroup>" +
                                "   <counterGroup>" +
                                "       <counterGroupName>org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter</counterGroupName>" +
                                "       <counter>" +
                                "           <name>BYTES_WRITTEN</name>" +
                                "           <totalCounterValue>0</totalCounterValue>" +
                                "           <mapCounterValue>0</mapCounterValue>" +
                                "           <reduceCounterValue>0</reduceCounterValue>" +
                                "       </counter>" +
                                "   </counterGroup>" +
                                "</jobCounters>";
        return new ByteArrayInputStream(sampleCounters.getBytes());
    }

    private InputStream getSampleConfInputStream() {
        String sampleConf = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                            "   <conf>" +
                            "       <path>hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/06/29/000000/job_1498650116199_0092_conf.xml</path>" +
                            "       <property>" +
                            "           <name>mapreduce.jobtracker.address</name>" +
                            "           <value>local</value>" +
                            "           <source>mapred-default.xml</source>" +
                            "           <source>job.xml</source>" +
                            "           <source>hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/06/29/000000/job_1498650116199_0092_conf.xml</source>" +
                            "       </property>" +
                            "       <property>" +
                            "           <name>dfs.namenode.resource.check.interval</name>" +
                            "           <value>5000</value>" +
                            "           <source>hdfs-default.xml</source>" +
                            "           <source>job.xml</source>" +
                            "           <source>hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/06/29/000000/job_1498650116199_0092_conf.xml</source>" +
                            "       </property>" +
                            "       <property>" +
                            "           <name>mapreduce.jobhistory.client.thread-count</name>" +
                            "           <value>10</value><source>mapred-default.xml</source>" +
                            "           <source>job.xml</source>" +
                            "           <source>hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/06/29/000000/job_1498650116199_0092_conf.xml</source>" +
                            "       </property>" +
                            "       <property>" +
                            "           <name>yarn.admin.acl</name>" +
                            "           <value>*</value>" +
                            "           <source>yarn-default.xml</source>" +
                            "           <source>job.xml</source>" +
                            "           <source>hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/06/29/000000/job_1498650116199_0092_conf.xml</source>" +
                            "       </property>" +
                            "       <property>" +
                            "           <name>yarn.app.mapreduce.am.job.committer.cancel-timeout</name>" +
                            "           <value>60000</value>" +
                            "           <source>mapred-default.xml</source>" +
                            "           <source>job.xml</source>" +
                            "           <source>hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/06/29/000000/job_1498650116199_0092_conf.xml</source>" +
                            "       </property>" +
                            "       <property>" +
                            "           <name>calvalus.productionType</name>" +
                            "           <value>L2</value>" +
                            "           <source>programatically</source>" +
                            "           <source>job.xml</source>" +
                            "           <source>hdfs://calvalus:8020/tmp/hadoop-yarn/staging/history/done/2017/07/01/000000/job_1498650116199_0708_conf.xml</source>" +
                            "       </property>" +
                            "   </conf>";
        return new ByteArrayInputStream(sampleConf.getBytes());
    }
}