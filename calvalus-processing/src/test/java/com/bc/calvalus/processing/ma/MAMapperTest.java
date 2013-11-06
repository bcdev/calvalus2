package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.JobConfigNames;
import com.vividsolutions.jts.util.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marco Peters
 */
public class MAMapperTest {

    @Test
    public void testRun() throws Exception {
        MAMapper mapper = new MAMapper();


        Mapper.Context context = Mockito.mock(Mapper.Context.class);
        FileSplit split = Mockito.mock(FileSplit.class);
        URI uri = MAMapperTest.class.getResource("/eodata/MER_RR__1P_TEST.N1").toURI();
        Mockito.when(split.getPath()).thenReturn(new Path(uri));
        Mockito.when(split.getLength()).thenReturn(1024L);
        Mockito.when(context.getInputSplit()).thenReturn(split);
        Mockito.when(context.getCounter(Mockito.anyString(), Mockito.anyString())).thenReturn(Mockito.mock(Counter.class));
        final List<RecordWritable> collectedMatchups = new ArrayList<RecordWritable>();
        Answer recordAnswer = new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                Text key = (Text) args[0];
                RecordWritable value = (RecordWritable) args[1];
                if (key.toString().startsWith("MER_RR__1P_TEST")) {
                    collectedMatchups.add(value);
                }
                return null;
            }
        };
        Mockito.doAnswer(recordAnswer).when(context).write(Mockito.any(Text.class), Mockito.any(RecordWritable.class));
        Configuration jobConf = new Configuration(true);
        MAConfig maConfig = new MAConfig();
        final String url = MAMapperTest.class.getResource("MER_RR__1P_TEST_MA-Data.txt").toExternalForm();
        maConfig.setRecordSourceUrl(url);
        maConfig.setMacroPixelSize(3);
        maConfig.setFilteredMeanCoeff(0.0);
        maConfig.setMaxTimeDifference(1.0);

        jobConf.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());
        Mockito.when(context.getConfiguration()).thenReturn(jobConf);
        mapper.run(context);

        Assert.equals(6, collectedMatchups.size());
    }
}
