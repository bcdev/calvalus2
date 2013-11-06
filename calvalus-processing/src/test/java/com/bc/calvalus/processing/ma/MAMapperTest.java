package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.JobConfigNames;
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

import static org.junit.Assert.*;

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

        assertEquals(6, collectedMatchups.size());
        int xColumn = 2;
        assertEquals(476.0f, getCenterMatchupValue(collectedMatchups, 0, xColumn), 1.0e-6f);
        assertEquals(477.0f, getCenterMatchupValue(collectedMatchups, 1, xColumn), 1.0e-6f);
        assertEquals(475.0f, getCenterMatchupValue(collectedMatchups, 2, xColumn), 1.0e-6f);
        assertEquals(477.0f, getCenterMatchupValue(collectedMatchups, 3, xColumn), 1.0e-6f);
        assertEquals(467.0f, getCenterMatchupValue(collectedMatchups, 4, xColumn), 1.0e-6f);
        assertEquals(466.0f, getCenterMatchupValue(collectedMatchups, 5, xColumn), 1.0e-6f);
        int yColumn = 3;
        assertEquals(1365.0f, getCenterMatchupValue(collectedMatchups, 0, yColumn), 1.0e-6f);
        assertEquals(1366.0f, getCenterMatchupValue(collectedMatchups, 1, yColumn), 1.0e-6f);
        assertEquals(1353.0f, getCenterMatchupValue(collectedMatchups, 2, yColumn), 1.0e-6f);
        assertEquals(1353.0f, getCenterMatchupValue(collectedMatchups, 3, yColumn), 1.0e-6f);
        assertEquals(1386.0f, getCenterMatchupValue(collectedMatchups, 4, yColumn), 1.0e-6f);
        assertEquals(1372.0f, getCenterMatchupValue(collectedMatchups, 5, yColumn), 1.0e-6f);
        int rad1Column = 6;
        assertEquals(67.13871f, getCenterMatchupValue(collectedMatchups, 0, rad1Column), 1.0e-6f);
        assertEquals(67.29978f, getCenterMatchupValue(collectedMatchups, 1, rad1Column), 1.0e-6f);
        assertEquals(68.66416f, getCenterMatchupValue(collectedMatchups, 2, rad1Column), 1.0e-6f);
        assertEquals(68.417816f, getCenterMatchupValue(collectedMatchups, 3, rad1Column), 1.0e-6f);
        assertEquals(66.52284f, getCenterMatchupValue(collectedMatchups, 4, rad1Column), 1.0e-6f);
        assertEquals(65.49009f, getCenterMatchupValue(collectedMatchups, 5, rad1Column), 1.0e-6f);

    }

    private float getCenterMatchupValue(List<RecordWritable> collectedMatchups, int matchupIndex, int columnIndex) {
        float[] data = getAggregatedNumber(collectedMatchups, matchupIndex, columnIndex).data;
        return data[data.length / 2];
    }

    private AggregatedNumber getAggregatedNumber(List<RecordWritable> matchups, int matchupIndex, int columnIndex) {
        return (AggregatedNumber) matchups.get(matchupIndex).getValues()[columnIndex];
    }

}
