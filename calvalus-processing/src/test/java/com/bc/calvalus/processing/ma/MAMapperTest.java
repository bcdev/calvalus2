package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
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

    private static final int EXCLUSION_REASON_INDEX = 0;

    private List<float[]> expectedMatchups;

    @Before
    public void setUp() throws Exception {
        expectedMatchups = new ArrayList<float[]>();
        //                                X    ,    Y   ,  radiance_1
        expectedMatchups.add(new float[]{475.0f, 1353.0f, 68.66416f});
        expectedMatchups.add(new float[]{477.0f, 1353.0f, 68.417816f});
        expectedMatchups.add(new float[]{476.0f, 1365.0f, 67.13871f});
        expectedMatchups.add(new float[]{477.0f, 1366.0f, 67.29978f});
        expectedMatchups.add(new float[]{466.0f, 1372.0f, 65.49009f});
        expectedMatchups.add(new float[]{467.0f, 1386.0f, 66.52284f});
    }


    @Test
    public void testMatchUp_WindowSize3() throws Exception {
        final List<RecordWritable> collectedMatchUps = new ArrayList<RecordWritable>();

        executeMatchup(collectedMatchUps, 3, false);

        assertEquals(6, collectedMatchUps.size());
        assertEquals(9, getAggregatedNumber(collectedMatchUps.get(0), 2).data.length);
        testMatchUp(collectedMatchUps, 0);
        testMatchUp(collectedMatchUps, 1);
        testMatchUp(collectedMatchUps, 2);
        testMatchUp(collectedMatchUps, 3);
        testMatchUp(collectedMatchUps, 4);
        testMatchUp(collectedMatchUps, 5);
    }

    @Test
    public void testMatchUp_WindowSize3_FilterOverlapping() throws Exception {
        final List<RecordWritable> collectedMatchUps = new ArrayList<RecordWritable>();

        executeMatchup(collectedMatchUps, 3, true);

        for (RecordWritable collectedMatchUp : collectedMatchUps) {
            System.out.println("collectedMatchUp = " + collectedMatchUp);
        }
        assertEquals(6, collectedMatchUps.size());
        assertEquals(9, getAggregatedNumber(collectedMatchUps.get(0), 2).data.length);
        testMatchUp(collectedMatchUps, 0);
        testMatchUp(collectedMatchUps, 1, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING);
        testMatchUp(collectedMatchUps, 2, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING);
        testMatchUp(collectedMatchUps, 3);
        testMatchUp(collectedMatchUps, 4);
        testMatchUp(collectedMatchUps, 5);
    }

    @Test
    public void testMatchUp_WindowSize5() throws Exception {
        final List<RecordWritable> collectedMatchUps = new ArrayList<RecordWritable>();

        executeMatchup(collectedMatchUps, 5, false);

        assertEquals(6, collectedMatchUps.size());
        assertEquals(25, getAggregatedNumber(collectedMatchUps.get(0), 2).data.length);
        testMatchUp(collectedMatchUps, 0);
        testMatchUp(collectedMatchUps, 1);
        testMatchUp(collectedMatchUps, 2);
        testMatchUp(collectedMatchUps, 3);
        testMatchUp(collectedMatchUps, 4);
        testMatchUp(collectedMatchUps, 5);
    }

    private void testMatchUp(List<RecordWritable> collectedMatchUps, int matchUpIndex) {
        testMatchUp(collectedMatchUps, matchUpIndex, "");
    }

    private void testMatchUp(List<RecordWritable> collectedMatchUps, int matchUpIndex, String expectedReason) {
        int xColumn = 2;
        int yColumn = 3;
        int rad1Column = 6;

        float[] expectedData = expectedMatchups.get(matchUpIndex);
        RecordWritable actualRecord = collectedMatchUps.get(matchUpIndex);
        assertEquals(expectedData[0], getCenterMatchupValue(actualRecord, xColumn), 1.0e-6f);
        assertEquals(expectedData[1], getCenterMatchupValue(actualRecord, yColumn), 1.0e-6f);
        assertEquals(expectedData[2], getCenterMatchupValue(actualRecord, rad1Column), 1.0e-6f);

        String actualReason = (String) actualRecord.getAnnotationValues()[EXCLUSION_REASON_INDEX];
        assertEquals(expectedReason, actualReason);
    }

    private void executeMatchup(final List<RecordWritable> collectedMatchups, int macroPixelSize, boolean filterOverlapping) throws Exception {
        MAMapper mapper = new MAMapper();

        Mapper.Context context = Mockito.mock(Mapper.Context.class);
        FileSplit split = Mockito.mock(FileSplit.class);
        URI uri = MAMapperTest.class.getResource("/eodata/MER_RR__1P_TEST.N1").toURI();
        Mockito.when(split.getPath()).thenReturn(new Path(uri));
        Mockito.when(split.getLength()).thenReturn(1024L);
        Mockito.when(context.getInputSplit()).thenReturn(split);
        Mockito.when(context.getCounter(Mockito.anyString(), Mockito.anyString())).thenReturn(Mockito.mock(Counter.class));
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
        maConfig.setMacroPixelSize(macroPixelSize);
        maConfig.setFilteredMeanCoeff(0.0);
        maConfig.setFilterOverlapping(filterOverlapping);
        maConfig.setMaxTimeDifference(1.0);
//        maConfig.setCopyInput(true);

        jobConf.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());
        Mockito.when(context.getConfiguration()).thenReturn(jobConf);
        mapper.run(context);
    }

    private float getCenterMatchupValue(RecordWritable record, int columnIndex) {
        float[] data = getAggregatedNumber(record, columnIndex).data;
        return data[data.length / 2];
    }

    private AggregatedNumber getAggregatedNumber(RecordWritable record, int columnIndex) {
        return (AggregatedNumber) record.getAttributeValues()[columnIndex];
    }

}
