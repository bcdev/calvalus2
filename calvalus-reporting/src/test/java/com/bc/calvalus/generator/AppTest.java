package com.bc.calvalus.generator;

import com.bc.calvalus.generator.writer.WriteJobDetail;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

/**
 * @author muhammad.bc.
 */
@RunWith(FileConnectionCheckIO.class)
public class AppTest {
    @Test
    @Ignore
    public void testGetLastJobID() throws Exception {
        Main app = new Main(TestUtils.getSaveLocation());
        String lastJobID = app.getLastJobID();
        assertNotNull(lastJobID);

        int[] rangeIndex = app.getRangeIndex(lastJobID);
        System.out.println(rangeIndex[0]);
        System.out.println(rangeIndex[1]);


        WriteJobDetail writeJobDetail = new WriteJobDetail(TestUtils.getSaveLocation());
//        writeJobDetail.write(rangeIndex[0], rangeIndex[1], app.getJobsType());
    }
}