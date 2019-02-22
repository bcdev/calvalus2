package com.bc.calvalus.processing.l2;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class ProcessingMapperTest {
    @Test
    public void getProductName() throws Exception {
        String input = "S3A_OL_1_EFR____20181001T022958_20181001T023258_20181002T090807_0179_036_203_1980_MAR_O_NT_002.SEN3";
        String name = new ProcessingMapper().getProductName(new JobConf(), input + ".zip");
        assertEquals("L2_of_" + input, name);
    }

}