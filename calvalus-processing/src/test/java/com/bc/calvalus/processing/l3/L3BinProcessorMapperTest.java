package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class L3BinProcessorMapperTest {

    @Test
    public void testGetMeanMJD() throws Exception {
        Configuration conf = new Configuration();
        conf.set(JobConfigNames.CALVALUS_MIN_DATE, "2005-04-10");
        conf.set(JobConfigNames.CALVALUS_MAX_DATE, "2005-04-20");
        assertEquals(53475.5, L3BinProcessorMapper.getMeanMJD(conf), 1e-6);

        conf = new Configuration();
        conf.set(JobConfigNames.CALVALUS_MIN_DATE, "2005-04-20");
        conf.set(JobConfigNames.CALVALUS_MAX_DATE, "2005-04-20");
        assertEquals(53480.5, L3BinProcessorMapper.getMeanMJD(conf), 1e-6);

        conf = new Configuration();
        assertEquals(0.0, L3BinProcessorMapper.getMeanMJD(conf), 1e-6);

        conf.set(JobConfigNames.CALVALUS_MIN_DATE, "2005-04-20");
        assertEquals(0.0, L3BinProcessorMapper.getMeanMJD(conf), 1e-6);
    }

    @Test
    public void testGetMJD() throws Exception {
        assertEquals(53480.0, L3BinProcessorMapper.getMJD("2005-04-20"), 1e-6);
        assertEquals(53481.0, L3BinProcessorMapper.getMJD("2005-04-21"), 1e-6);
        assertEquals(0.0, L3BinProcessorMapper.getMJD(null), 1e-6);
        assertEquals(0.0, L3BinProcessorMapper.getMJD("foo"), 1e-6);
    }
}
