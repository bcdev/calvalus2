package com.bc.calvalus.processing.fire.format.grid.s2;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by Thomas on 11.11.2016.
 */
public class S2GridInputFormatTest {

    @Test
    public void name() throws Exception {
        assertTrue("hdfs://calvalus/calvalus/projects/fire/s2-ba/T28PHU/BA-T28PHU-20160527T112220.nc".matches(
                "hdfs://calvalus/calvalus/projects/fire/s2-ba/(T27NYB|T28PHU)/BA-.*201605.*nc"));

    }
}