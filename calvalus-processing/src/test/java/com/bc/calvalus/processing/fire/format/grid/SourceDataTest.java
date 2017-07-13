package com.bc.calvalus.processing.fire.format.grid;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SourceDataTest {

    @Test
    public void merge() throws Exception {
        SourceData data1 = new SourceData(1, 1);
        SourceData data2 = new SourceData(1, 1);
        SourceData data3 = new SourceData(1, 1);

        data1.reset();
        data2.reset();
        data3.reset();

        List<SourceData> data = new ArrayList<>();
        data.add(data1);
        data.add(data2);
        data.add(data3);

        SourceData merged = SourceData.merge(data);

        assertEquals(3, merged.width);
        assertEquals(1, merged.height);


    }

}