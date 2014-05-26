package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PixelPos;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Marco Peters
 */
public class OverlappingRecordSelectorTest {

    private static final int EXCLUSION_REASON_INDEX = 0;

    private ArrayList<Record> records;

    @Before
    public void setUp() throws Exception {
        records = new ArrayList<Record>();
    }

    @Test
    public void testSelection() throws Exception {
        int macroPixelSize = 3;
        ProductRecordSource.PixelPosRecordFactory recordFactory = Mockito.mock(ProductRecordSource.PixelPosRecordFactory.class);
        OverlappingRecordSelector selector = new OverlappingRecordSelector(macroPixelSize, recordFactory, new TestHeader());

        mockPixelPosRecord(recordFactory, 0, new PixelPos(10, 11), 10); // keep this one
        mockPixelPosRecord(recordFactory, 1, new PixelPos(100, 11), 10);
        mockPixelPosRecord(recordFactory, 2, new PixelPos(10, 12), 10);
        mockPixelPosRecord(recordFactory, 3, new PixelPos(100, 12), 2); // keep this one
        mockPixelPosRecord(recordFactory, 4, new PixelPos(10, 13), 10);
        mockPixelPosRecord(recordFactory, 5, new PixelPos(100, 13), 10);


        mockPixelPosRecord(recordFactory, 6, new PixelPos(10, 20), 4);
        mockPixelPosRecord(recordFactory, 7, new PixelPos(11, 20), 5);
        mockPixelPosRecord(recordFactory, 8, new PixelPos(9, 21), 3);
        mockPixelPosRecord(recordFactory, 9, new PixelPos(10, 21), 2); // keep this one
        mockPixelPosRecord(recordFactory, 10, new PixelPos(11, 21), 5);


        List<Record> selectedRecords = (List<Record>) selector.select(records);

        assertEquals(11, selectedRecords.size());
        assertRecord(0, "", selectedRecords.get(0));
        assertRecord(1, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING, selectedRecords.get(1));
        assertRecord(2, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING, selectedRecords.get(2));
        assertRecord(3, "", selectedRecords.get(3));
        assertRecord(4, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING, selectedRecords.get(4));
        assertRecord(5, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING, selectedRecords.get(5));
        assertRecord(6, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING, selectedRecords.get(6));
        assertRecord(7, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING, selectedRecords.get(7));
        assertRecord(8, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING, selectedRecords.get(8));
        assertRecord(9, "", selectedRecords.get(9));
        assertRecord(10, OverlappingRecordSelector.EXCLUSION_REASON_OVERLAPPING, selectedRecords.get(10));
    }

    private static void assertRecord(int srcIndex, String expectedReason, Record record) {
        assertEquals((float) srcIndex, record.getLocation().lat, 1.0e-6);
        String actualReason = (String) record.getAnnotationValues()[EXCLUSION_REASON_INDEX];
        assertEquals(expectedReason, actualReason);
    }

    private void mockPixelPosRecord(ProductRecordSource.PixelPosRecordFactory recordFactory, int recordIndex, PixelPos pixelPos,
                                    int timeDiffInMinutes) {
        DefaultRecord record = new DefaultRecord(0, new GeoPos(recordIndex, -1), new Date(), new Object[0]);
        records.add(record);
        OngoingStubbing<ProductRecordSource.PixelPosRecord> ongoingStubbing = Mockito.when(recordFactory.create(record));
        ongoingStubbing.thenReturn(new ProductRecordSource.PixelPosRecord(pixelPos, record, timeDiffInMinutes * 60 * 1000));
    }


}
