package com.bc.calvalus.processing.ma;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Norman Fomferra
 */
public class ExpressionRecordFilterTest {
    @Test
    public void testExprFilterWithAggregatedNumbers() throws Exception {
        Header header = new DefaultHeader("chl");
        RecordFilter filter = ExpressionRecordFilter.create(header, "chl.mean > 2.0");

        assertEquals(true, filter.accept(new DefaultRecord(new AggregatedNumber(25, 0.0, 3.0, 2.6, 0.4, 24, 2.4, 0.2, 16))));
        assertEquals(false, filter.accept(new DefaultRecord(new AggregatedNumber(25, 0.0, 3.0, 1.7, 0.3, 16, 2.5, 0.1, 12))));
    }

    @Test
    public void testExprFilterWithPrimitiveTypes() throws Exception {
        Header header = new DefaultHeader("valid", "chl", "tsm");
        RecordFilter filter = ExpressionRecordFilter.create(header, "valid && chl < 0.5 && tsm < 0.2");

        assertEquals(false, filter.accept(new DefaultRecord(0, 0.4F, 0.1F)));
        assertEquals(true, filter.accept(new DefaultRecord(1, 0.4F, 0.1F)));
        assertEquals(false, filter.accept(new DefaultRecord(0, 0.6F, 0.1F)));
        assertEquals(false, filter.accept(new DefaultRecord(1, 0.6F, 0.1F)));
        assertEquals(false, filter.accept(new DefaultRecord(0, 0.4F, 0.3F)));
        assertEquals(false, filter.accept(new DefaultRecord(1, 0.4F, 0.3F)));
    }

}
