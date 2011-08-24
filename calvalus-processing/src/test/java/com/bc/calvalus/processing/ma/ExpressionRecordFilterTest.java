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

        assertEquals(true, filter.accept(new DefaultRecord(new AggregatedNumber(25, 24, 16, 2.6f, 0.4f, 2.4f, 0.2f))));
        assertEquals(false, filter.accept(new DefaultRecord(new AggregatedNumber(25, 16, 12, 1.7f, 0.3f, 2.5f, 0.1f))));
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
