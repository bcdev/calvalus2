package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import org.esa.beam.framework.datamodel.ProductData;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by thomas on 19.11.15.
 */
public class FireL3ProductionTypeTest {

    @Test
    public void testGetDateRanges() throws Exception {
        DateRange dateRange = new DateRange(getAsDate("2008-06-01"), getAsDate("2008-06-11"));
        List<DateRange> dateRanges = FireL3ProductionType.getIndividualDays(dateRange);
        assertEquals(11, dateRanges.size());
        for (DateRange currentDateRange : dateRanges) {
            assertEquals(currentDateRange.getStartDate().getTime(), currentDateRange.getStopDate().getTime());
        }
        assertEquals(dateRanges.get(0).getStartDate().getTime(), getAsDate("2008-06-01").getTime());
        assertEquals(dateRanges.get(10).getStopDate().getTime(), getAsDate("2008-06-11").getTime());
    }

    private static Date getAsDate(String date) throws ParseException {
        return ProductData.UTC.parse(date, "yyyy-MM-dd").getAsDate();
    }
}