package org.esa.beam.gpf.operators.matchup;

import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;

import static org.junit.Assert.*;


public class MatchupOpTest {

    @Test
    public void testInitialisation() throws ParseException {
        MatchupOp op = new MatchupOp();
        Product sourceProduct = new Product("name", "type", 2, 2);
        op.setSourceProduct(sourceProduct);
        op.setParameter("startTime", date("2001"));
        op.setParameter("endTime", date("2010"));
        op.getTargetProduct();
    }

    private static ProductData.UTC date(String date) throws ParseException {
        return ProductData.UTC.parse(date, "yyyy");
    }

    @Test
    public void testDateRange_noDateGiven() throws ParseException {
        Product product = new Product("name", "type", 2, 2);

        assertTrue(MatchupOp.isProductInTimeRange(product, null, null));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2005"), null));
        assertFalse(MatchupOp.isProductInTimeRange(product, null, date("2010")));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2005"), date("2010")));
    }

    @Test
    public void testDateRange_startOnly() throws ParseException {
        Product product = new Product("name", "type", 2, 2);
        product.setStartTime(date("2005"));

        assertTrue(MatchupOp.isProductInTimeRange(product, null, null));

        assertTrue(MatchupOp.isProductInTimeRange(product, date("2004"), null));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2006"), null));

        assertFalse(MatchupOp.isProductInTimeRange(product, null, date("2004")));
        assertTrue(MatchupOp.isProductInTimeRange(product, null, date("2010")));

        assertFalse(MatchupOp.isProductInTimeRange(product, date("2006"), date("2010")));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2000"), date("2004")));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2000"), date("2010")));
    }

    @Test
    public void testDateRange_endOnly() throws ParseException {
        Product product = new Product("name", "type", 2, 2);
        product.setEndTime(date("2005"));

        assertTrue(MatchupOp.isProductInTimeRange(product, null, null));

        assertTrue(MatchupOp.isProductInTimeRange(product, date("2004"), null));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2006"), null));

        assertFalse(MatchupOp.isProductInTimeRange(product, null, date("2004")));
        assertTrue(MatchupOp.isProductInTimeRange(product, null, date("2010")));

        assertFalse(MatchupOp.isProductInTimeRange(product, date("2006"), date("2010")));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2000"), date("2004")));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2000"), date("2010")));
    }

    @Test
    public void testDateRange_bothDates() throws ParseException {
        Product product = new Product("name", "type", 2, 2);
        product.setStartTime(date("2003"));
        product.setEndTime(date("2005"));

        assertTrue(MatchupOp.isProductInTimeRange(product, null, null));

        assertTrue(MatchupOp.isProductInTimeRange(product, date("2002"), null));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2004"), null));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2006"), null));

        assertFalse(MatchupOp.isProductInTimeRange(product, null, date("2002")));
        assertTrue(MatchupOp.isProductInTimeRange(product, null, date("2004")));
        assertTrue(MatchupOp.isProductInTimeRange(product, null, date("2010")));

        assertFalse(MatchupOp.isProductInTimeRange(product, date("2000"), date("2002")));
        assertFalse(MatchupOp.isProductInTimeRange(product, date("2006"), date("2010")));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2000"), date("2004")));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2004"), date("2007")));
        assertTrue(MatchupOp.isProductInTimeRange(product, date("2000"), date("2010")));

    }


//    @Test
    public void testfindReferences() throws ParseException {

    }
}
