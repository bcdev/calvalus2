package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.Product;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Norman
 * @see ExtractorTest
 */
public class MatcherTest extends ExtractorTest {

    @Test
    public void testGoodPixelExpressionCriterion() throws Exception {
        Extractor extractor = createExtractor();
        extractor.getConfig().setMaxTimeDifference(null);
        extractor.getConfig().setGoodPixelExpression("X==1");
        extractor.setInput(new DefaultRecordSource(new DefaultHeader("latitude", "longitude", "time"),
                                                   newRecord(new GeoPos(0, 0), null), // not ok
                                                   newRecord(new GeoPos(0, 1), null), // not ok
                                                   newRecord(new GeoPos(1, 0), null), // ok
                                                   newRecord(new GeoPos(1, 1), null))); // ok
        List<Record> records = getRecords(extractor);
        assertEquals(2, records.size());
    }

    @Test
    public void testTimeCriterion() throws Exception {

        // Note: product starts at  "07-MAY-2010 10:25:14"
        //               ends at    "07-MAY-2010 11:24:46"

        Extractor extractor = createExtractor();
        extractor.getConfig().setMaxTimeDifference(null);
        extractor.setInput(new DefaultRecordSource(new DefaultHeader("latitude", "longitude", "time"),
                                                   newRecord(new GeoPos(0, 0), date("07-MAY-2010 11:25:00")), // still ok
                                                   newRecord(new GeoPos(0, 1), date("07-MAY-2010 10:25:00")), // ok
                                                   newRecord(new GeoPos(1, 0), date("07-MAY-2010 10:59:00")), // ok
                                                   newRecord(new GeoPos(1, 1), date("07-MAY-2010 09:25:00")))); // still ok
        List<Record> records = getRecords(extractor);
        assertEquals(4, records.size());

        extractor = createExtractor();
        extractor.getConfig().setMaxTimeDifference(0.25);
        extractor.setInput(new DefaultRecordSource(new DefaultHeader("latitude", "longitude", "time"),
                                                   newRecord(new GeoPos(0.5F, 0.5F), date("07-MAY-2010 11:26:00")), // rejected
                                                   newRecord(new GeoPos(0.5F, 0.5F), date("07-JUN-2010 13:25:00")), // rejected
                                                   newRecord(new GeoPos(0.5F, 0.5F), date("07-MAY-2010 10:59:00")), // ok
                                                   newRecord(new GeoPos(0.5F, 0.5F), date("07-MAY-2010 08:10:00")))); // rejected
        records = getRecords(extractor);
        assertEquals(1, records.size());
    }



    @Override
    protected Matcher createExtractor(Product product, MAConfig config) {
        return new Matcher(product, config);
    }

}
