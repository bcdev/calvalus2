package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.Product;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Norman
 * @see ExtractorTest
 */
public class MatcherTest extends ExtractorTest {

    @Test
    public void testGoodPixelExpressionCriterion() throws Exception {
        Extractor extractor = createExtractor(2, 3);
        extractor.getConfig().setMaxTimeDifference(null);
        extractor.getConfig().setGoodPixelExpression("feq(X, 1.5)");
        extractor.setSortInputByPixelYX(true);
        extractor.setInput(new DefaultRecordSource(new DefaultHeader("latitude", "longitude", "time"),
                                                   newRecord(new GeoPos(0.0F, 0.0F), null),  // --> X=0.5,Y=2.5 --> reject
                                                   newRecord(new GeoPos(0.0F, 1.0F), null),  // --> X=1.5,Y=2.5 --> ok
                                                   newRecord(new GeoPos(0.5F, 0.0F), null),  // --> X=0.5,Y=1.5 --> reject
                                                   newRecord(new GeoPos(0.5F, 1.0F), null),  // --> X=1.5,Y=1.5 --> ok
                                                   newRecord(new GeoPos(1.0F, 0.0F), null),  // --> X=0.5,Y=0.5 --> reject
                                                   newRecord(new GeoPos(1.0F, 1.0F), null))  // --> X=0.5,Y=0.5 --> ok
        ); // ok
        List<Record> records = getRecords(extractor);
        assertEquals(3, records.size());
    }

    @Test
    public void testMacroPixelsAreExtracted() throws Exception {

        int w = 2 * 3;
        int h = 3 * 3;
        Extractor extractor = createExtractor(w, h);
        extractor.getConfig().setCopyInput(false);
        extractor.getConfig().setMacroPixelSize(3);
        extractor.getConfig().setAggregateMacroPixel(false);

        float lon = 0.0F + 1.0F / (w - 1.0F);
        float lat = 1.0F - 1.0F / (h - 1.0F);
        extractor.setInput(new DefaultRecordSource(new DefaultHeader("latitude", "longitude", "time"),
                                                   newRecord(new GeoPos(lat, lon), null)));  // --> center of first macro pixel
        List<Record> records = getRecords(extractor);
        assertEquals(1, records.size());

        final int PIXEL_X = 1;
        final int PIXEL_Y = 2;

        //  "pixel_x" : int[9]
        Object pixelXValue = records.get(0).getAttributeValues()[PIXEL_X];
        assertEquals(int[].class, pixelXValue.getClass());
        int[] actualX = (int[]) pixelXValue;
        assertEquals(9, actualX.length);

        //  "pixel_y" : int[9]
        Object pixelYValue = records.get(0).getAttributeValues()[PIXEL_Y];
        assertEquals(int[].class, pixelYValue.getClass());
        int[] actualY = (int[]) pixelYValue;
        assertEquals(9, actualY.length);

        assertArrayEquals(new int[]{0, 1, 2, 0, 1, 2, 0, 1, 2}, actualX);
        assertArrayEquals(new int[]{0, 0, 0, 1, 1, 1, 2, 2, 2}, actualY);

        List<Record> flattenedRecords = new RecordTransformer(-1, 1.5).expand(records.get(0));
        assertNotNull(flattenedRecords);
        assertEquals(9, flattenedRecords.size());

        actualX = new int[9];
        actualY = new int[9];
        for (int i = 0; i < 9; i++) {
            pixelXValue = flattenedRecords.get(i).getAttributeValues()[PIXEL_X];
            assertEquals(Integer.class, pixelXValue.getClass());
            actualX[i] = (Integer) pixelXValue;

            pixelYValue = flattenedRecords.get(i).getAttributeValues()[PIXEL_Y];
            assertEquals(Integer.class, pixelYValue.getClass());
            actualY[i] = (Integer) pixelYValue;
        }
        assertArrayEquals(new int[]{0, 1, 2, 0, 1, 2, 0, 1, 2}, actualX);
        assertArrayEquals(new int[]{0, 0, 0, 1, 1, 1, 2, 2, 2}, actualY);
    }

    @Test
    public void testTimeCriterion() throws Exception {

        // Note: product starts at  "07-MAY-2010 10:25:14"
        //               ends at    "07-MAY-2010 11:24:46"

        Extractor extractor = createExtractor(2, 3);
        extractor.getConfig().setMaxTimeDifference(null);
        extractor.setInput(new DefaultRecordSource(new DefaultHeader("latitude", "longitude", "time"),
                                                   newRecord(new GeoPos(0, 0), date("07-MAY-2010 11:25:00")), // still ok
                                                   newRecord(new GeoPos(0, 1), date("07-MAY-2010 10:25:00")), // ok
                                                   newRecord(new GeoPos(1, 0), date("07-MAY-2010 10:59:00")), // ok
                                                   newRecord(new GeoPos(1, 1), date("07-MAY-2010 09:25:00")))); // still ok
        List<Record> records = getRecords(extractor);
        assertEquals(4, records.size());

        extractor = createExtractor(2, 3);
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
