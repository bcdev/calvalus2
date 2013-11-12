package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.FlagCoding;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.datamodel.TiePointGeoCoding;
import org.esa.beam.framework.datamodel.TiePointGrid;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * @author MarcoZ
 * @author Norman
 */
public class ProductRecordSourceTest {

    @Test(expected = NullPointerException.class)
    public void testConstructorArg1CannotBeNull() throws Exception {
        new ProductRecordSource(null, createRecordSource(1), new MAConfig());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorArg2CannotBeNull() throws Exception {
        new ProductRecordSource(new Product("a", "b", 4, 4), null, new MAConfig());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorArg3CannotBeNull() throws Exception {
        new ProductRecordSource(new Product("a", "b", 4, 4), createRecordSource(1), null);
    }

    @Test
    public void testThatRecordsAreGeneratedForContainedCoordinates() throws Exception {
        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);

        RecordSource input = new DefaultRecordSource(new DefaultHeader(true, "lat", "lon"),
                                                     new TestRecord(new GeoPos(1, 0)),
                                                     new TestRecord(new GeoPos(1, 1)),
                                                     new TestRecord(new GeoPos(0, 0)),
                                                     new TestRecord(new GeoPos(0, 1)));

        ProductRecordSource output = createProductRecordSource(2, 3, input, config);
        int n = 0;
        for (Record r : output.getRecords()) {
            assertNotNull(r);
            n++;
        }
        assertEquals(4, n);
    }

    @Test
    public void testThatRecordsAreNotGeneratedForOutlyingCoordinates() throws Exception {
        // same test, but this time using the iterator
        DefaultRecordSource input = new DefaultRecordSource(new DefaultHeader(true, "lat", "lon"),
                                                            new TestRecord(new GeoPos(-1, -1)),
                                                            new TestRecord(new GeoPos(-1, 2)),
                                                            new TestRecord(new GeoPos(-3, -1)));
        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);
        ProductRecordSource output = createProductRecordSource(2, 3, input, config);
        int n = 0;
        for (Record r : output.getRecords()) {
            assertNotNull(r);
            n++;
        }
        assertEquals(0, n);
    }

    @Test
    public void testTheIteratorForSourceRecordsThatAreInAndOutOfProductBoundaries() throws Exception {
        DefaultRecordSource input = new DefaultRecordSource(new DefaultHeader(true, "lat", "lon"),
                                                            new TestRecord(new GeoPos(1, 0)),// in
                                                            new TestRecord(new GeoPos(-1, 2)),   // out
                                                            new TestRecord(new GeoPos(1, 1)), // in
                                                            new TestRecord(new GeoPos(-1, -1)), // out
                                                            new TestRecord(new GeoPos(0, 0)), // in
                                                            new TestRecord(new GeoPos(0, 1)), // in
                                                            new TestRecord(new GeoPos(-3, -1))); // out
        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);
        ProductRecordSource output = createProductRecordSource(2, 3, input, config);
        int n = 0;
        for (Record r : output.getRecords()) {
            assertNotNull(r);
            n++;
        }
        assertEquals(4, n);
    }

    @Test
    public void testThatInputSortingWorks() throws Exception {
        DefaultRecordSource input = new DefaultRecordSource(new DefaultHeader(true, "lat", "lon"),
                                                            new TestRecord(new GeoPos(0.0F, 0.0F)),
                                                            new TestRecord(new GeoPos(0.0F, 1.0F)),
                                                            new TestRecord(new GeoPos(1.0F, 0.0F)),
                                                            new TestRecord(new GeoPos(0.5F, 0.5F)),
                                                            new TestRecord(new GeoPos(1.0F, 1.0F)));

        final int PIXEL_Y = 3;

        MAConfig config1 = new MAConfig();
        config1.setMacroPixelSize(1);
        config1.setCopyInput(false);

        ProductRecordSource sort = createProductRecordSource(2, 3, input, config1);
        List<Record> sorted = getRecords(sort);
        assertEquals(5, sorted.size());

        assertEquals(0, ((int[]) sorted.get(0).getAttributeValues()[PIXEL_Y])[0]);
        assertEquals(0, ((int[]) sorted.get(1).getAttributeValues()[PIXEL_Y])[0]);
        assertEquals(1, ((int[]) sorted.get(2).getAttributeValues()[PIXEL_Y])[0]);
        assertEquals(2, ((int[]) sorted.get(3).getAttributeValues()[PIXEL_Y])[0]);
        assertEquals(2, ((int[]) sorted.get(4).getAttributeValues()[PIXEL_Y])[0]);
    }

    @Test
    public void testThatHeaderIsCorrect() throws Exception {
        MAConfig config = new MAConfig();
        config.setCopyInput(false);
        config.setMacroPixelSize(1);

        ProductRecordSource output = createProductRecordSource(2, 3, createRecordSource(1), config);
        Header header = output.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);

        int index = 0;

        // 0. derived information
        assertEquals("source_name", attributeNames[index++]);
        assertEquals("pixel_time", attributeNames[index++]);
        assertEquals("*pixel_x", attributeNames[index++]);
        assertEquals("*pixel_y", attributeNames[index++]);
        assertEquals("*pixel_lat", attributeNames[index++]);
        assertEquals("*pixel_lon", attributeNames[index++]);
        // 1. bands
        assertEquals("*b1", attributeNames[index++]);
        assertEquals("*b2", attributeNames[index++]);
        assertEquals("*b3", attributeNames[index++]);
        // 2. flags
        assertEquals("*f.valid", attributeNames[index++]);
        // 3. tie-points
        assertEquals("*latitude", attributeNames[index++]);
        assertEquals("*longitude", attributeNames[index++]);

        assertEquals(12, index);
    }

    @Test
    public void testThatInputIsCopied() throws Exception {
        RecordSource input = new RecordSource() {
            @Override
            public Header getHeader() {
                return new DefaultHeader(true, "u", "v", "w");
            }

            @Override
            public Iterable<Record> getRecords() throws Exception {
                return Arrays.asList((Record) RecordUtils.newRecord(new GeoPos(0F, 1F), null, "?"));
            }

            @Override
            public String getTimeAndLocationColumnDescription() {
                return null;
            }
        };

        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);
        config.setCopyInput(true);
        config.setGoodPixelExpression("b1 > 0");

        ProductRecordSource output = createProductRecordSource(2, 3, input, config);
        Header header = output.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);

        int index = 0;

        // 0. derived information
        assertEquals("u", attributeNames[index++]);
        assertEquals("v", attributeNames[index++]);
        assertEquals("w", attributeNames[index++]);
        assertEquals("source_name", attributeNames[index++]);
        assertEquals("pixel_time", attributeNames[index++]);
        assertEquals("*pixel_x", attributeNames[index++]);
        assertEquals("*pixel_y", attributeNames[index++]);
        assertEquals("*pixel_lat", attributeNames[index++]);
        assertEquals("*pixel_lon", attributeNames[index++]);
        assertEquals("*pixel_mask", attributeNames[index++]);
        // 1. bands
        assertEquals("*b1", attributeNames[index++]);
        assertEquals("*b2", attributeNames[index++]);
        assertEquals("*b3", attributeNames[index++]);
        // 2. flags
        assertEquals("*f.valid", attributeNames[index++]);
        // 3. tie-points
        assertEquals("*latitude", attributeNames[index++]);
        assertEquals("*longitude", attributeNames[index++]);
        assertEquals(16, index);

        Iterable<Record> records = output.getRecords();
        Record next = records.iterator().next();
        assertNotNull(next);
        Object[] attributeValues = next.getAttributeValues();
        assertNotNull(attributeValues);
        assertEquals(16, attributeValues.length);
        assertEquals(0.0F, (Float) attributeValues[0], 1E-5F);
        assertEquals(1.0F, (Float) attributeValues[1], 1E-5F);
        assertEquals("?", attributeValues[2]);
    }

    @Test
    public void testThatGetRecordsFulfillsIterableContract() throws Exception {
        DefaultRecordSource input = new DefaultRecordSource(new DefaultHeader(true, "lat", "lon"));
        RecordUtils.addPointRecord(input, 1F, 0F);
        RecordUtils.addPointRecord(input, 1F, 1F);
        RecordUtils.addPointRecord(input, -2F, 1F);
        RecordUtils.addPointRecord(input, 0F, 0F);
        RecordUtils.addPointRecord(input, 0F, 1F);
        RecordUtils.addPointRecord(input, 0F, -1F);
        RecordUtils.addPointRecord(input, 5F, 0F);

        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);
        ProductRecordSource output = createProductRecordSource(2, 3, input, config);


        Iterable<Record> records = output.getRecords();
        assertNotNull(records);

        Iterator<Record> iterator = records.iterator();
        // 1
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        // 2
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        // 3
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        // 4
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        // end
        assertFalse(iterator.hasNext());
        // Check that it is still false
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());

        try {
            iterator.next();
            fail("NSEE expected");
        } catch (NoSuchElementException e) {
            // ok
        }

    }

    @Test
    public void testExtract() throws Exception {
        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);
        config.setCopyInput(false);
        config.setGoodPixelExpression("b1 == 0");

        ProductRecordSource output = createProductRecordSource(2, 3, new DefaultRecordSource(new DefaultHeader(true, "lat", "lon"),
                                                                                             new TestRecord(new GeoPos(1.0F, 0.0F))), config);

        Iterable<Record> records = output.getRecords();
        Iterator<Record> iterator = records.iterator();
        assertTrue(iterator.hasNext());

        Record extract = iterator.next();
        assertNotNull(extract);

        assertNotNull(output.getHeader());
        assertNotNull(output.getHeader().getAttributeNames());

        GeoPos coordinate = extract.getLocation();
        assertEquals(1.0F, coordinate.lat, 0.0001F);
        assertEquals(0.0F, coordinate.lon, 0.0001F);

        Object[] values = extract.getAttributeValues();
        assertNotNull(values);
        assertEquals(output.getHeader().getAttributeNames().length, values.length);
        int index = 0;
        assertEquals("MER_RR__2P.N1", values[index++]); // product_name
        assertEquals("07-May-2010 10:25:14", ProductData.UTC.createDateFormat().format((Date) values[index++])); // pixel_time
        assertEquals(0, ((int[]) values[index++])[0]);  // pixel_x
        assertEquals(0, ((int[]) values[index++])[0]);  // pixel_y
        assertEquals(1.0F, ((float[]) values[index++])[0], 1e-5F);  // pixel_lat
        assertEquals(0.0F, ((float[]) values[index++])[0], 1e-5F);  // pixel_lon
        assertEquals(1, ((int[]) values[index++])[0]);   // pixel_mask
        assertEquals(0.0F, ((int[]) values[index++])[0], 1e-5F);   // b1 = X-0.5
        assertEquals(0.0F, ((int[]) values[index++])[0], 1e-5F);   // b2 = Y-0.5
        assertEquals(0.5F, ((float[]) values[index++])[0], 1e-5F);   // b3 = 0.5 * (X+Y)
        assertEquals(1, ((int[]) values[index++])[0]);    // f.valid = true
        assertEquals(1.0F, ((float[]) values[index++])[0], 1e-5F);  // latitude
        assertEquals(0.0F, ((float[]) values[index])[0], 1e-5F);   // longitude
    }


    @Test
    public void testGoodPixelExpressionCriterion() throws Exception {
        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);
        config.setMaxTimeDifference(null);
        config.setGoodPixelExpression("feq(X, 1.5)");

        RecordSource input = new DefaultRecordSource(new DefaultHeader(true, "latitude", "longitude"),
                                                     RecordUtils.newRecord(new GeoPos(0.0F, 0.0F), null),  // --> X=0.5,Y=2.5 --> reject
                                                     RecordUtils.newRecord(new GeoPos(0.0F, 1.0F), null),  // --> X=1.5,Y=2.5 --> ok
                                                     RecordUtils.newRecord(new GeoPos(0.5F, 0.0F), null),  // --> X=0.5,Y=1.5 --> reject
                                                     RecordUtils.newRecord(new GeoPos(0.5F, 1.0F), null),  // --> X=1.5,Y=1.5 --> ok
                                                     RecordUtils.newRecord(new GeoPos(1.0F, 0.0F), null),  // --> X=0.5,Y=0.5 --> reject
                                                     RecordUtils.newRecord(new GeoPos(1.0F, 1.0F), null));  // --> X=0.5,Y=0.5 --> ok
        ProductRecordSource output = createProductRecordSource(2, 3, input, config);
        List<Record> records = getRecords(output);
        assertEquals(3, records.size());
    }

    @Test
    public void test3x3MacroPixelGeneratesFloatArraysWith9Elements() throws Exception {
        MAConfig config = new MAConfig();
        config.setCopyInput(false);
        config.setMacroPixelSize(3);

        int w = 2 * 3;
        int h = 3 * 3;

        // This (lat,lon) corresponds to the center of the upper left pixel of the macro pixel
        float lon = 0.0F + 1.0F / (w - 1.0F);
        float lat = 1.0F - 1.0F / (h - 1.0F);

        RecordSource input = new DefaultRecordSource(new DefaultHeader(true, "latitude", "longitude"),
                                                     RecordUtils.newRecord(new GeoPos(lat, lon), null));  // --> center of first macro pixel

        ProductRecordSource output = createProductRecordSource(w, h, input, config);

        List<Record> records = getRecords(output);
        assertEquals(1, records.size());

        final int PIXEL_X = 2;
        final int PIXEL_Y = 3;

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
    }

    @Test
    public void testTimeCriterion() throws Exception {

        // Note: test product starts at  "07-MAY-2010 10:25:14"
        //                    ends at    "07-MAY-2010 11:24:46"


        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);
        config.setMaxTimeDifference(null);

        RecordSource input = new DefaultRecordSource(new DefaultHeader(true, true, "latitude", "longitude", "time"),
                                                     RecordUtils.newRecord(new GeoPos(0, 0), date("07-MAY-2010 11:25:00")), // still ok
                                                     RecordUtils.newRecord(new GeoPos(0, 1), date("07-MAY-2010 10:25:00")), // ok
                                                     RecordUtils.newRecord(new GeoPos(1, 0), date("07-MAY-2010 10:59:00")), // ok
                                                     RecordUtils.newRecord(new GeoPos(1, 1), date("07-MAY-2010 09:25:00"))); // still ok

        ProductRecordSource output = createProductRecordSource(2, 3, input, config);
        List<Record> records = getRecords(output);
        assertEquals(4, records.size());

        config = new MAConfig();
        config.setMacroPixelSize(1);
        config.setMaxTimeDifference(0.25);

        input = new DefaultRecordSource(new DefaultHeader(true, true, "latitude", "longitude", "time"),
                                        RecordUtils.newRecord(new GeoPos(0.5F, 0.5F), date("07-MAY-2010 11:26:00")), // rejected
                                        RecordUtils.newRecord(new GeoPos(0.5F, 0.5F), date("07-JUN-2010 13:25:00")), // rejected
                                        RecordUtils.newRecord(new GeoPos(0.5F, 0.5F), date("07-MAY-2010 10:59:00")), // ok
                                        RecordUtils.newRecord(new GeoPos(0.5F, 0.5F), date("07-MAY-2010 08:10:00"))); // rejected
        output = createProductRecordSource(2, 3, input, config);
        records = getRecords(output);
        assertEquals(1, records.size());
    }

    @Test
    public void testYXComparator() throws Exception {
        ProductRecordSource.YXComparator comparator = new ProductRecordSource.YXComparator();
        List<ProductRecordSource.PixelPosRecord> list = new ArrayList<ProductRecordSource.PixelPosRecord>();
        list.add(new ProductRecordSource.PixelPosRecord(new PixelPos(10, 0), null));
        list.add(new ProductRecordSource.PixelPosRecord(new PixelPos(12, 0), null));
        list.add(new ProductRecordSource.PixelPosRecord(new PixelPos(1, 6), null));
        list.add(new ProductRecordSource.PixelPosRecord(new PixelPos(16, 3), null));
        list.add(new ProductRecordSource.PixelPosRecord(new PixelPos(1, 3), null));
        list.add(new ProductRecordSource.PixelPosRecord(new PixelPos(4, 0), null));
        list.add(new ProductRecordSource.PixelPosRecord(new PixelPos(130, 23.498f), null));
        list.add(new ProductRecordSource.PixelPosRecord(new PixelPos(125, 23.501f), null));

        Collections.sort(list, comparator);
        assertEquals(new PixelPos(4, 0), list.get(0).pixelPos);
        assertEquals(new PixelPos(10, 0), list.get(1).pixelPos);
        assertEquals(new PixelPos(12, 0), list.get(2).pixelPos);
        assertEquals(new PixelPos(1, 3), list.get(3).pixelPos);
        assertEquals(new PixelPos(16, 3), list.get(4).pixelPos);
        assertEquals(new PixelPos(1, 6), list.get(5).pixelPos);
        assertEquals(new PixelPos(125, 23.501f), list.get(6).pixelPos);
        assertEquals(new PixelPos(130, 23.498f), list.get(7).pixelPos);

    }

    private ProductRecordSource createProductRecordSource(int w, int h, RecordSource input, MAConfig config) {
        Product product = createProduct(w, h);
        return createProductRecordSource(product, input, config);
    }

    private ProductRecordSource createProductRecordSource(Product product, RecordSource input, MAConfig config) {
        return new ProductRecordSource(product, input, config);
    }

    private Product createProduct(int w, int h) {
        Product product = new Product("MER_RR__2P.N1", "MER_RR__2P", w, h);
        product.addTiePointGrid(new TiePointGrid("latitude", 2, 2, 0.5f, 0.5f, w - 1, h - 1, new float[]{1, 1, 0, 0}));
        product.addTiePointGrid(new TiePointGrid("longitude", 2, 2, 0.5f, 0.5f, w - 1, h - 1, new float[]{0, 1, 0, 1}));
        product.setGeoCoding(new TiePointGeoCoding(product.getTiePointGrid("latitude"), product.getTiePointGrid("longitude")));
        product.setStartTime(utc("07-MAY-2010 10:25:14"));
        product.setEndTime(utc("07-MAY-2010 11:24:46"));
        product.addBand("b1", "X-0.5", ProductData.TYPE_INT16);
        product.addBand("b2", "Y-0.5", ProductData.TYPE_INT16);
        product.addBand("b3", "0.5 * (X + Y)");
        Band f = product.addBand("f", ProductData.TYPE_UINT8);
        ProductData fData = f.createCompatibleRasterData();
        for (int i = 0; i < w * h; i++) {
            fData.setElemBooleanAt(i, i % 2 == 0);
        }
        f.setRasterData(fData);


        // Initialise flag coding
        FlagCoding flagCoding = new FlagCoding("f");
        flagCoding.addFlag("valid", 1, "Pixel is valid");
        f.setSampleCoding(flagCoding);

        return product;
    }

    private ProductData.UTC utc(String date) {
        try {
            return ProductData.UTC.parse(date);
        } catch (ParseException e) {
            throw new IllegalArgumentException("date=" + date, e);
        }
    }

    private Date date(String date) {
        return utc(date).getAsDate();
    }

    private List<Record> getRecords(RecordSource source) throws Exception {
        Iterable<Record> records = source.getRecords();
        ArrayList<Record> list = new ArrayList<Record>();
        for (Record record : records) {
            assertNotNull("Unexpected null record detected.", record);
            list.add(record);
        }
        return list;
    }

    private DefaultRecordSource createRecordSource(int n) {
        Record[] records = new Record[n];
        for (int i = 0; i < records.length; i++) {
            records[i] = new DefaultRecord(new GeoPos(i, i), 1000L + i);
        }
        return new DefaultRecordSource(new DefaultHeader(true, "lat", "lon"), records);
    }

    protected static class TestRecord implements Record {

        GeoPos coordinate;

        private TestRecord(GeoPos coordinate) {
            this.coordinate = new GeoPos(coordinate);
        }

        @Override
        public Object[] getAttributeValues() {
            return new Object[]{
                    coordinate.lat,
                    coordinate.lon,
            };
        }

        @Override
        public GeoPos getLocation() {
            return coordinate;
        }

        @Override
        public Date getTime() {
            return null;
        }

    }

}
