package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.*;
import org.junit.Test;

import java.text.ParseException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author MarcoZ
 * @author Norman
 * @see MatcherTest
 */
public class ExtractorTest {

    @Test(expected = NullPointerException.class)
    public void testExtractDoesNotAcceptNullProduct() throws Exception {
        new Extractor(null, new MAConfig());
    }

    @Test(expected = NullPointerException.class)
    public void testExtractDoesNotAcceptNullRecord() throws Exception {
        new Extractor(new Product("A", "B", 2, 2), new MAConfig()).extract(null);
    }

    @Test
    public void testThatRecordsAreGeneratedForContainedCoordinates() throws Exception {
        Extractor extractor = createExtractor(2, 3);
        extractor.getConfig().setCopyInput(false);
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(1, 0))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(1, 1))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(0, 0))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(0, 1))));

        // same test, but this time using the iterator
        extractor = createExtractor(2, 3);
        extractor.setInput(new DefaultRecordSource(new DefaultHeader("lat", "lon"),
                                                   new TestRecord(new GeoPos(1, 0)),
                                                   new TestRecord(new GeoPos(1, 1)),
                                                   new TestRecord(new GeoPos(0, 0)),
                                                   new TestRecord(new GeoPos(0, 1))));
        int n = 0;
        for (Record r : extractor.getRecords()) {
            assertNotNull(r);
            n++;
        }
        assertEquals(4, n);
    }

    @Test
    public void testThatRecordsAreNotGeneratedForOutlyingCoordinates() throws Exception {
        Extractor extractor = createExtractor(2, 3);
        assertNull(extractor.extract(new TestRecord(new GeoPos(-1, -1))));
        assertNull(extractor.extract(new TestRecord(new GeoPos(-1, 2))));
        assertNull(extractor.extract(new TestRecord(new GeoPos(-3, -1))));

        // same test, but this time using the iterator
        extractor = createExtractor(2, 3);
        extractor.setInput(new DefaultRecordSource(new DefaultHeader("lat", "lon"),
                                                   new TestRecord(new GeoPos(-1, -1)),
                                                   new TestRecord(new GeoPos(-1, 2)),
                                                   new TestRecord(new GeoPos(-3, -1))));
        int n = 0;
        for (Record r : extractor.getRecords()) {
            assertNotNull(r);
            n++;
        }
        assertEquals(0, n);
    }

    @Test
    public void testTheIteratorForSourceRecordsThatAreInAndOutOfProductBoundaries() throws Exception {
        Extractor extractor = createExtractor(2, 3);
        extractor.setInput(new DefaultRecordSource(new DefaultHeader("lat", "lon"),
                                                   new TestRecord(new GeoPos(1, 0)),// in
                                                   new TestRecord(new GeoPos(-1, 2)),   // out
                                                   new TestRecord(new GeoPos(1, 1)), // in
                                                   new TestRecord(new GeoPos(-1, -1)), // out
                                                   new TestRecord(new GeoPos(0, 0)), // in
                                                   new TestRecord(new GeoPos(0, 1)), // in
                                                   new TestRecord(new GeoPos(-3, -1)))); // out
        int n = 0;
        for (Record r : extractor.getRecords()) {
            assertNotNull(r);
            n++;
        }
        assertEquals(4, n);
    }

    @Test
    public void testThatPixelYXSortingWorks() throws Exception {
        DefaultRecordSource recordSource = new DefaultRecordSource(new DefaultHeader("lat", "lon"),
                                                                   new TestRecord(new GeoPos(0.0F, 0.0F)),
                                                                   new TestRecord(new GeoPos(0.0F, 1.0F)),
                                                                   new TestRecord(new GeoPos(1.0F, 0.0F)),
                                                                   new TestRecord(new GeoPos(0.5F, 0.5F)),
                                                                   new TestRecord(new GeoPos(1.0F, 1.0F)));

        final int PIXEL_Y = 2;

        Extractor noSort = createExtractor(2, 3);
        noSort.getConfig().setCopyInput(false);
        noSort.setSortInputByPixelYX(false);
        noSort.setInput(recordSource);
        List<Record> unsorted = getRecords(noSort);
        assertEquals(5, unsorted.size());

        assertEquals(2, ((int[]) unsorted.get(0).getAttributeValues()[PIXEL_Y])[0]);
        assertEquals(2, ((int[]) unsorted.get(1).getAttributeValues()[PIXEL_Y])[0]);
        assertEquals(0, ((int[]) unsorted.get(2).getAttributeValues()[PIXEL_Y])[0]);
        assertEquals(1, ((int[]) unsorted.get(3).getAttributeValues()[PIXEL_Y])[0]);
        assertEquals(0, ((int[]) unsorted.get(4).getAttributeValues()[PIXEL_Y])[0]);

        Extractor sort = createExtractor(2, 3);
        sort.getConfig().setCopyInput(false);
        sort.setSortInputByPixelYX(true);
        sort.setInput(recordSource);
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
        Extractor extractor = createExtractor(2, 3);
        extractor.getConfig().setCopyInput(false);
        Header header = extractor.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);

        int index = 0;

        // 0. derived information
        assertEquals("source_name", attributeNames[index++]);
        assertEquals("pixel_x", attributeNames[index++]);
        assertEquals("pixel_y", attributeNames[index++]);
        assertEquals("pixel_lat", attributeNames[index++]);
        assertEquals("pixel_lon", attributeNames[index++]);
        assertEquals("pixel_time", attributeNames[index++]);
        assertEquals("pixel_mask", attributeNames[index++]);
        // 1. bands
        assertEquals("b1", attributeNames[index++]);
        assertEquals("b2", attributeNames[index++]);
        assertEquals("b3", attributeNames[index++]);
        // 2. flags
        assertEquals("f.valid", attributeNames[index++]);
        // 3. tie-points
        assertEquals("latitude", attributeNames[index++]);
        assertEquals("longitude", attributeNames[index++]);

        assertEquals(13, index);
    }

    @Test
    public void testThatInputIsCopied() throws Exception {
        Extractor extractor = createExtractor(2, 3);
        extractor.getConfig().setCopyInput(true);
        extractor.setInput(new RecordSource() {
            @Override
            public Header getHeader() {
                return new DefaultHeader("u", "v", "w");
            }

            @Override
            public Iterable<Record> getRecords() throws Exception {
                return Arrays.asList((Record) newRecord(new GeoPos(0F, 1F), null, "?"));
            }
        });
        Header header = extractor.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);

        int index = 0;

        // 0. derived information
        assertEquals("u", attributeNames[index++]);
        assertEquals("v", attributeNames[index++]);
        assertEquals("w", attributeNames[index++]);
        assertEquals("source_name", attributeNames[index++]);
        assertEquals("pixel_x", attributeNames[index++]);
        assertEquals("pixel_y", attributeNames[index++]);
        assertEquals("pixel_lat", attributeNames[index++]);
        assertEquals("pixel_lon", attributeNames[index++]);
        assertEquals("pixel_time", attributeNames[index++]);
        assertEquals("pixel_mask", attributeNames[index++]);
        // 1. bands
        assertEquals("b1", attributeNames[index++]);
        assertEquals("b2", attributeNames[index++]);
        assertEquals("b3", attributeNames[index++]);
        // 2. flags
        assertEquals("f.valid", attributeNames[index++]);
        // 3. tie-points
        assertEquals("latitude", attributeNames[index++]);
        assertEquals("longitude", attributeNames[index++]);
        assertEquals(16, index);

        Iterable<Record> records = extractor.getRecords();
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
    public void testGetRecords() throws Exception {
        Extractor extractor = createExtractor(2, 3);

        DefaultRecordSource input = new DefaultRecordSource(new DefaultHeader("lat", "lon"));
        addPointRecord(input, 1F, 0F);
        addPointRecord(input, 1F, 1F);
        addPointRecord(input, -2F, 1F);
        addPointRecord(input, 0F, 0F);
        addPointRecord(input, 0F, 1F);
        addPointRecord(input, 0F, -1F);
        addPointRecord(input, 5F, 0F);

        extractor.setInput(input);

        Iterable<Record> records = extractor.getRecords();
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

    @Test(expected = IllegalStateException.class)
    public void testThatGetRecordsRequiresInput() throws Exception {
        Extractor extractor = createExtractor(2, 3);
        extractor.getRecords();
    }

    @Test
    public void testExtract() throws Exception {
        Extractor extractor = createExtractor(2, 3);
        extractor.getConfig().setCopyInput(false);
        Record extract = extractor.extract(new TestRecord(new GeoPos(1.0F, 0.0F)));
        assertNotNull(extract);

        assertNotNull(extractor.getHeader());
        assertNotNull(extractor.getHeader().getAttributeNames());

        GeoPos coordinate = extract.getLocation();
        assertEquals(1.0F, coordinate.lat, 0.0001F);
        assertEquals(0.0F, coordinate.lon, 0.0001F);

        Object[] values = extract.getAttributeValues();
        assertNotNull(values);
        assertEquals(extractor.getHeader().getAttributeNames().length, values.length);
        int index = 0;
        assertEquals("MER_RR__2P.N1", values[index++]); // product_name
        assertEquals(0, ((int[]) values[index++])[0]);  // pixel_x
        assertEquals(0, ((int[]) values[index++])[0]);  // pixel_y
        assertEquals(1.0F, ((float[]) values[index++])[0], 1e-5F);  // pixel_lat
        assertEquals(0.0F, ((float[]) values[index++])[0], 1e-5F);  // pixel_lon
        assertEquals("07-May-2010 10:25:14", extractor.getHeader().getTimeFormat().format((Date) values[index++])); // pixel_time
        assertEquals(0, ((int[]) values[index++])[0]);   //pixel_mask
        assertEquals(0.0F, ((int[]) values[index++])[0], 1e-5F);   // b1 = X-0.5
        assertEquals(0.0F, ((int[]) values[index++])[0], 1e-5F);   // b2 = Y-0.5
        assertEquals(0.5F, ((float[]) values[index++])[0], 1e-5F);   // b3 = 0.5 * (X+Y)
        assertEquals(1, ((int[]) values[index++])[0]);    // f.valid = true
        assertEquals(1.0F, ((float[]) values[index++])[0], 1e-5F);  // latitude
        assertEquals(0.0F, ((float[]) values[index])[0], 1e-5F);   // longitude
    }

    protected Extractor createExtractor(int w, int h) {
        Product product = createProduct(w, h);
        MAConfig config = new MAConfig(); // use defaults
        return createExtractor(product, config);
    }

    protected Extractor createExtractor(Product product, MAConfig config) {
        return new Extractor(product, config);
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

    protected ProductData.UTC utc(String date) {
        try {
            return ProductData.UTC.parse(date);
        } catch (ParseException e) {
            throw new IllegalArgumentException("date=" + date, e);
        }
    }

    protected Date date(String date) {
        return utc(date).getAsDate();
    }

    protected List<Record> getRecords(RecordSource source) throws Exception {
        Iterable<Record> records = source.getRecords();
        ArrayList<Record> list = new ArrayList<Record>();
        for (Record record : records) {
            assertNotNull("Unexpected null record detected.", record);
            list.add(record);
        }
        return list;
    }

    public static DefaultRecord newRecord(GeoPos coordinate, Date time, Object... values) {
        if (coordinate != null || time != null) {
            ArrayList<Object> list;
            if (coordinate != null && time != null) {
                list = new ArrayList<Object>(Arrays.asList((Object) coordinate.lat, coordinate.lon, time));
            } else if (coordinate != null) {
                list = new ArrayList<Object>(Arrays.asList((Object) coordinate.lat, coordinate.lon));
            } else {
                list = new ArrayList<Object>(Arrays.asList((Object) time));
            }
            list.addAll(Arrays.asList(values));
            return new DefaultRecord(coordinate, time, list.toArray(new Object[list.size()]));
        } else {
            return new DefaultRecord(values);
        }
    }

    public static void addPointRecord(DefaultRecordSource recordSource, float lat, float lon, Object... attributeValues) {
        recordSource.addRecord(newRecord(new GeoPos(lat, lon), null, attributeValues));
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
