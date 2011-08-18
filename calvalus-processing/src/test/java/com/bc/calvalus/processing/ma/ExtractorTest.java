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

    public static DefaultRecord merge(GeoPos coordinate, Date time, Object... values) {
        if (coordinate != null || time != null) {
            ArrayList<Object> list;
            if (coordinate != null && time != null) {
                // todo - use time format pattern from header
                String timeStr = ProductData.UTC.create(time, 0L).format();
                list = new ArrayList<Object>(Arrays.asList((Object) coordinate.lat, coordinate.lon, timeStr));
            } else if (coordinate != null) {
                list = new ArrayList<Object>(Arrays.asList((Object) coordinate.lat, coordinate.lon));
            } else {
                list = new ArrayList<Object>(Arrays.asList((Object) time));
            }
            list.addAll(Arrays.asList(values));
            return new DefaultRecord(coordinate, time, list.toArray(new Object[list.size()]));
        } else {
            return new DefaultRecord(coordinate, time, values.clone());
        }
    }

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
        Extractor extractor = createExtractor();
        extractor.getConfig().setCopyInput(false);
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(1, 0))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(1, 1))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(0, 0))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(0, 1))));

        // same test, but this time using the iterator
        extractor = createExtractor();
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
        Extractor extractor = createExtractor();
        assertNull(extractor.extract(new TestRecord(new GeoPos(-1, -1))));
        assertNull(extractor.extract(new TestRecord(new GeoPos(-1, 2))));
        assertNull(extractor.extract(new TestRecord(new GeoPos(-3, -1))));

        // same test, but this time using the iterator
        extractor = createExtractor();
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
        Extractor extractor = createExtractor();
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
                                                                   new TestRecord(new GeoPos(0, 0)),
                                                                   new TestRecord(new GeoPos(0, 1)),
                                                                   new TestRecord(new GeoPos(1, 0)),
                                                                   new TestRecord(new GeoPos(0.5F, 0.5F)),
                                                                   new TestRecord(new GeoPos(1, 1)));

        Extractor noSort = createExtractor();
        noSort.getConfig().setCopyInput(false);
        noSort.setSortInputByPixelYX(false);
        noSort.setInput(recordSource);
        List<Record> unsorted = getRecords(noSort);
        assertEquals(5, unsorted.size());
        assertEquals(0.5F, (Float) unsorted.get(0).getAttributeValues()[1], 1e-3F);
        assertEquals(1.5F, (Float) unsorted.get(0).getAttributeValues()[2], 1e-3F);
        assertEquals(1.5F, (Float) unsorted.get(1).getAttributeValues()[1], 1e-3F);
        assertEquals(1.5F, (Float) unsorted.get(1).getAttributeValues()[2], 1e-3F);
        assertEquals(0.5F, (Float) unsorted.get(2).getAttributeValues()[1], 1e-3F);
        assertEquals(0.5F, (Float) unsorted.get(2).getAttributeValues()[2], 1e-3F);
        assertEquals(1.0F, (Float) unsorted.get(3).getAttributeValues()[1], 1e-3F);
        assertEquals(1.0F, (Float) unsorted.get(3).getAttributeValues()[2], 1e-3F);
        assertEquals(1.5F, (Float) unsorted.get(4).getAttributeValues()[1], 1e-3F);
        assertEquals(0.5F, (Float) unsorted.get(4).getAttributeValues()[2], 1e-3F);

        Extractor sort = createExtractor();
        sort.getConfig().setCopyInput(false);
        sort.setSortInputByPixelYX(true);
        sort.setInput(recordSource);
        List<Record> sorted = getRecords(sort);
        assertEquals(5, sorted.size());
        assertEquals(0.5F, (Float) sorted.get(0).getAttributeValues()[1], 1e-3F);
        assertEquals(0.5F, (Float) sorted.get(0).getAttributeValues()[2], 1e-3F);
        assertEquals(1.5F, (Float) sorted.get(1).getAttributeValues()[1], 1e-3F);
        assertEquals(0.5F, (Float) sorted.get(1).getAttributeValues()[2], 1e-3F);
        assertEquals(1.0F, (Float) sorted.get(2).getAttributeValues()[1], 1e-3F);
        assertEquals(1.0F, (Float) sorted.get(2).getAttributeValues()[2], 1e-3F);
        assertEquals(0.5F, (Float) sorted.get(3).getAttributeValues()[1], 1e-3F);
        assertEquals(1.5F, (Float) sorted.get(3).getAttributeValues()[2], 1e-3F);
        assertEquals(1.5F, (Float) sorted.get(4).getAttributeValues()[1], 1e-3F);
        assertEquals(1.5F, (Float) sorted.get(4).getAttributeValues()[2], 1e-3F);

    }

    @Test
    public void testThatHeaderIsCorrect() throws Exception {
        Extractor extractor = createExtractor();
        extractor.getConfig().setCopyInput(false);
        Header header = extractor.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);

        int index = 0;

        // 0. derived information
        assertEquals("product_name", attributeNames[index++]);
        assertEquals("pixel_x", attributeNames[index++]);
        assertEquals("pixel_y", attributeNames[index++]);
        assertEquals("pixel_lat", attributeNames[index++]);
        assertEquals("pixel_lon", attributeNames[index++]);
        assertEquals("pixel_time", attributeNames[index++]);
        // 1. bands
        assertEquals("b1", attributeNames[index++]);
        assertEquals("b2", attributeNames[index++]);
        assertEquals("b3", attributeNames[index++]);
        // 2. flags
        assertEquals("f.valid", attributeNames[index++]);
        // 3. tie-points
        assertEquals("latitude", attributeNames[index++]);
        assertEquals("longitude", attributeNames[index++]);

        assertEquals(12, index);
    }

    @Test
    public void testThatInputIsCopied() throws Exception {
        Extractor extractor = createExtractor();
        extractor.getConfig().setCopyInput(true);
        extractor.setInput(new RecordSource() {
            @Override
            public Header getHeader() {
                return new DefaultHeader("u", "v", "w");
            }

            @Override
            public Iterable<Record> getRecords() throws Exception {
                return Arrays.asList((Record) merge(new GeoPos(0F, 1F), null, "?"));
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
        assertEquals("product_name", attributeNames[index++]);
        assertEquals("pixel_x", attributeNames[index++]);
        assertEquals("pixel_y", attributeNames[index++]);
        assertEquals("pixel_lat", attributeNames[index++]);
        assertEquals("pixel_lon", attributeNames[index++]);
        assertEquals("pixel_time", attributeNames[index++]);
        // 1. bands
        assertEquals("b1", attributeNames[index++]);
        assertEquals("b2", attributeNames[index++]);
        assertEquals("b3", attributeNames[index++]);
        // 2. flags
        assertEquals("f.valid", attributeNames[index++]);
        // 3. tie-points
        assertEquals("latitude", attributeNames[index++]);
        assertEquals("longitude", attributeNames[index++]);
        assertEquals(15, index);

        Iterable<Record> records = extractor.getRecords();
        Record next = records.iterator().next();
        assertNotNull(next);
        Object[] attributeValues = next.getAttributeValues();
        assertNotNull(attributeValues);
        assertEquals(15, attributeValues.length);
        assertEquals(0.0F, (Float) attributeValues[0], 1E-5F);
        assertEquals(1.0F, (Float) attributeValues[1], 1E-5F);
        assertEquals("?", attributeValues[2]);
    }

    @Test
    public void testGetRecords() throws Exception {
        Extractor extractor = createExtractor();

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
        Extractor extractor = createExtractor();
        extractor.getRecords();
    }

    @Test
    public void testExtract() throws Exception {
        Extractor extractor = createExtractor();
        extractor.getConfig().setCopyInput(false);
        Record extract = extractor.extract(new TestRecord(new GeoPos(1, 0)));
        assertNotNull(extract);

        assertNotNull(extractor.getHeader());
        assertNotNull(extractor.getHeader().getAttributeNames());

        GeoPos coordinate = extract.getCoordinate();
        assertEquals(1.0F, coordinate.lat, 0.0001F);
        assertEquals(0.0F, coordinate.lon, 0.0001F);

        Object[] values = extract.getAttributeValues();
        assertNotNull(values);
        assertEquals(extractor.getHeader().getAttributeNames().length, values.length);
        int index = 0;
        assertEquals("MER_RR__2P.N1", values[index++]); // product_name
        assertEquals(0.5F, (Float) values[index++], 1e-5F);  // pixel_x
        assertEquals(0.5F, (Float) values[index++], 1e-5F);  // pixel_y
        assertEquals(1.0F, (Float) values[index++], 1e-5F);  // pixel_lat
        assertEquals(0.0F, (Float) values[index++], 1e-5F);  // pixel_lon
        assertEquals("07-May-2010 10:25:14", values[index++]); // pixel_time
        assertEquals(0.5F, (Float) values[index++], 1e-5F);   // b1 = X
        assertEquals(0.5F, (Float) values[index++], 1e-5F);   // b2 = Y
        assertEquals(1.0F, (Float) values[index++], 1e-5F);   // b3 = X+Y
        assertEquals(1, values[index++]);    // f.valid = true
        assertEquals(1.0F, (Float) values[index++], 1e-5F);  // latitude
        assertEquals(0.0F, (Float) values[index], 1e-5F);   // longitude
    }

    protected Extractor createExtractor() {
        Product product = createProduct();
        MAConfig config = new MAConfig(); // use defaults
        return createExtractor(product, config);
    }

    protected Extractor createExtractor(Product product, MAConfig config) {
        return new Extractor(product, config);
    }

    protected Product createProduct() {
        Product product = new Product("MER_RR__2P.N1", "MER_RR__2P", 2, 2);
        product.addTiePointGrid(new TiePointGrid("latitude", 2, 2, 0.5f, 0.5f, 1, 1, new float[]{1, 1, 0, 0}));
        product.addTiePointGrid(new TiePointGrid("longitude", 2, 2, 0.5f, 0.5f, 1, 1, new float[]{0, 1, 0, 1}));
        product.setGeoCoding(new TiePointGeoCoding(product.getTiePointGrid("latitude"), product.getTiePointGrid("longitude")));
        product.setStartTime(utc("07-MAY-2010 10:25:14"));
        product.setEndTime(utc("07-MAY-2010 10:25:16"));
        product.addBand("b1", "X");
        product.addBand("b2", "Y");
        product.addBand("b3", "X+Y");
        FlagCoding flagCoding = new FlagCoding("f");
        flagCoding.addFlag("valid", 1, "Pixel is valid");
        Band f = product.addBand("f", ProductData.TYPE_UINT8);
        f.setSampleCoding(flagCoding);
        ProductData fData = f.createCompatibleRasterData();
        fData.setElemBooleanAt(0, true);
        fData.setElemBooleanAt(1, false);
        fData.setElemBooleanAt(2, true);
        fData.setElemBooleanAt(3, false);
        f.setRasterData(fData);
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
            list.add(record);
        }
        return list;
    }

    public static void addPointRecord(DefaultRecordSource recordSource, float lat, float lon, Object... attributeValues) {
        recordSource.addRecord(merge(new GeoPos(lat, lon), null, attributeValues));
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
        public GeoPos getCoordinate() {
            return coordinate;
        }

        @Override
        public Date getTime() {
            return null;
        }
    }

}
