package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.*;
import org.junit.Test;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author MarcoZ
 * @author Norman
 */
public class ExtractorTest {

    @Test(expected = NullPointerException.class)
    public void testExtractDoesNotAcceptNullProduct() throws Exception {
        new Extractor(null);
    }

    @Test(expected = NullPointerException.class)
    public void testExtractDoesNotAcceptNullRecord() throws Exception {
        new Extractor(new Product("A", "B", 2, 2)).extract(null);
    }

    @Test
    public void testThatRecordsAreGeneratedForContainedCoordinates() throws Exception {
        Extractor extractor = createExtractor();
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(1, 0))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(1, 1))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(0, 0))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(0, 1))));
    }

    @Test
    public void testThatRecordsAreNotGeneratedForOutlyingCoordinates() throws Exception {
        Extractor extractor = createExtractor();
        assertNull(extractor.extract(new TestRecord(new GeoPos(-1, -1))));
        assertNull(extractor.extract(new TestRecord(new GeoPos(-1, 2))));
        assertNull(extractor.extract(new TestRecord(new GeoPos(-3, -1))));
    }

    @Test
    public void testThatHeaderIsCorrect() throws Exception {
        Extractor extractor = createExtractor();
        Header header = extractor.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);

        int index = 0;

        // 0. derived information
        assertEquals("product_name", attributeNames[index++]);
        assertEquals("pixel_x", attributeNames[index++]);
        assertEquals("pixel_y", attributeNames[index++]);
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

        assertEquals(10, index);
    }

    @Test
    public void testThatInputIsCopied() throws Exception {
        Extractor extractor = createExtractor();
        extractor.setInput(new RecordSource() {
            @Override
            public Header getHeader() {
                return new DefaultHeader("u", "v", "w");
            }

            @Override
            public Iterable<Record> getRecords() throws Exception {
                return Arrays.asList((Record) new DefaultRecord(new GeoPos(0F, 1F), 0F, 1F, "?"));
            }
        });
        extractor.setCopyInput(true);
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
        assertEquals(13, index);

        Iterable<Record> records = extractor.getRecords();
        Record next = records.iterator().next();
        assertNotNull(next);
        Object[] attributeValues = next.getAttributeValues();
        assertNotNull(attributeValues);
        assertEquals(13, attributeValues.length);
        assertEquals(0F, (Float) attributeValues[0], 1E-10F);
        assertEquals(1F, (Float) attributeValues[1], 1E-10F);
        assertEquals("?", attributeValues[2]);
    }

    @Test
    public void testGetRecords() throws Exception {
        Extractor extractor = createExtractor();

        DefaultRecordSource input = new DefaultRecordSource(new DefaultHeader("lat", "lon"));
        input.addRecord(new GeoPos(1, 0));  // ok 1
        input.addRecord(new GeoPos(1, 1));  // ok 2
        input.addRecord(new GeoPos(-2, 1)); // reject
        input.addRecord(new GeoPos(0, 0));  // ok 3
        input.addRecord(new GeoPos(0, 1));  // ok 4
        input.addRecord(new GeoPos(0, -1)); // reject
        input.addRecord(new GeoPos(5, 0));  // reject

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
        Record extract = extractor.extract(new TestRecord(new GeoPos(1, 0)));
        assertNotNull(extract);

        assertNotNull(extractor.getHeader());
        assertNotNull(extractor.getHeader().getAttributeNames());

        GeoPos coordinate = extract.getCoordinate();
        assertEquals(1, coordinate.lat, 0.0001f);
        assertEquals(0, coordinate.lon, 0.0001f);

        Object[] values = extract.getAttributeValues();
        assertNotNull(values);
        assertEquals(extractor.getHeader().getAttributeNames().length, values.length);
        int index = 0;
        assertEquals("A", values[index++]); // product_name
        assertEquals(0.5f, (Float) values[index++], 1e-5f);  // pixel_x
        assertEquals(0.5f, (Float) values[index++], 1e-5f);  // pixel_y
        assertEquals("07-May-2010 10:25:14", values[index++]); // pixel_time
        assertEquals(0.5f, (Float) values[index++], 1e-5f);   // b1 = X
        assertEquals(0.5f, (Float) values[index++], 1e-5f);   // b2 = Y
        assertEquals(1.0f, (Float) values[index++], 1e-5f);   // b3 = X+Y
        assertEquals(1, values[index++]);    // f.valid = true
        assertEquals(1f, values[index++]);  // latitude
        assertEquals(0f, values[index++]);   // longitude
    }

    private Extractor createExtractor() {
        Product product = createProduct();
        return new Extractor(product);
    }

    private Product createProduct() {
        Product product = new Product("A", "B", 2, 2);
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

    private ProductData.UTC utc(String date)  {
        try {
            return ProductData.UTC.parse(date);
        } catch (ParseException e) {
            throw new IllegalArgumentException("date=" + date, e);
        }
    }

    private static class TestRecord implements Record {
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
    }

}
