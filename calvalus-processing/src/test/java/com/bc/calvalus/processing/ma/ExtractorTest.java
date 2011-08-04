package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.*;
import org.junit.Test;

import static org.junit.Assert.*;

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
    public void testGetRecords() throws Exception {
        Extractor extractor = createExtractor();

        DefaultRecordSource input = new DefaultRecordSource(new DefaultHeader("lat", "lon"));
        input.addRecord(new GeoPos(1, 0));  // ok
        input.addRecord(new GeoPos(1, 1));  // ok
        input.addRecord(new GeoPos(0, 0));  // ok
        input.addRecord(new GeoPos(0, 1));  // ok

        extractor.setInput(input);

        Iterable<Record> records = extractor.getRecords();
        assertNotNull(records);
        int n = 0;
        for (Record record : records) {
            n++;
        }
        assertEquals(4, n);
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
        assertEquals("time", values[index++]); // pixel_time
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
        product.addBand("b1", "X");
        product.addBand("b2", "Y");
        product.addBand("b3", "X+Y");
        FlagCoding flagCoding = new FlagCoding("f");
        flagCoding.addFlag("valid", 1, "I feel soil under my feet");
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
