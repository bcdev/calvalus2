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
        Product product = createProduct();
        Extractor extractor = new Extractor(product);
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(1, 0))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(1, 1))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(0, 0))));
        assertNotNull(extractor.extract(new TestRecord(new GeoPos(0, 1))));
    }

    @Test
    public void testThatRecordsAreNotGeneratedForOutlyingCoordinates() throws Exception {
        Product product = createProduct();
        Extractor extractor = new Extractor(product);
        assertNull(extractor.extract(new TestRecord(new GeoPos(-1, -1))));
        assertNull(extractor.extract(new TestRecord(new GeoPos(-1, 2))));
        assertNull(extractor.extract(new TestRecord(new GeoPos(-3, -1))));
    }

    @Test
    public void testThatHeaderIsCorrect() throws Exception {
        Product product = createProduct();
        Extractor extractor = new Extractor(product);
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
    public void testThatExtractIsCorrect() throws Exception {
        Product product = createProduct();
        Extractor extractor = new Extractor(product);
        Record extract = extractor.extract(new TestRecord(new GeoPos(1, 0)));
        assertNotNull(extract);

        assertSame(extract.getHeader(), extract.getHeader());

        GeoPos coordinate = extract.getCoordinate();
        assertEquals(1, coordinate.lat, 0.0001f);
        assertEquals(0, coordinate.lon, 0.0001f);

        Object[] values = extract.getValues();
        assertNotNull(values);
        assertEquals(extract.getHeader().getAttributeNames().length, values.length);
        int index = 0;
        assertEquals("A", values[index++]); // product_name
        assertEquals(0.5f, (Float) values[index++], 1e-5f);  // pixel_x
        assertEquals(0.5f, (Float) values[index++], 1e-5f);  // pixel_y
        assertEquals("time", values[index++]); // pixel_time
        assertEquals(0.5f, (Float)values[index++], 1e-5f);   // b1 = X
        assertEquals(0.5f, (Float)values[index++], 1e-5f);   // b2 = Y
        assertEquals(1.0f, (Float)values[index++], 1e-5f);   // b3 = X+Y
        assertEquals(1, values[index++]);    // f.valid = true
        assertEquals(1f, values[index++]);  // latitude
        assertEquals(0f, values[index++]);   // longitude
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
        public Header getHeader() {
            return new Header() {
                @Override
                public String[] getAttributeNames() {
                    return new String[]{
                            "lat",
                            "lon"
                    };
                }
            };
        }


        @Override
        public Object[] getValues() {
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
