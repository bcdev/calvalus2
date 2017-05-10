package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.utils.ProductTransformation;
import org.esa.snap.core.dataio.ProductFlipper;
import org.esa.snap.core.dataio.ProductSubsetBuilder;
import org.esa.snap.core.dataio.ProductSubsetDef;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.FlagCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.TiePointGeoCoding;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.junit.Test;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
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

    public static final int EXCLUSION_REASON_INDEX = 0;
    public static final double EPS = 1E-10;

    @Test(expected = NullPointerException.class)
    public void testConstructorArg1CannotBeNull() throws Exception {
        new ProductRecordSource(null, new DefaultHeader(true, true, "foo"), Collections.emptyList(), new MAConfig(), new AffineTransform());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorArg2CannotBeNull() throws Exception {
        new ProductRecordSource(new Product("a", "b", 4, 4), null, Collections.emptyList(), new MAConfig(), new AffineTransform());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorArg3CannotBeNull() throws Exception {
        new ProductRecordSource(new Product("a", "b", 4, 4), new DefaultHeader(true, true, "foo"), null, new MAConfig(), new AffineTransform());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorArg4CannotBeNull() throws Exception {
        new ProductRecordSource(new Product("a", "b", 4, 4), new DefaultHeader(true, true, "foo"), Collections.emptyList(), null, new AffineTransform());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorArg5CannotBeNull() throws Exception {
        new ProductRecordSource(new Product("a", "b", 4, 4), new DefaultHeader(true, true, "foo"), Collections.emptyList(), new MAConfig(), null);
    }

    @Test
    public void testThatRecordsAreGeneratedForContainedCoordinates() throws Exception {
        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);

        RecordSource input = new DefaultRecordSource(new TestHeader(true, "lat", "lon"),
                                                     new TestRecord(0, new GeoPos(1, 0)),
                                                     new TestRecord(1, new GeoPos(1, 1 - EPS)),
                                                     new TestRecord(2, new GeoPos(0, 0)),
                                                     new TestRecord(3, new GeoPos(0, 1 - EPS)));

        ProductRecordSource output = createProductRecordSource(2, 3, input, config);
        int n = 0;
        for (Record r : output.getRecords()) {
            assertSame(DefaultRecord.class, r.getClass());
            assertNotNull(r);
            n++;
        }
        assertEquals(4, n);
    }

    @Test
    public void testThatRecordsAreNotGeneratedForOutlyingCoordinates() throws Exception {
        // same test, but this time using the iterator
        DefaultRecordSource input = new DefaultRecordSource(new TestHeader(true, "lat", "lon"),
                                                            new TestRecord(0, new GeoPos(-1, -1)),
                                                            new TestRecord(1, new GeoPos(-1, 2)),
                                                            new TestRecord(2, new GeoPos(-3, -1)));
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
        DefaultRecordSource input = new DefaultRecordSource(new TestHeader(true, "lat", "lon"),
                                                            new TestRecord(0, new GeoPos(1, 0)),// in
                                                            new TestRecord(1, new GeoPos(-1, 2)),   // out
                                                            new TestRecord(2, new GeoPos(1, 1 - EPS)), // in
                                                            new TestRecord(3, new GeoPos(-1, -1)), // out
                                                            new TestRecord(4, new GeoPos(0, 0)), // in
                                                            new TestRecord(5, new GeoPos(0, 1 - EPS)), // in
                                                            new TestRecord(6, new GeoPos(-3, -1))); // out
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
        DefaultRecordSource input = new DefaultRecordSource(new TestHeader(true, "lat", "lon"),
                                                            new TestRecord(0, new GeoPos(0.0F, 0.0F)),
                                                            new TestRecord(1, new GeoPos(0.0F, 1.0F - EPS)),
                                                            new TestRecord(2, new GeoPos(1.0F, 0.0F)),
                                                            new TestRecord(3, new GeoPos(0.5F, 0.5F)),
                                                            new TestRecord(4, new GeoPos(1.0F, 1.0F - EPS)));

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
                return new TestHeader(true, "u", "v", "w");
            }

            @Override
            public Iterable<Record> getRecords() throws Exception {
                return Arrays.asList((Record) RecordUtils.create(new GeoPos(0F, 1F - EPS), null, "?"));
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
        assertEquals(0.0, (Double) attributeValues[0], 1E-5);
        assertEquals(1.0, (Double) attributeValues[1], 1E-5);
        assertEquals("?", attributeValues[2]);
    }

    @Test
    public void testThatGetRecordsFulfillsIterableContract() throws Exception {
        DefaultRecordSource input = new DefaultRecordSource(new TestHeader(true, "lat", "lon"));
        RecordUtils.addPointRecord(input, 1F, 0F);
        RecordUtils.addPointRecord(input, 1F, 1F - EPS);
        RecordUtils.addPointRecord(input, -2F, 1F - EPS);
        RecordUtils.addPointRecord(input, 0F, 0F);
        RecordUtils.addPointRecord(input, 0F, 1F - EPS);
        RecordUtils.addPointRecord(input, 0F, -1F - EPS);
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

        ProductRecordSource output = createProductRecordSource(2, 3, new DefaultRecordSource(new DefaultHeader(true, false, "lat", "lon"),
                                                                                             new TestRecord(0, new GeoPos(1.0F, 0.0F))), config);

        assertNotNull(output.getHeader());
        assertTrue(output.getHeader().hasLocation());
        assertFalse(output.getHeader().hasTime());
        assertNotNull(output.getHeader().getAttributeNames());

        Iterable<Record> records = output.getRecords();
        Iterator<Record> iterator = records.iterator();
        assertTrue(iterator.hasNext());

        Record extract = iterator.next();
        assertNotNull(extract);
        assertSame(DefaultRecord.class, extract.getClass());

        assertNull(extract.getTime()); // no insitu time
        assertEquals("", extract.getAnnotationValues()[EXCLUSION_REASON_INDEX]);

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

        RecordSource input = new DefaultRecordSource(new TestHeader(true, "latitude", "longitude"),
                                                     RecordUtils.create(new GeoPos(0.0, 0.0), null),  // --> X=0.5,Y=2.5 --> reject
                                                     RecordUtils.create(new GeoPos(0.0, 1- EPS), null),  // --> X=1.5,Y=2.5 --> ok
                                                     RecordUtils.create(new GeoPos(0.5, 0.0), null),  // --> X=0.5,Y=1.5 --> reject
                                                     RecordUtils.create(new GeoPos(0.5, 1- EPS), null),  // --> X=1.5,Y=1.5 --> ok
                                                     RecordUtils.create(new GeoPos(1- EPS, 0.0), null),  // --> X=0.5,Y=0.5 --> reject
                                                     RecordUtils.create(new GeoPos(1- EPS, 1- EPS), null));  // --> X=0.5,Y=0.5 --> ok
        ProductRecordSource output = createProductRecordSource(2, 3, input, config);
        List<Record> records = getRecords(output);
        assertEquals(6, records.size());
        int goodRecords = 0;
        int allMaskedRecords = 0;
        for (Record record : records) {
            String reason = (String) record.getAnnotationValues()[EXCLUSION_REASON_INDEX];
            if (reason.isEmpty()) {
                goodRecords++;
            } else if (reason.equals(PixelExtractor.EXCLUSION_REASON_ALL_MASKED)) {
                allMaskedRecords++;
            }
        }
        assertEquals(3, goodRecords);
        assertEquals(3, allMaskedRecords);
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

        RecordSource input = new DefaultRecordSource(new TestHeader(true, "latitude", "longitude"),
                                                     RecordUtils.create(new GeoPos(lat, lon), null));  // --> center of first macro pixel

        ProductRecordSource output = createProductRecordSource(w, h, input, config);

        List<Record> records = getRecords(output);
        assertEquals(1, records.size());

        final int PIXEL_X = 2;
        final int PIXEL_Y = 3;
        final int B2 = 7;

        Object[] firstAttributeValues = records.get(0).getAttributeValues();

        //  "pixel_x" : int[9]
        Object pixelXValue = firstAttributeValues[PIXEL_X];
        assertEquals(int[].class, pixelXValue.getClass());
        int[] actualX = (int[]) pixelXValue;
        assertEquals(9, actualX.length);
        assertArrayEquals(new int[]{0, 1, 2, 0, 1, 2, 0, 1, 2}, actualX);

        //  "pixel_y" : int[9]
        Object pixelYValue = firstAttributeValues[PIXEL_Y];
        assertEquals(int[].class, pixelYValue.getClass());
        int[] actualY = (int[]) pixelYValue;
        assertEquals(9, actualY.length);
        assertArrayEquals(new int[]{0, 0, 0, 1, 1, 1, 2, 2, 2}, actualY);

        //  "b2" : float[9]
        Object b2Value = firstAttributeValues[B2];
        assertEquals(int[].class, b2Value.getClass());
        int[] actualB2 = (int[]) b2Value;
        assertEquals(9, actualB2.length);
        assertArrayEquals(new int[]{0, 0, 0, 1, 1, 1, 2, 2, 2}, actualB2);

    }

    @Test
    public void test3x3MacroPixelGeneratesFloatArraysWith9ElementsUsingTransformation_subset() throws Exception {
        MAConfig config = new MAConfig();
        config.setCopyInput(false);
        config.setMacroPixelSize(3);

        int w = 2 * 3;
        int h = 3 * 3;

        // This (lat,lon) corresponds to the center of the middle left pixel of the macro pixel (x=1.5, y=4.5)
        float lon = 0.0F + 1.0F / (w - 1.0F);
        float lat = 1.0F - 4.0F / (h - 1.0F);

        RecordSource input = new DefaultRecordSource(new TestHeader(true, "latitude", "longitude"),
                                                     RecordUtils.create(new GeoPos(lat, lon), null));  // --> center of 2nd macro pixel

        Rectangle subsetRect = new Rectangle(0, 3, w, 6);
        Product product = createProduct(w, h);
        ProductSubsetDef subsetDef = new ProductSubsetDef();
        subsetDef.setRegion(subsetRect);
        subsetDef.setTreatVirtualBandsAsRealBands(true);
        Product subset = ProductSubsetBuilder.createProductSubset(product, subsetDef, "subset", "subset");

        AffineTransform transform = new ProductTransformation(subsetRect, false, false).getTransform();

        Header referenceRecordHeader = input.getHeader();
        PixelPosProvider pixelPosProvider = new PixelPosProvider(product,
                                                                 PixelTimeProvider.create(product),
                                                                 config.getMaxTimeDifference(),
                                                                 referenceRecordHeader.hasTime());

        List<PixelPosProvider.PixelPosRecord> pixelPosRecords;
        try {
            pixelPosRecords = pixelPosProvider.computePixelPosRecords(input.getRecords());
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve input records.", e);
        }
        assertEquals(1, pixelPosRecords.size());
        PixelPos pixelPos = pixelPosRecords.get(0).getPixelPos();
        assertEquals(1.5f, pixelPos.x, 1E-5);
        assertEquals(4.5f, pixelPos.y, 1E-5);

        ProductRecordSource output = new ProductRecordSource(subset, referenceRecordHeader, pixelPosRecords, config, transform);

        List<Record> records = getRecords(output);
        assertEquals(1, records.size());

        final int PIXEL_X = 2;
        final int PIXEL_Y = 3;
        final int B2 = 7;

        Object[] attributeValues = records.get(0).getAttributeValues();
        //  "pixel_x" : int[9]
        Object pixelXValue = attributeValues[PIXEL_X];
        assertEquals(int[].class, pixelXValue.getClass());
        int[] actualX = (int[]) pixelXValue;
        assertEquals(9, actualX.length);
        assertArrayEquals(new int[]{0, 1, 2, 0, 1, 2, 0, 1, 2}, actualX);

        //  "pixel_y" : int[9]
        Object pixelYValue = attributeValues[PIXEL_Y];
        assertEquals(int[].class, pixelYValue.getClass());
        int[] actualY = (int[]) pixelYValue;
        assertEquals(9, actualY.length);
        assertArrayEquals(new int[]{3, 3, 3, 4, 4, 4, 5, 5, 5}, actualY);

        //  "b2" : int[9]
        Object b2Value = attributeValues[B2];
        assertEquals(int[].class, b2Value.getClass());
        int[] actualB2 = (int[]) b2Value;
        assertEquals(9, actualB2.length);
        assertArrayEquals(new int[]{3, 3, 3, 4, 4, 4, 5, 5, 5}, actualB2);
    }

    @Test
    public void test3x3MacroPixelGeneratesFloatArraysWith9ElementsUsingTransformation_SubsetAndFlip() throws Exception {
        MAConfig config = new MAConfig();
        config.setCopyInput(false);
        config.setMacroPixelSize(3);

        int w = 2 * 3;
        int h = 3 * 3;

        // This (lat,lon) corresponds to the center of the middle left pixel of the macro pixel (x=1.5, y=4.5)
        float lon = 0.0F + 1.0F / (w - 1.0F);
        float lat = 1.0F - 4.0F / (h - 1.0F);

        RecordSource input = new DefaultRecordSource(new TestHeader(true, "latitude", "longitude"),
                                                     RecordUtils.create(new GeoPos(lat, lon), null));  // --> center of 2nd macro pixel

        Rectangle subsetRect = new Rectangle(0, 3, w, 6);
        Product product = createProduct(w, h);
        ProductSubsetDef subsetDef = new ProductSubsetDef();
        subsetDef.setRegion(subsetRect);
        subsetDef.setTreatVirtualBandsAsRealBands(true);
        Product subset = ProductSubsetBuilder.createProductSubset(product, subsetDef, "subset", "subset");

        Product flippedProduct = ProductFlipper.createFlippedProduct(subset, ProductFlipper.FLIP_BOTH, "flip", "flip");

        AffineTransform transform = new ProductTransformation(subsetRect, true, true).getTransform();

        Header referenceRecordHeader = input.getHeader();
        PixelPosProvider pixelPosProvider = new PixelPosProvider(product,
                                                                 PixelTimeProvider.create(product),
                                                                 config.getMaxTimeDifference(),
                                                                 referenceRecordHeader.hasTime());

        List<PixelPosProvider.PixelPosRecord> pixelPosRecords;
        try {
            pixelPosRecords = pixelPosProvider.computePixelPosRecords(input.getRecords());
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve input records.", e);
        }
        assertEquals(1, pixelPosRecords.size());
        PixelPos pixelPos = pixelPosRecords.get(0).getPixelPos();
        assertEquals(1.5, pixelPos.x, 1E-5);
        assertEquals(4.5, pixelPos.y, 1E-5);

        ProductRecordSource output = new ProductRecordSource(flippedProduct, referenceRecordHeader, pixelPosRecords, config, transform);

        List<Record> records = getRecords(output);
        assertEquals(1, records.size());

        final int PIXEL_X = 2;
        final int PIXEL_Y = 3;
        final int B2 = 7;

        Object[] attributeValues = records.get(0).getAttributeValues();
        //  "pixel_x" : int[9]
        Object pixelXValue = attributeValues[PIXEL_X];
        assertEquals(int[].class, pixelXValue.getClass());
        int[] actualX = (int[]) pixelXValue;
        assertEquals(9, actualX.length);
        assertArrayEquals(new int[]{0, 1, 2, 0, 1, 2, 0, 1, 2}, actualX);

        //  "pixel_y" : int[9]
        Object pixelYValue = attributeValues[PIXEL_Y];
        assertEquals(int[].class, pixelYValue.getClass());
        int[] actualY = (int[]) pixelYValue;
        assertEquals(9, actualY.length);
        assertArrayEquals(new int[]{3, 3, 3, 4, 4, 4, 5, 5, 5}, actualY);

        //  "b2" : int[9]
        Object b2Value = attributeValues[B2];
        assertEquals(int[].class, b2Value.getClass());
        int[] actualB2 = (int[]) b2Value;
        assertEquals(9, actualB2.length);
        assertArrayEquals(new int[]{3, 3, 3, 4, 4, 4, 5, 5, 5}, actualB2);
    }

    @Test
    public void testTimeCriterion() throws Exception {

        // Note: test product starts at  "07-MAY-2010 10:25:14"
        //                    ends at    "07-MAY-2010 11:24:46"


        MAConfig config = new MAConfig();
        config.setMacroPixelSize(1);
        config.setMaxTimeDifference(null);

        RecordSource input = new DefaultRecordSource(new DefaultHeader(true, true, "latitude", "longitude", "time"),
                                                     RecordUtils.create(new GeoPos(0, 0), date("07-MAY-2010 11:25:00")), // still ok
                                                     RecordUtils.create(new GeoPos(0, 1 - EPS), date("07-MAY-2010 10:25:00")), // ok
                                                     RecordUtils.create(new GeoPos(1, 0), date("07-MAY-2010 10:59:00")), // ok
                                                     RecordUtils.create(new GeoPos(1, 1 - EPS), date("07-MAY-2010 09:25:00"))); // still ok

        ProductRecordSource output = createProductRecordSource(2, 3, input, config);
        List<Record> records = getRecords(output);
        assertEquals(4, records.size());

        config = new MAConfig();
        config.setMacroPixelSize(1);
        config.setMaxTimeDifference("0.25");

        input = new DefaultRecordSource(new DefaultHeader(true, true, "latitude", "longitude", "time"),
                                        RecordUtils.create(new GeoPos(0.5F, 0.5F), date("07-MAY-2010 11:26:00")), // rejected
                                        RecordUtils.create(new GeoPos(0.5F, 0.5F), date("07-JUN-2010 13:25:00")), // rejected
                                        RecordUtils.create(new GeoPos(0.5F, 0.5F), date("07-MAY-2010 10:59:00")), // ok
                                        RecordUtils.create(new GeoPos(0.5F, 0.5F), date("07-MAY-2010 08:10:00"))); // rejected
        output = createProductRecordSource(2, 3, input, config);
        records = getRecords(output);
        assertEquals(1, records.size());
    }

    private ProductRecordSource createProductRecordSource(int w, int h, RecordSource input, MAConfig config) {
        Product product = createProduct(w, h);
        return createProductRecordSource(product, input, config);
    }

    private ProductRecordSource createProductRecordSource(Product product, RecordSource referenceRecords, MAConfig maConfig) {
        return createProductRecordSource(product, referenceRecords, maConfig, new AffineTransform());
    }

    private ProductRecordSource createProductRecordSource(Product product, RecordSource referenceRecords, MAConfig maConfig, AffineTransform tranform) {
        Header referenceRecordHeader = referenceRecords.getHeader();
        PixelPosProvider pixelPosProvider = new PixelPosProvider(product,
                                                                 PixelTimeProvider.create(product),
                                                                 maConfig.getMaxTimeDifference(),
                                                                 referenceRecordHeader.hasTime());

        List<PixelPosProvider.PixelPosRecord> pixelPosRecords;
        try {
            pixelPosRecords = pixelPosProvider.computePixelPosRecords(referenceRecords.getRecords());
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve input records.", e);
        }

        return new ProductRecordSource(product, referenceRecordHeader, pixelPosRecords, maConfig, tranform);
    }

    private Product createProduct(int w, int h) {
        Product product = new Product("MER_RR__2P.N1", "MER_RR__2P", w, h);
        product.addTiePointGrid(new TiePointGrid("latitude", 2, 2, 0.5, 0.5, w - 1, h - 1, new float[]{1, 1, 0, 0}));
        product.addTiePointGrid(new TiePointGrid("longitude", 2, 2, 0.5, 0.5, w - 1, h - 1, new float[]{0, 1, 0, 1}));
        product.setSceneGeoCoding(new TiePointGeoCoding(product.getTiePointGrid("latitude"), product.getTiePointGrid("longitude")));
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
        ArrayList<Record> list = new ArrayList<>();
        for (Record record : records) {
            assertNotNull("Unexpected null record detected.", record);
            list.add(record);
        }
        return list;
    }

    private DefaultRecordSource createRecordSource(int n) {
        Record[] records = new Record[n];
        for (int i = 0; i < records.length; i++) {
            records[i] = RecordUtils.create(new GeoPos(i, i), 1000L + i);
        }
        return new DefaultRecordSource(new TestHeader(true, "lat", "lon"), records);
    }

    protected static class TestRecord implements Record {

        private final int recordId;
        GeoPos coordinate;

        private TestRecord(int recordId, GeoPos coordinate) {
            this.recordId = recordId;
            this.coordinate = new GeoPos(coordinate);
        }

        @Override
        public int getId() {
            return recordId;
        }

        @Override
        public Object[] getAttributeValues() {
            return new Object[]{
                    coordinate.lat,
                    coordinate.lon,
            };
        }

        @Override
        public Object[] getAnnotationValues() {
            return new Object[0];
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
