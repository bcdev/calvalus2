package com.bc.calvalus.processing.ma;

import com.bc.ceres.core.Assert;
import org.esa.beam.framework.datamodel.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Extracts an output record.
 *
 * @author MarcoZ
 * @author Norman
 */
public class Extractor implements RecordSource {
    private final Product product;
    private final Header header;
    private RecordSource input;

    public Extractor(Product product) {
        Assert.notNull(product, "product");
        this.product = product;
        this.header = createHeader();
    }

    public RecordSource getInput() {
        return input;
    }

    public void setInput(RecordSource input) {
        this.input = input;
    }

    @Override
    public Header getHeader() {
        return header;
    }

    /**
     * Extracts output records from input records.
     * Input records must have been set using the {@link #setInput(RecordSource)} method.
     *
     * @return Extracted records.
     * @throws IllegalStateException if no input has been set
     */
    @Override
    public Iterable<Record> getRecords() throws Exception {
        return new RecordIterable();
    }

    public Record extract(Record input) throws Exception {
        Assert.notNull(input, "input");
        PixelPos pixelPos = product.getGeoCoding().getPixelPos(input.getCoordinate(), null);

        if (pixelPos.isValid() && product.containsPixel(pixelPos)) {
            float[] floatSample = new float[1];
            int[] intSample = new int[1];
            Object[] values = new Object[header.getAttributeNames().length];
            int index = 0;
            values[index++] = product.getName();
            values[index++] = pixelPos.x;
            values[index++] = pixelPos.y;
            values[index++] = "time"; // TODO
            Band[] productBands = product.getBands();
            for (Band band : productBands) {
                if (!band.isFlagBand()) {
                    if (band.isFloatingPointType()) {
                        band.readPixels((int) pixelPos.x, (int) pixelPos.y, 1, 1, floatSample);
                        values[index++] = floatSample[0];
                    } else {
                        band.readPixels((int) pixelPos.x, (int) pixelPos.y, 1, 1, intSample);
                        values[index++] = intSample[0];
                    }
                }
            }
            for (TiePointGrid tiePointGrid : product.getTiePointGrids()) {
                tiePointGrid.readPixels((int) pixelPos.x, (int) pixelPos.y, 1, 1, floatSample);
                values[index++] = floatSample[0];
            }
            return new DefaultRecord(input.getCoordinate(), values);
        }
        return null;
    }

    private Header createHeader() {
        final List<String> attributeNames = new ArrayList<String>();

        // 0. derived information
        attributeNames.add("product_name");
        attributeNames.add("pixel_x");
        attributeNames.add("pixel_y");
        attributeNames.add("pixel_time");

        // 1. bands
        Band[] productBands = product.getBands();
        for (Band band : productBands) {
            if (!band.isFlagBand()) {
                attributeNames.add(band.getName());
            }
        }

        // 2. flags (virtual bands)
        for (Band band : productBands) {
            if (band.isFlagBand()) {
                FlagCoding flagCoding = band.getFlagCoding();
                String[] flagNames = flagCoding.getFlagNames();
                for (String flagName : flagNames) {
                    attributeNames.add(band.getName() + "." + flagName);
                    // todo - move this side-effect into a separate method
                    product.addBand("flag_" + band.getName() + "_" + flagName, band.getName() + "." + flagName, ProductData.TYPE_INT8);
                }
            }
        }

        // 3. tie-points
        attributeNames.addAll(Arrays.asList(product.getTiePointGridNames()));

        return new DefaultHeader(attributeNames.toArray(new String[attributeNames.size()]));
    }


    private class RecordIterable implements Iterable<Record> {
        private final Iterator<Record> inputIterator;

        public RecordIterable() throws Exception {
            if (input == null) {
                throw new IllegalStateException("No input record source set.");
            }
            this.inputIterator = input.getRecords().iterator();
        }

        @Override
        public Iterator<Record> iterator() {
            return new RecordIterator(inputIterator);
        }
    }

    private class RecordIterator implements Iterator<Record> {
        private final Iterator<Record> inputIterator;

        public RecordIterator(Iterator<Record> inputIterator) {
            this.inputIterator = inputIterator;
        }

        @Override
        public boolean hasNext() {
            return inputIterator.hasNext();
        }

        @Override
        public Record next() {
            Record record = inputIterator.next();
            try {
                return extract(record);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception t) {
                throw new RuntimeException(t);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
