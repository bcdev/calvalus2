package com.bc.calvalus.processing.ma;

import com.bc.ceres.core.Assert;
import org.esa.beam.framework.datamodel.*;

import java.text.DateFormat;
import java.util.*;

/**
 * Extracts an output record.
 *
 * @author MarcoZ
 * @author Norman
 */
public class Extractor implements RecordSource {
    public static final Float FLOAT_NAN = new Float(Float.NaN);
    public static final Integer INTEGER_NAN = new Integer(0);
    private final Product product;
    private Header header;
    private RecordSource input;
    private boolean copyInput;
    private String dateFormat;

    public Extractor(Product product) {
        Assert.notNull(product, "product");
        this.product = product;
        this.dateFormat = ProductData.UTC.DATE_FORMAT_PATTERN;
    }

    public void setInput(RecordSource input) {
        this.input = input;
    }

    public void setCopyInput(boolean copyInput) {
        this.copyInput = copyInput;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    @Override
    public Header getHeader() {
        if (header == null) {
            header = createHeader();
        }
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

        final PixelPos pixelPos = product.getGeoCoding().getPixelPos(input.getCoordinate(), null);
        final PixelTimeProvider pixelTimeProvider = PixelTimeProvider.create(product, dateFormat);

        if (pixelPos.isValid() && product.containsPixel(pixelPos)) {
            final float[] floatSample = new float[1];
            final int[] intSample = new int[1];

            final Object[] values = new Object[getHeader().getAttributeNames().length];

            int index = 0;
            if (copyInput) {
                Object[] inputValues = input.getAttributeValues();
                System.arraycopy(inputValues, 0, values, 0, inputValues.length);
                index = inputValues.length;
            }

            values[index++] = product.getName();
            values[index++] = pixelPos.x;
            values[index++] = pixelPos.y;
            values[index++] = pixelTimeProvider != null ? pixelTimeProvider.getTime(pixelPos) : "";

            final int x = (int) pixelPos.x;
            final int y = (int) pixelPos.y;

            final Band[] productBands = product.getBands();
            for (Band band : productBands) {
                if (!band.isFlagBand()) {
                    if (band.isFloatingPointType()) {
                        if (band.isPixelValid(x, y)) {
                            band.readPixels(x, y, 1, 1, floatSample);
                            values[index++] = floatSample[0];
                        } else {
                            values[index++] = FLOAT_NAN;
                        }
                    } else {
                        if (band.isPixelValid(x, y)) {
                            band.readPixels(x, y, 1, 1, intSample);
                            values[index++] = intSample[0];
                        } else {
                            values[index++] = INTEGER_NAN;
                        }
                    }
                }
            }
            for (TiePointGrid tiePointGrid : product.getTiePointGrids()) {
                tiePointGrid.readPixels(x, y, 1, 1, floatSample);
                values[index++] = floatSample[0];
            }
            return new DefaultRecord(input.getCoordinate(), values);
        }
        return null;
    }

    private Header createHeader() {
        final List<String> attributeNames = new ArrayList<String>();

        if (copyInput && input != null) {
            Collections.addAll(attributeNames, input.getHeader().getAttributeNames());
        }

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
        private Record next;
        private boolean nextValid;

        public RecordIterator(Iterator<Record> inputIterator) {
            this.inputIterator = inputIterator;
        }

        @Override
        public boolean hasNext() {
            if (!nextValid) {
                next = next0();
                nextValid = true;
            }
            return next != null;
        }

        @Override
        public Record next() {
            if (!nextValid) {
                next = next0();
                nextValid = true;
            }
            if (next == null) {
                throw new NoSuchElementException();
            }
            nextValid = false;
            return next;
        }

        private Record next0() {
            while (inputIterator.hasNext()) {
                Record record = inputIterator.next();
                try {
                    Record next = extract(record);
                    if (next != null) {
                        return next;
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception t) {
                    throw new RuntimeException(t);
                }
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class PixelTimeProvider {

        private final DateFormat dateFormat;
        private final double startMJD;
        private final double deltaMJD;

        static PixelTimeProvider create(Product product, String format) {
            final ProductData.UTC startTime = product.getStartTime();
            final ProductData.UTC endTime = product.getEndTime();
            final int rasterHeight = product.getSceneRasterHeight();
            if (startTime != null && endTime != null && rasterHeight > 1) {
                return new PixelTimeProvider(ProductData.UTC.createDateFormat(format),
                                             startTime.getMJD(),
                                             (endTime.getMJD() - startTime.getMJD()) / (rasterHeight - 1));
            } else {
                return null;
            }
        }

        private PixelTimeProvider(DateFormat dateFormat, double startMJD, double deltaMJD) {
            this.dateFormat = dateFormat;
            this.startMJD = startMJD;
            this.deltaMJD = deltaMJD;
        }

        public String getTime(PixelPos pixelPos) {
            return dateFormat.format(getUTC(pixelPos).getAsDate());
        }

        private ProductData.UTC getUTC(PixelPos pixelPos) {
            return new ProductData.UTC(startMJD + Math.floor(pixelPos.y) * deltaMJD);
        }
    }
}
