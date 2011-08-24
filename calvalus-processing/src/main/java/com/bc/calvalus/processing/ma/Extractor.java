package com.bc.calvalus.processing.ma;

import com.bc.ceres.core.Assert;
import org.esa.beam.framework.datamodel.*;

import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;

/**
 * Extracts an output record.
 *
 * @author MarcoZ
 * @author Norman
 */
public class Extractor implements RecordSource {
    public static final Float FLOAT_NAN = Float.NaN;
    public static final String GOOD_PIXEL_MASK_NAME = "good_pixel";
    private final Product product;
    private final MAConfig config;
    private final PixelTimeProvider pixelTimeProvider;
    private Header header;
    private RecordSource input;
    private boolean sortInputByPixelYX;

    public Extractor(Product product, MAConfig config) {
        Assert.notNull(product, "product");
        Assert.notNull(config, "config");
        this.product = product;
        this.config = config;
        this.pixelTimeProvider = PixelTimeProvider.create(product);
    }

    public MAConfig getConfig() {
        return config;
    }

    public void setInput(RecordSource input) {
        this.input = input;
    }

    public void setSortInputByPixelYX(boolean sortInputByPixelYX) {
        this.sortInputByPixelYX = sortInputByPixelYX;
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
        if (input == null) {
            throw new IllegalStateException("No input record source set.");
        }

        // If the time criterion cannot be used, this data product cannot be used.
        if (shallApplyTimeCriterion() && !canApplyTimeCriterion()) {
            return Collections.emptyList();
        }

        if (shallApplyGoodPixelExpression()) {
            addGoodPixelMaskToProduct();
        }

        final Iterable<Record> records = input.getRecords();
        return new Iterable<Record>() {
            @Override
            public Iterator<Record> iterator() {
                if (sortInputByPixelYX) {
                    return new PixelPosRecordIterator(getInputRecordsSortedByPixelYX(records).iterator());
                } else {
                    return new RecordIterator(records.iterator());
                }
            }
        };
    }

    private void addGoodPixelMaskToProduct() {
        Mask mask = product.getMaskGroup().get(GOOD_PIXEL_MASK_NAME);
        if (mask != null) {
            product.getMaskGroup().remove(mask);
            mask.dispose();
        }

        int width = product.getSceneRasterWidth();
        int height = product.getSceneRasterHeight();

        Mask goodPixelMask = Mask.BandMathsType.create(GOOD_PIXEL_MASK_NAME,
                                                       null,
                                                       width,
                                                       height,
                                                       config.getGoodPixelExpression(),
                                                       Color.RED,
                                                       0.5);
        product.getMaskGroup().add(goodPixelMask);
    }

    private boolean shallApplyGoodPixelExpression() {
        return config.getGoodPixelExpression() != null && !config.getGoodPixelExpression().isEmpty();
    }

    private boolean shallApplyTimeCriterion() {
        return config.getMaxTimeDifference() != null;
    }

    private boolean canApplyTimeCriterion() {
        return pixelTimeProvider != null && getHeader().getTimeIndex() >= 0;
    }

    private Iterable<PixelPosRecord> getInputRecordsSortedByPixelYX(Iterable<Record> inputRecords) {
        ArrayList<PixelPosRecord> pixelPosList = new ArrayList<PixelPosRecord>(128);
        for (Record inputRecord : inputRecords) {
            final PixelPos pixelPos = getTemporallyAndSpatiallyValidPixelPos(inputRecord);
            if (pixelPos != null) {
                pixelPosList.add(new PixelPosRecord(pixelPos, inputRecord));
            }
        }
        PixelPosRecord[] records = pixelPosList.toArray(new PixelPosRecord[pixelPosList.size()]);
        Arrays.sort(records, new Comparator<PixelPosRecord>() {
            @Override
            public int compare(PixelPosRecord o1, PixelPosRecord o2) {
                float y1 = o1.pixelPos.y;
                float y2 = o2.pixelPos.y;
                if (y1 < y2) {
                    return -2;
                }
                if (y1 > y2) {
                    return 2;
                }
                float x1 = o1.pixelPos.x;
                float x2 = o2.pixelPos.x;
                if (x1 < x2) {
                    return -1;
                }
                if (x1 > x2) {
                    return 1;
                }
                return 0;
            }
        });
        return Arrays.asList(records);
    }

    /**
     * Extracts an output record.
     *
     * @param inputRecord The input record.
     * @return The output record or {@code null}, if a certain inclusion criterion is not met.
     * @throws IOException If any I/O error occurs
     */
    public Record extract(Record inputRecord) throws IOException {
        Assert.notNull(inputRecord, "inputRecord");
        final PixelPos pixelPos = getTemporallyAndSpatiallyValidPixelPos(inputRecord);
        if (pixelPos != null) {
            return extract(inputRecord, pixelPos);
        }
        return null;
    }

    private PixelPos getTemporallyAndSpatiallyValidPixelPos(Record referenceRecord) {

        final boolean checkTime = shallApplyTimeCriterion() && canApplyTimeCriterion();
        if (checkTime) {

            long minReferenceTime = getMinReferenceTime(referenceRecord);
            if (minReferenceTime > product.getEndTime().getAsDate().getTime()) {
                return null;
            }

            long maxReferenceTime = getMaxReferenceTime(referenceRecord);
            if (maxReferenceTime < product.getStartTime().getAsDate().getTime()) {
                return null;
            }

            PixelPos pixelPos = getSpatiallyValidPixelPos(referenceRecord);
            if (pixelPos != null) {
                long pixelTime = pixelTimeProvider.getTime(pixelPos).getTime();
                if (pixelTime >= minReferenceTime && pixelTime <= maxReferenceTime) {
                    return pixelPos;
                }
            }
        } else {
            PixelPos pixelPos = getSpatiallyValidPixelPos(referenceRecord);
            if (pixelPos != null) {
                return pixelPos;
            }
        }
        return null;
    }

    private PixelPos getSpatiallyValidPixelPos(Record referenceRecord) {
        final PixelPos pixelPos = product.getGeoCoding().getPixelPos(referenceRecord.getLocation(), null);
        if (pixelPos.isValid() && product.containsPixel(pixelPos)) {
            return pixelPos;
        }
        return null;
    }

    private long getMinReferenceTime(Record referenceRecord) {
        return referenceRecord.getTimestamp().getTime() - getMaxTimeDifferenceInMillis();
    }

    private long getMaxReferenceTime(Record referenceRecord) {
        return referenceRecord.getTimestamp().getTime() + getMaxTimeDifferenceInMillis();
    }

    private long getMaxTimeDifferenceInMillis() {
        return Math.round(config.getMaxTimeDifference() * 60 * 60 * 1000);
    }

    /**
     * Extracts an output record.
     *
     * @param inputRecord The input record.
     * @param pixelPos    The validated pixel pos.
     * @return The output record or {@code null}, if a certain inclusion criterion is not met.
     * @throws IOException If an I/O error occurs
     */
    private Record extract(Record inputRecord, PixelPos pixelPos) throws IOException {

        final int macroPixelSize = getConfig().macroPixelSize;

        final Rectangle macroPixelRect = new Rectangle(product.getSceneRasterWidth(), product.getSceneRasterHeight()).intersection(
                new Rectangle((int) pixelPos.x - macroPixelSize / 2,
                              (int) pixelPos.y - macroPixelSize / 2,
                              macroPixelSize, macroPixelSize));


        int x0 = macroPixelRect.x;
        int y0 = macroPixelRect.y;
        int width = macroPixelRect.width;
        int height = macroPixelRect.height;

        final int[] maskSamples = new int[width * height];
        final Mask mask = product.getMaskGroup().get(GOOD_PIXEL_MASK_NAME);
        if (mask != null) {
            mask.readPixels(macroPixelRect.x, macroPixelRect.y, macroPixelRect.width, macroPixelRect.height, maskSamples);
            boolean allBad = true;
            for (int sample : maskSamples) {
                if (sample != 0) {
                    allBad = false;
                    break;
                }
            }

            if (allBad) {
                return null;
            }
        }


        final Object[] values = new Object[getHeader().getAttributeNames().length];

        int index = 0;
        if (config.isCopyInput()) {
            Object[] inputValues = inputRecord.getAttributeValues();
            System.arraycopy(inputValues, 0, values, 0, inputValues.length);
            index = inputValues.length;
        }

        final int[] pixelXPositions = new int[width * height];
        final int[] pixelYPositions = new int[width * height];
        final float[] pixelLatitudes = new float[width * height];
        final float[] pixelLongitudes = new float[width * height];

        for (int i = 0, y = y0; y < y0 + height; y++) {
            for (int x = x0; x < x0 + width; x++, i++) {
                PixelPos pp = new PixelPos(x + 0.5F, y + 0.5F);
                GeoPos gp =  product.getGeoCoding().getGeoPos(pp, null);
                // todo - compute actual source pixel positions (need offsets here!)  (mz,nf)
                pixelXPositions[i] = x;
                pixelYPositions[i] = y;
                pixelLatitudes[i] = gp.lat;
                pixelLongitudes[i] = gp.lon;
            }
        }

        // field "source_name"
        values[index++] = product.getName();
        // field "pixel_x"
        values[index++] = pixelXPositions;
        // field "pixel_y"
        values[index++] = pixelYPositions;
        // field "pixel_lat"
        values[index++] = pixelLatitudes;
        // field "pixel_lon"
        values[index++] = pixelLongitudes;
        // field "pixel_time"
        values[index++] = pixelTimeProvider != null ? pixelTimeProvider.getTime(pixelPos) : null;
        // field "pixel_mask"
        values[index++] = maskSamples;

        final Band[] productBands = product.getBands();
        for (Band band : productBands) {
            if (!band.isFlagBand()) {
                if (band.isFloatingPointType()) {
                    final float[] floatSamples = new float[macroPixelRect.width * macroPixelRect.height];
                    band.readPixels(x0, y0, width, height, floatSamples);
                    values[index++] = floatSamples;
                    maskNaN(band, x0, y0, width, height, floatSamples);
                } else {
                    final int[] intSamples = new int[macroPixelRect.width * macroPixelRect.height];
                    band.readPixels(x0, y0, width, height, intSamples);
                    values[index++] = intSamples;
                }
            }
        }

        for (TiePointGrid tiePointGrid : product.getTiePointGrids()) {
            final float[] floatSamples = new float[macroPixelRect.width * macroPixelRect.height];
            tiePointGrid.readPixels(x0, y0, width, height, floatSamples);
            values[index++] = floatSamples;
        }

        return new DefaultRecord(inputRecord.getLocation(), inputRecord.getTimestamp(), values);
    }

    private void maskNaN(Band band, int x0, int y0, int width, int height, float[] samples) {
        for (int i = 0, y = y0; y < y0 + height; y++) {
            for (int x = x0; x < x0 + width; x++, i++) {
                if (!band.isPixelValid(x0, y0)) {
                    samples[i] = FLOAT_NAN;
                }
            }
        }
    }

    private Header createHeader() {
        final List<String> attributeNames = new ArrayList<String>();

        if (config.isCopyInput()) {
            if (input == null) {
                throw new IllegalStateException("Still no input set, but config.copyInput=true.");
            }
            Collections.addAll(attributeNames, input.getHeader().getAttributeNames());
        }

        // 0. derived information
        attributeNames.add("source_name");
        attributeNames.add("pixel_x");
        attributeNames.add("pixel_y");
        attributeNames.add("pixel_lat");
        attributeNames.add("pixel_lon");
        attributeNames.add("pixel_time");
        attributeNames.add("pixel_mask");

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

        DefaultHeader defaultHeader = new DefaultHeader(attributeNames.toArray(new String[attributeNames.size()]));
        defaultHeader.setLatitudeIndex(attributeNames.indexOf("pixel_lat"));
        defaultHeader.setLongitudeIndex(attributeNames.indexOf("pixel_lon"));
        defaultHeader.setTimeIndex(attributeNames.indexOf("pixel_time"));
        defaultHeader.setTimeFormat(ProductData.UTC.createDateFormat(config.getExportDateFormat()));
        return defaultHeader;
    }


    private abstract class OutputRecordGenerator<T> implements Iterator<Record> {
        private final Iterator<T> inputIterator;
        private Record next;
        private boolean nextValid;

        public OutputRecordGenerator(Iterator<T> inputIterator) {
            this.inputIterator = inputIterator;
        }

        @Override
        public boolean hasNext() {
            ensureValidNext();
            return next != null;
        }

        @Override
        public Record next() {
            ensureValidNext();
            if (next == null) {
                throw new NoSuchElementException();
            }
            nextValid = false;
            return next;
        }

        private void ensureValidNext() {
            if (!nextValid) {
                next = getNextNonNullRecord();
                nextValid = true;
            }
        }

        private Record getNextNonNullRecord() {
            while (inputIterator.hasNext()) {
                T input = inputIterator.next();
                try {
                    Record next = getRecord(input);
                    if (next != null) {
                        return next;
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to get output record for input " + input, e);
                }
            }
            return null;
        }

        protected abstract Record getRecord(T input) throws IOException;

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class RecordIterator extends OutputRecordGenerator<Record> {

        public RecordIterator(Iterator<Record> inputIterator) {
            super(inputIterator);
        }

        @Override
        protected Record getRecord(Record input) throws IOException {
            return extract(input);
        }
    }

    private class PixelPosRecordIterator extends OutputRecordGenerator<PixelPosRecord> {

        public PixelPosRecordIterator(Iterator<PixelPosRecord> inputIterator) {
            super(inputIterator);
        }

        @Override
        protected Record getRecord(PixelPosRecord input) throws IOException {
            return extract(input.record, input.pixelPos);
        }
    }

    private static class PixelPosRecord {
        private final PixelPos pixelPos;
        private final Record record;

        private PixelPosRecord(PixelPos pixelPos, Record record) {
            this.pixelPos = pixelPos;
            this.record = record;
        }

        @Override
        public String toString() {
            return "PixelPosRecord{" +
                    "pixelPos=" + pixelPos +
                    ", record=" + record +
                    '}';
        }
    }

    private static class PixelTimeProvider {

        private final double startMJD;
        private final double deltaMJD;

        static PixelTimeProvider create(Product product) {
            final ProductData.UTC startTime = product.getStartTime();
            final ProductData.UTC endTime = product.getEndTime();
            final int rasterHeight = product.getSceneRasterHeight();
            if (startTime != null && endTime != null && rasterHeight > 1) {
                return new PixelTimeProvider(startTime.getMJD(),
                                             (endTime.getMJD() - startTime.getMJD()) / (rasterHeight - 1));
            } else {
                return null;
            }
        }

        private PixelTimeProvider(double startMJD, double deltaMJD) {
            this.startMJD = startMJD;
            this.deltaMJD = deltaMJD;
        }

        public Date getTime(PixelPos pixelPos) {
            return getUTC(pixelPos).getAsDate();
        }

        private ProductData.UTC getUTC(PixelPos pixelPos) {
            return new ProductData.UTC(startMJD + Math.floor(pixelPos.y) * deltaMJD);
        }
    }
}
