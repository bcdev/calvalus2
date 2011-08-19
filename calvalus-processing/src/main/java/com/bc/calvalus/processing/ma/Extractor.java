package com.bc.calvalus.processing.ma;

import com.bc.ceres.core.Assert;
import org.esa.beam.framework.datamodel.*;

import java.io.IOException;
import java.util.*;

/**
 * Extracts an output record.
 *
 * @author MarcoZ
 * @author Norman
 */
public class Extractor implements RecordSource {
    public static final Float FLOAT_NAN = Float.NaN;
    public static final Integer INTEGER_NAN = 0;
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

        final Iterable<Record> records = input.getRecords();
        return new Iterable<Record>() {
            @Override
            public Iterator<Record> iterator() {
                if (sortInputByPixelYX) {
                    return new RecordIterator2(getInputRecordsSortedByPixelYX(records).iterator());
                } else {
                    return new RecordIterator1(records.iterator());
                }
            }
        };
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

    public Record extract(Record inputRecord) throws Exception {
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
        final PixelPos pixelPos = product.getGeoCoding().getPixelPos(referenceRecord.getCoordinate(), null);
        if (pixelPos.isValid() && product.containsPixel(pixelPos)) {
            return pixelPos;
        }
        return null;
    }

    private long getMinReferenceTime(Record referenceRecord) {
        return referenceRecord.getTime().getTime() - getMaxTimeDifferenceInMillis();
    }

    private long getMaxReferenceTime(Record referenceRecord) {
        return referenceRecord.getTime().getTime() + getMaxTimeDifferenceInMillis();
    }

    private long getMaxTimeDifferenceInMillis() {
        return Math.round(config.getMaxTimeDifference() * 60 * 60 * 1000);
    }

    private Record extract(Record inputRecord, PixelPos pixelPos) throws IOException {
        final float[] floatSample = new float[1];
        final int[] intSample = new int[1];

        final Object[] values = new Object[getHeader().getAttributeNames().length];

        int index = 0;
        if (config.isCopyInput()) {
            Object[] inputValues = inputRecord.getAttributeValues();
            System.arraycopy(inputValues, 0, values, 0, inputValues.length);
            index = inputValues.length;
        }

        GeoPos geoPos = product.getGeoCoding().canGetGeoPos() ? product.getGeoCoding().getGeoPos(pixelPos, null) : null;
        Date time = pixelTimeProvider != null ? pixelTimeProvider.getTime(pixelPos) : null;

        // field "source_name"
        values[index++] = product.getName();
        // field "pixel_x"
        values[index++] = pixelPos.x;
        // field "pixel_y"
        values[index++] = pixelPos.y;
        // field "pixel_lat"
        values[index++] = geoPos != null ? geoPos.lat : Float.NaN;
        // field "pixel_lon"
        values[index++] = geoPos != null ? geoPos.lon : Float.NaN;
        // field "pixel_time"
        values[index++] = time != null ? getHeader().getTimeFormat().format(time) : null;

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

        return new DefaultRecord(geoPos, time, values);
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


    private class RecordIterator1 implements Iterator<Record> {
        private final Iterator<Record> inputIterator;
        private Record next;
        private boolean nextValid;

        public RecordIterator1(Iterator<Record> inputIterator) {
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

    private class RecordIterator2 implements Iterator<Record> {
        private final Iterator<PixelPosRecord> inputIterator;

        public RecordIterator2(Iterator<PixelPosRecord> inputIterator) {
            this.inputIterator = inputIterator;
        }

        @Override
        public boolean hasNext() {
            return inputIterator.hasNext();
        }

        @Override
        public Record next() {
            final PixelPosRecord record = inputIterator.next();
            try {
                return extract(record.inputRecord, record.pixelPos);
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

    private static class PixelPosRecord {
        private final PixelPos pixelPos;
        private final Record inputRecord;

        private PixelPosRecord(PixelPos pixelPos, Record inputRecord) {
            this.pixelPos = pixelPos;
            this.inputRecord = inputRecord;
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
