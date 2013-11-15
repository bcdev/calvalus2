package com.bc.calvalus.processing.ma;

import com.bc.ceres.core.Assert;
import com.bc.jexp.ParseException;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * A special record source that generates its output records from a {@link Product} and an input record source.
 *
 * @author MarcoZ
 * @author Norman
 */
public class ProductRecordSource implements RecordSource {

    public static final String SOURCE_NAME_ATT_NAME = "source_name";

    public static final String PIXEL_X_ATT_NAME = "pixel_x";
    public static final String PIXEL_Y_ATT_NAME = "pixel_y";
    public static final String PIXEL_LAT_ATT_NAME = "pixel_lat";
    public static final String PIXEL_LON_ATT_NAME = "pixel_lon";
    public static final String PIXEL_TIME_ATT_NAME = "pixel_time";
    public static final String PIXEL_MASK_ATT_NAME = "pixel_mask";

    private final RecordSource referenceRecordSource;
    private final boolean empty;
    private final PixelExtractor pixelExtractor;
    private final MAConfig config;

    public ProductRecordSource(Product product, RecordSource referenceRecordSource, MAConfig config) {

        Assert.notNull(product, "product");
        Assert.notNull(referenceRecordSource, "referenceRecordSource");
        Assert.notNull(config, "config");
        if (!referenceRecordSource.getHeader().hasLocation()) {
            throw new IllegalArgumentException("Input records don't have locations.");
        }

        this.referenceRecordSource = referenceRecordSource;
        this.empty = shallApplyTimeCriterion(config) && !canApplyTimeCriterion(referenceRecordSource);

        this.config = config;
        pixelExtractor = new PixelExtractor(referenceRecordSource.getHeader(),
                                            product,
                                            this.config.getMacroPixelSize(),
                                            this.config.getGoodPixelExpression(),
                                            this.config.getMaxTimeDifference(),
                                            this.config.getCopyInput());
    }

    @Override
    public Header getHeader() {
        return pixelExtractor.getHeader();
    }

    /**
     * Extracts output records from input records.
     *
     * @return Extracted records.
     */
    @Override
    public Iterable<Record> getRecords() throws Exception {
        if (empty) {
            return Collections.emptyList();
        }

        final Iterable<Record> inputRecords = referenceRecordSource.getRecords();
        return new Iterable<Record>() {
            @Override
            public Iterator<Record> iterator() {
                return new PixelPosRecordGenerator(getInputRecordsSortedByPixelYX(inputRecords, pixelExtractor).iterator());
            }
        };
    }

    @Override
    public String getTimeAndLocationColumnDescription() {
        return "BEAM product format";
    }

    public static RecordTransformer createShiftTransformer(Header header, Rectangle inputRect) {
        List<String> attributeNames = Arrays.asList(header.getAttributeNames());
        final int xAttributeIndex = attributeNames.indexOf(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX + PIXEL_X_ATT_NAME);
        final int yAttributeIndex = attributeNames.indexOf(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX + PIXEL_Y_ATT_NAME);
        int xOffset = 0;
        int yOffset = 0;
        if (inputRect != null) {
            xOffset = inputRect.x;
            yOffset = inputRect.y;
        }
        return new ProductOffsetTransformer(xAttributeIndex, yAttributeIndex, xOffset, yOffset);
    }

    public static RecordTransformer createAggregator(Header header, MAConfig config) {
        String pixelMaskAttributeName = PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX + PIXEL_MASK_ATT_NAME;
        final int pixelMaskAttributeIndex = Arrays.asList(header.getAttributeNames()).indexOf(pixelMaskAttributeName);
        return new RecordAggregator(pixelMaskAttributeIndex, config.getFilteredMeanCoeff());
    }

    public static RecordFilter createRecordFilter(Header header, MAConfig config) {
        if (shallApplyGoodRecordExpression(config)) {
            final String goodRecordExpression = config.getGoodRecordExpression();
            final RecordFilter recordFilter;
            try {
                recordFilter = ExpressionRecordFilter.create(header, goodRecordExpression);
            } catch (ParseException e) {
                throw new IllegalStateException("Illegal configuration: goodRecordExpression is invalid: " + e.getMessage(), e);
            }
            return recordFilter;
        } else {
            return new RecordFilter() {
                @Override
                public boolean accept(Record record) {
                    return true;
                }
            };
        }
    }

    public RecordSelector createRecordSelector() {
        if (config.getFilterOverlapping()) {
            PixelPosRecordFactory pixelPosRecordFactory = new PixelPosRecordFactory(getHeader());
            return new OverlappingRecordSelector(config.getMacroPixelSize(), pixelPosRecordFactory, getHeader());
        } else {
            return new RecordSelector() {
                @Override
                public Iterable<Record> select(Iterable<Record> aggregatedRecords) {
                    return aggregatedRecords;
                }
            };
        }
    }

    private static boolean shallApplyGoodRecordExpression(MAConfig config) {
        String goodRecordExpression = config.getGoodRecordExpression();
        return goodRecordExpression != null && !goodRecordExpression.isEmpty();
    }

    private static boolean shallApplyTimeCriterion(MAConfig config) {
        Double maxTimeDifference = config.getMaxTimeDifference();
        return maxTimeDifference != null && maxTimeDifference > 0;
    }

    private static boolean canApplyTimeCriterion(RecordSource input) {
        return input.getHeader().hasTime();
    }

    private Iterable<PixelPosRecord> getInputRecordsSortedByPixelYX(Iterable<Record> inputRecords, PixelExtractor pixelExtractor) {
        ArrayList<PixelPosRecord> pixelPosList = new ArrayList<PixelPosRecord>(128);
        for (Record inputRecord : inputRecords) {
            final PixelPos pixelPos = pixelExtractor.getPixelPos(inputRecord);
            if (pixelPos != null) {
                pixelPosList.add(new PixelPosRecord(pixelPos, inputRecord));
            }
        }
        PixelPosRecord[] records = pixelPosList.toArray(new PixelPosRecord[pixelPosList.size()]);
        Arrays.sort(records, new YXComparator());
        return Arrays.asList(records);
    }

    static class YXComparator implements Comparator<PixelPosRecord> {

        @Override
        public int compare(PixelPosRecord o1, PixelPosRecord o2) {
            // because in the code we get the pixel-pos by the geo-pos it can happen that pixels on the same line are
            // wrongly sorted because of the little difference in y-direction.
            // that's the reason why we compare y-position only as integer
            int y1 = (int) o1.pixelPos.y;
            int y2 = (int) o2.pixelPos.y;
            if (y1 < y2) {
                return -2;
            } else if (y1 > y2) {
                return 2;
            }
            float x1 = o1.pixelPos.x;
            float x2 = o2.pixelPos.x;
            if (x1 < x2) {
                return -1;
            } else if (x1 > x2) {
                return 1;
            }
            return 0;
        }
    }

    private abstract class OutputRecordGenerator<T> extends RecordIterator {

        private final Iterator<T> inputIterator;

        public OutputRecordGenerator(Iterator<T> inputIterator) {
            this.inputIterator = inputIterator;
        }

        @Override
        public Record getNextRecord() {
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
    }

    private class PixelPosRecordGenerator extends OutputRecordGenerator<PixelPosRecord> {

        public PixelPosRecordGenerator(Iterator<PixelPosRecord> inputIterator) {
            super(inputIterator);
        }

        @Override
        protected Record getRecord(PixelPosRecord input) throws IOException {
            return pixelExtractor.extract(input.record, input.pixelPos);
        }
    }

    static class PixelPosRecordFactory {

        private final int xAttributeIndex;
        private final int yAttributeIndex;
        private final int timeAttributeIndex;

        PixelPosRecordFactory(Header header) {
            xAttributeIndex = header.getAttributeIndex(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX + PIXEL_X_ATT_NAME);
            yAttributeIndex = header.getAttributeIndex(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX + PIXEL_Y_ATT_NAME);
            timeAttributeIndex = header.getAttributeIndex(PIXEL_TIME_ATT_NAME);
        }

        PixelPosRecord create(Record record) {
            Object[] attributeValues = record.getAttributeValues();
            float[] xAttributeValue = ((AggregatedNumber) attributeValues[xAttributeIndex]).data;
            float[] yAttributeValue = ((AggregatedNumber) attributeValues[yAttributeIndex]).data;
            int xPos = (int) xAttributeValue[xAttributeValue.length / 2];
            int yPos = (int) yAttributeValue[yAttributeValue.length / 2];

            // can be null !!!!
            Date eoTime = (Date) attributeValues[timeAttributeIndex];
            Date insituTime = record.getTime();
            long timeDiff = -1;
            if (insituTime != null && eoTime != null) {
                timeDiff = Math.abs(insituTime.getTime() - eoTime.getTime());
            }
            return new PixelPosRecord(new PixelPos(xPos, yPos), record, timeDiff);
        }

    }

    static class PixelPosRecord {

        final PixelPos pixelPos;
        final Record record;
        final long timeDiff;

        PixelPosRecord(PixelPos pixelPos, Record record) {
            this(pixelPos, record, -1);
        }

        PixelPosRecord(PixelPos pixelPos, Record record, long timeDiff) {
            this.record = record;
            this.pixelPos = pixelPos;
            this.timeDiff = timeDiff;
        }

        PixelPos getPixelPos() {
            return pixelPos;
        }

        long getTimeDifference() {
            return timeDiff;
        }

        @Override
        public String toString() {
            return "PixelPosRecord{" +
                   "pixelPos=" + pixelPos +
                   ", record=" + record +
                   ", timeDiff=" + timeDiff +
                   '}';
        }
    }

}
