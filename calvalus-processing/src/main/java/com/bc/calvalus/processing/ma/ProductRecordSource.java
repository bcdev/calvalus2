package com.bc.calvalus.processing.ma;

import com.bc.ceres.core.Assert;
import com.bc.jexp.ParseException;
import org.esa.beam.framework.datamodel.Product;

import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

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

    private final Iterable<PixelPosProvider.PixelPosRecord> pixelPosRecords;
    private final boolean empty;
    private final PixelExtractor pixelExtractor;
    private final MAConfig config;

    public ProductRecordSource(Product product,
                               Header referenceRecordHeader,
                               Iterable<PixelPosProvider.PixelPosRecord> pixelPosRecords,
                               MAConfig config,
                               AffineTransform transform) {


        Assert.notNull(product, "product");
        Assert.notNull(referenceRecordHeader, "referenceRecordHeader");
        Assert.notNull(pixelPosRecords, "pixelPosRecords");
        Assert.notNull(config, "config");
        Assert.notNull(transform, "transform");

        if (!referenceRecordHeader.hasLocation()) {
            throw new IllegalArgumentException("Input records don't have locations.");
        }

        this.pixelPosRecords = pixelPosRecords;
        this.empty = shallApplyTimeCriterion(config) && !canApplyTimeCriterion(referenceRecordHeader);
        this.config = config;

        pixelExtractor = new PixelExtractor(referenceRecordHeader,
                                            product,
                                            this.config.getMacroPixelSize(),
                                            this.config.getGoodPixelExpression(),
                                            this.config.getMaxTimeDifference(),
                                            this.config.getCopyInput(),
                                            transform);
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

        return new Iterable<Record>() {
            @Override
            public Iterator<Record> iterator() {
                return new PixelPosRecordGenerator(pixelPosRecords.iterator());
            }
        };
    }

    @Override
    public String getTimeAndLocationColumnDescription() {
        return "BEAM product format";
    }

    private static boolean shallApplyTimeCriterion(MAConfig config) {
        Double maxTimeDifference = config.getMaxTimeDifference();
        return maxTimeDifference != null && maxTimeDifference > 0;
    }

    private static boolean canApplyTimeCriterion(Header referenceHeader) {
        return referenceHeader.hasTime();
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

    private class PixelPosRecordGenerator extends OutputRecordGenerator<PixelPosProvider.PixelPosRecord> {

        public PixelPosRecordGenerator(Iterator<PixelPosProvider.PixelPosRecord> inputIterator) {
            super(inputIterator);
        }

        @Override
        protected Record getRecord(PixelPosProvider.PixelPosRecord input) throws IOException {
            return pixelExtractor.extract(input.getRecord(), input.getPixelPos());
        }
    }


}
