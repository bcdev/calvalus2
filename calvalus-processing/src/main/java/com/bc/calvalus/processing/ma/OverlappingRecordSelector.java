package com.bc.calvalus.processing.ma;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * A {@link RecordSelector selector} implementation to get rid of overlapping match-ups.
 *
 * @author Marco Peters
 */
class OverlappingRecordSelector implements RecordSelector {

    private final ProductRecordSource.PixelPosRecordFactory pixelPosRecordFactory;
    private ArrayList<ProductRecordSource.PixelPosRecord> workRecords;
    private ArrayList<Record> selectedRecords;
    private int macroPixelSize;

    OverlappingRecordSelector(int macroPixelSize, ProductRecordSource.PixelPosRecordFactory pixelPosRecordFactory) {
        this.macroPixelSize = macroPixelSize;
        this.pixelPosRecordFactory = pixelPosRecordFactory;
        workRecords = new ArrayList<ProductRecordSource.PixelPosRecord>();
        selectedRecords = new ArrayList<Record>();
    }

    @Override
    public Iterable<Record> select(Iterable<Record> records) {
        for (Record record : records) {
            ProductRecordSource.PixelPosRecord pixelPosRecord = pixelPosRecordFactory.create(record);

            boolean addToWorkRecords = true;
            if (!workRecords.isEmpty()) {
                Iterator<ProductRecordSource.PixelPosRecord> workRecordsIterator = workRecords.iterator();
                while (workRecordsIterator.hasNext()) {
                    ProductRecordSource.PixelPosRecord workRecord = workRecordsIterator.next();
                    boolean overlapping = isOverlapping(pixelPosRecord, workRecord, macroPixelSize);
                    if (overlapping) {
                        if (workRecord.getTimeDifference() > pixelPosRecord.getTimeDifference()) {
                            workRecordsIterator.remove();
                        } else {
                            addToWorkRecords = false;
                            break;
                        }
                    }
                }
            }
            if (addToWorkRecords) {
                workRecords.add(pixelPosRecord);
            }
            float minYRow = pixelPosRecord.getPixelPos().y - macroPixelSize;
            Iterator<ProductRecordSource.PixelPosRecord> workRecordsIterator = workRecords.iterator();
            while (workRecordsIterator.hasNext()) {
                ProductRecordSource.PixelPosRecord workRecord = workRecordsIterator.next();
                if (workRecord.getPixelPos().y < minYRow) {
                    workRecordsIterator.remove();
                    selectedRecords.add(workRecord.record);
                }
            }
        }
        for (ProductRecordSource.PixelPosRecord workRecord : workRecords) {
            selectedRecords.add(workRecord.record);
        }
        return selectedRecords;
    }

    private static boolean isOverlapping(ProductRecordSource.PixelPosRecord record1, ProductRecordSource.PixelPosRecord record2, int macroPixelSize) {
        float xDist = Math.abs(record1.getPixelPos().x - record2.getPixelPos().x);
        float yDist = Math.abs(record1.getPixelPos().y - record2.getPixelPos().y);
        return yDist < macroPixelSize && xDist < macroPixelSize;
    }


}
