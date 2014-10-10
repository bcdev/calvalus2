package com.bc.calvalus.processing.ma;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * A {@link RecordSelector selector} implementation to get rid of overlapping match-ups.
 *
 * @author Marco Peters
 */
class OverlappingRecordSelector implements RecordSelector {

    static final String EXCLUSION_REASON_OVERLAPPING = "OVERLAPPING";

    private final PixelPosProvider.PixelPosRecordFactory pixelPosRecordFactory;
    private ArrayList<PixelPosProvider.PixelPosRecord> workRecords;
    private ArrayList<Record> selectedRecords;
    private int macroPixelSize;
    private final int exclusionIndex;

    OverlappingRecordSelector(int macroPixelSize, PixelPosProvider.PixelPosRecordFactory pixelPosRecordFactory, Header header) {
        this.macroPixelSize = macroPixelSize;
        this.pixelPosRecordFactory = pixelPosRecordFactory;
        workRecords = new ArrayList<>();
        selectedRecords = new ArrayList<>();
        exclusionIndex = header.getAnnotationIndex(DefaultHeader.ANNOTATION_EXCLUSION_REASON);
    }

    @Override
    public Iterable<Record> select(Iterable<Record> records) {
        for (Record record : records) {
            String reason = (String) record.getAnnotationValues()[exclusionIndex];
            if (!reason.isEmpty()) {
                selectedRecords.add(record);
            } else {
                PixelPosProvider.PixelPosRecord pixelPosRecord = pixelPosRecordFactory.create(record);

                boolean addToWorkRecords = true;
                if (!workRecords.isEmpty()) {
                    for (int i = 0; i < workRecords.size(); i++) {
                        PixelPosProvider.PixelPosRecord workRecord = workRecords.get(i);
                        boolean overlapping = isOverlapping(pixelPosRecord, workRecord, macroPixelSize);
                        if (overlapping) {
                            if (workRecord.getTimeDifference() > pixelPosRecord.getTimeDifference()) {
                                workRecord.getRecord().getAnnotationValues()[exclusionIndex] = EXCLUSION_REASON_OVERLAPPING;
                                workRecords.set(i, workRecord);
                            } else {
                                addToWorkRecords = false;
                                break;
                            }
                        }
                    }
                }
                if (addToWorkRecords) {
                    workRecords.add(pixelPosRecord);
                } else {
                    pixelPosRecord.getRecord().getAnnotationValues()[exclusionIndex] = EXCLUSION_REASON_OVERLAPPING;
                    workRecords.add(pixelPosRecord);
                }
                float minYRow = pixelPosRecord.getPixelPos().y - macroPixelSize;
                Iterator<PixelPosProvider.PixelPosRecord> workRecordsIterator = workRecords.iterator();
                while (workRecordsIterator.hasNext()) {
                    PixelPosProvider.PixelPosRecord workRecord = workRecordsIterator.next();
                    if (workRecord.getPixelPos().y < minYRow) {
                        workRecordsIterator.remove();
                        selectedRecords.add(workRecord.getRecord());
                    }
                }
            }
        }
        for (PixelPosProvider.PixelPosRecord workRecord : workRecords) {
            selectedRecords.add(workRecord.getRecord());
        }
        return selectedRecords;
    }

    private static boolean isOverlapping(PixelPosProvider.PixelPosRecord record1, PixelPosProvider.PixelPosRecord record2, int macroPixelSize) {
        float xDist = Math.abs(record1.getPixelPos().x - record2.getPixelPos().x);
        float yDist = Math.abs(record1.getPixelPos().y - record2.getPixelPos().y);
        return yDist < macroPixelSize && xDist < macroPixelSize;
    }


}
