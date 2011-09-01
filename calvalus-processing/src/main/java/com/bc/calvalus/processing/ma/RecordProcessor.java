package com.bc.calvalus.processing.ma;

import java.io.IOException;

/**
 * Processes record values.
 *
 * @author Norman
 */
public interface RecordProcessor {
    void processHeaderRecord(Object[] headerValues) throws IOException;
    void processDataRecord(int recordIndex, Object[] recordValues) throws IOException;
    void finalizeRecordProcessing(int numRecords) throws IOException;
}
