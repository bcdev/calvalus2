package com.bc.calvalus.processing.ma;

/**
 * A source for match-up records.
 *
 * @author Norman
 */
public interface RecordSourceSpi {
    RecordSource createRecordSource(MAConfig maConfig);
}
