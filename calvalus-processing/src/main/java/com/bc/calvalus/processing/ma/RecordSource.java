package com.bc.calvalus.processing.ma;

/**
 * A factory for sources for match-up records.
 *
 * @author Norman
 */
public interface RecordSource {
    Iterable<Record> getRecords() throws Exception;
}
