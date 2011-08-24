package com.bc.calvalus.processing.ma;

/**
 * A filter for records.
 *
 * @author Norman
 */
public interface RecordFilter {
    boolean accept(Record record);
}
