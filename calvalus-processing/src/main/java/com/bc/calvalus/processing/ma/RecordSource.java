package com.bc.calvalus.processing.ma;

/**
 * A factory for record sources.
 *
 * @author Norman
 */
public interface RecordSource {
    /**
     * @return The header of the record source.
     */
    Header getHeader();

    /**
     * Gets the records.
     *
     * @return The records.
     * @throws Exception if any error occurs.
     */
    Iterable<Record> getRecords() throws Exception;
}
