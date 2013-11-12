package com.bc.calvalus.processing.ma;

/**
 * @author Marco Peters
 */
public interface RecordSelector {

    Iterable<Record> select(Iterable<Record> aggregatedRecords);
}
