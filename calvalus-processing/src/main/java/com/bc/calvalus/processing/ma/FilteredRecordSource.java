package com.bc.calvalus.processing.ma;

import java.util.Iterator;

/**
 * A record source that outputs a filtered input record source using a {@link RecordFilter}.
 *
 * @author Norman
 */
public class FilteredRecordSource implements RecordSource {
    private final RecordSource input;
    private final RecordFilter filter;

    public FilteredRecordSource(RecordSource input, RecordFilter filter) {
        this.input = input;
        this.filter = filter;
    }

    @Override
    public Header getHeader() {
        return input.getHeader();
    }

    @Override
    public Iterable<Record> getRecords() throws Exception {
        final Iterator<Record> inputIt = input.getRecords().iterator();
        return new Iterable<Record>() {
            @Override
            public Iterator<Record> iterator() {
                return new RecIt(inputIt);
            }
        };
    }

    private class RecIt extends RecordIterator {

        private final Iterator<Record> inputIt;

        private RecIt(Iterator<Record> inputIt) {
            this.inputIt = inputIt;
        }

        @Override
        protected Record getNextRecord() {
            while (inputIt.hasNext()) {
                Record next = inputIt.next();
                if (filter.accept(next)) {
                    return next;
                }
            }
            return null;
        }
    }
}
