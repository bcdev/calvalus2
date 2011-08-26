package com.bc.calvalus.processing.ma;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator used in various implementations of the {@link RecordSource} interface.
 *
 * @author Norman
 */
public abstract class RecordIterator implements Iterator<Record> {
    private Record next;
    private boolean nextValid;

    public RecordIterator() {
    }

    @Override
    public boolean hasNext() {
        ensureValidNext();
        return next != null;
    }

    @Override
    public Record next() {
        ensureValidNext();
        if (next == null) {
            throw new NoSuchElementException();
        }
        nextValid = false;
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void ensureValidNext() {
        if (!nextValid) {
            next = getNextRecord();
            nextValid = true;
        }
    }

    /**
     * @return The next record, or {@code null} if there is no next record.
     */
    protected abstract Record getNextRecord();
}
