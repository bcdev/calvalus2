package com.bc.calvalus.processing.l3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator for temporal bins originating from a Hadoop sequence file.
 *
 * @author Norman
 */
public final class SequenceFileBinIterator implements Iterator<L3TemporalBin> {
    private final SequenceFile.Reader reader;
    private L3TemporalBin temporalBin;
    private boolean mustRead;
    private boolean lastItemValid;
    private IOException ioException;

    SequenceFileBinIterator(SequenceFile.Reader reader) {
        this.reader = reader;
        mustRead = true;
        lastItemValid = true;
    }

    public IOException getIOException() {
        return ioException;
    }

    @Override
    public boolean hasNext() {
        maybeReadNext();
        return lastItemValid;
    }

    @Override
    public L3TemporalBin next() {
        maybeReadNext();
        if (!lastItemValid) {
            throw new NoSuchElementException();
        }
        mustRead = true;
        return temporalBin;
    }

    private void maybeReadNext() {
        if (mustRead && lastItemValid) {
            mustRead = false;
            try {
                LongWritable binIndex = new LongWritable();
                temporalBin = new L3TemporalBin();
                lastItemValid = reader.next(binIndex, temporalBin);
                if (lastItemValid) {
                    temporalBin.setIndex(binIndex.get());
                }
            } catch (IOException e) {
                ioException = e;
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public void remove() {
        throw new IllegalStateException("remove() not supported");
    }
}
