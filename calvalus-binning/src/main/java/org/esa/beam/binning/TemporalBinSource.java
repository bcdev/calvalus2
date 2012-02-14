package org.esa.beam.binning;

import com.bc.calvalus.binning.TemporalBin;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Norman Fomferra
 */
public interface TemporalBinSource {

    /**
     * Opens the bin source.
     *
     * @return The number of parts.
     * @throws IOException If an I/O error occurred.
     */
    int open() throws IOException;

    /**
     * @param index The part index.
     * @return The parts.
     * @throws IOException If an I/O error occurred.
     */
    Iterator<? extends TemporalBin> getPart(int index) throws IOException;

    /**
     * Informs the source that the given part has been processed.
     *
     * @param index The part index.
     * @param part  The part instance returned by {@code getPart(index)}.
     * @throws IOException If an I/O error occurred.
     */
    void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException;

    /**
     * Closes the bin source.
     *
     * @throws IOException If an I/O error occurred.
     */
    void close() throws IOException;
}
