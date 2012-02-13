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
     * @return The number of parts.
     */
    int open(Outputter outputter) throws IOException;

    /**
     * @param index The part index.
     * @return The parts.
     */
    Iterator<? extends TemporalBin> getPart(int index)throws IOException;

    /**
     * Informs the source that the given part has been processed.
     * @param index  The part index.
     */
    void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException;

    /**
     * Closes the bin source.
     */
    void close()throws IOException;
}
