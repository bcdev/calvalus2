package com.bc.calvalus.processing.l3.binprocessing;

import org.esa.beam.binning.VariableContext;

/**
 * A descriptor for a bin processor.
 */
public interface BinProcessorDescriptor {
    String getName();

    BinProcessorConfig createBinProcessorConfig();

    BinProcessor createBinProcessor(VariableContext varCtx, BinProcessorConfig binProcessorConfig);

}
