package com.bc.calvalus.processing.l3.binprocessing;

import org.esa.beam.binning.Bin;
import org.esa.beam.binning.BinManager;
import org.esa.beam.binning.Observation;
import org.esa.beam.binning.SpatialBin;
import org.esa.beam.binning.support.ObservationImpl;

/**
 * A processor for processing bins.
 */
public abstract class BinProcessor {

    abstract public float[] process(float[] featureValues);
}
