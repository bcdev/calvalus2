package com.bc.calvalus.processing.mosaic;

import org.esa.snap.binning.VariableContext;

import java.io.IOException;

/**
* An algorithm used for mosaicking.
 *
 * @author MarcoZ
*/
public interface MosaicAlgorithm {

    void initTemporal(TileIndexWritable tileIndex) throws IOException;

    void processTemporal(float[][] samples);

    float[][] getTemporalResult();

    void setVariableContext(VariableContext variableContext);

    String[] getTemporalFeatures();

    float[][] getOutputResult(float[][] temporalData);

    String[] getOutputFeatures();

    MosaicProductFactory getProductFactory();
}
