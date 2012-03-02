package com.bc.calvalus.processing.mosaic;

import org.esa.beam.binning.VariableContext;

import java.io.IOException;

/**
* An algorithm used for mosaicking.
 *
 * @author MarcoZ
*/
interface MosaicAlgorithm {

    void init(TileIndexWritable tileIndex) throws IOException;

    void process(float[][] samples);

    float[][] getResult();

    void setVariableContext(VariableContext variableContext);

    String[] getOutputFeatures();

    MosaicProductFactory getProductFactory();
}
