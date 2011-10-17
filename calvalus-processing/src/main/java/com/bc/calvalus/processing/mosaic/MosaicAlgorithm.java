package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.binning.VariableContext;

/**
* An algorithm used for mosaicking.
 *
 * @author MarcoZ
*/
interface MosaicAlgorithm {

    void process(float[][] samples);

    float[][] getResult();

    void setVariableContext(VariableContext variableContext);

    String[] getOutputFeatures();
}
