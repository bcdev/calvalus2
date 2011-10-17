package com.bc.calvalus.processing.mosaic;

/**
* An algorithm used for mosaicking.
 *
 * @author MarcoZ
*/
interface MosaicAlgorithm {

    void process(float[][] samples);

    float[][] getResult();
}
