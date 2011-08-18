package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;

/**
 * Matches an input (reference) record with EO data by extracting an output record.
 *
 * @author Norman
 */
public class Matcher extends Extractor {

    @Override
    protected PixelPos getValidPixelPos(Record referenceRecord) {
        PixelPos validPixelPos = super.getValidPixelPos(referenceRecord);

        return validPixelPos;
    }

    public Matcher(Product product, MAConfig config) {
        super(product, config);
    }
}
