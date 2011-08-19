package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.Product;

/**
 * Matches an input (reference) record with EO data by extracting an output record.
 *
 * @author Norman
 */
public class Matcher extends Extractor {

    public Matcher(Product product, MAConfig config) {
        super(product, config);
    }
}
