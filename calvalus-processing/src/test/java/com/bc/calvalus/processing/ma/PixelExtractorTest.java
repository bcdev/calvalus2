package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.Product;
import org.junit.Test;

/**
 * @author MarcoZ
 * @author Norman
 */
public class PixelExtractorTest {

    @Test(expected = NullPointerException.class)
    public void testExtractDoesNotAcceptNullRecord() throws Exception {
        PixelExtractor extractor = new PixelExtractor(new DefaultHeader(), new Product("A", "B", 2, 2), 1, null, null, true);
        extractor.extract(null);
    }

    @Test
    public void testHeaderNameReplacement() {
        // 1. After aggregation, a single name "chl" shall generate 3 new names: "chl.mean", "chl.sigma", "chl.n"
        //    Actually these could be expressions that are evaluated in the context of the record.

        // String headerName = "chl-->chl.mean;chl.sigma;chl.n";

        // 2. Or we simply expand header names marked with, e.g. '@': "@chl" would
        //    then always produce 3 new names: "chl.mean", "chl.sigma", "chl.n"

        // String headerName = "@chl";
    }
}
