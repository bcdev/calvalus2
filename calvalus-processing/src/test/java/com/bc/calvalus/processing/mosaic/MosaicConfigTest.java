package com.bc.calvalus.processing.mosaic;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class MosaicConfigTest {
    @Test
    public void testToXml() throws Exception {
        final MosaicConfig config = new MosaicConfig("com.bc.calvalus.processing.mosaic.landcover.LcSDR8MosaicAlgorithm",
                                                     "status == 1 and not nan(refl_2)",
                                                     new String[]{"pixel_classif_flags", "refl_2", "ndvi"},
                                                     new String[]{"status"},
                                                     new String[]{"pixel_classif_flags.F_CLOUD ? 4 : pixel_classif_flags.F_CLOUD_SHADOW ? 5 : pixel_classif_flags.F_SNOW_ICE ? 3 : pixel_classif_flags.F_LAND ? 1 : 2"});
        final String xml = config.toXml();
        assertEquals("<parameters>\n" +
                             "  <algorithmName>com.bc.calvalus.processing.mosaic.landcover.LcSDR8MosaicAlgorithm</algorithmName>\n" +
                             "  <validMaskExpression>status == 1 and not nan(refl_2)</validMaskExpression>\n" +
                             "  <variableNames>pixel_classif_flags,refl_2,ndvi</variableNames>\n" +
                             "  <virtualVariableNames>status</virtualVariableNames>\n" +
                             "  <virtualVariableExpr>pixel_classif_flags.F_CLOUD ? 4 : pixel_classif_flags.F_CLOUD_SHADOW ? 5 : pixel_classif_flags.F_SNOW_ICE ? 3 : pixel_classif_flags.F_LAND ? 1 : 2</virtualVariableExpr>\n" +
                             "</parameters>", xml);
    }
}
