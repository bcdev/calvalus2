package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class L3ProductionTypeTest {
    @Test
    public void testCreateBinningConfig() throws ProductionException {
        ProductionRequest productionRequest = createValidL3ProductionRequest();
        L3Config l3Config = L3ProductionType.createL3Config(productionRequest);
        assertNotNull(l3Config);
        assertEquals(4320, l3Config.getBinningContext().getBinningGrid().getNumRows());
        assertEquals("NOT INVALID", l3Config.getVariableContext().getMaskExpr());
        float[] superSamplingSteps = l3Config.getSuperSamplingSteps();
        assertEquals(1, superSamplingSteps.length);
        assertEquals(0.5f, superSamplingSteps[0], 1e-5);
        assertEquals(3, l3Config.getVariableContext().getVariableCount());
        assertEquals("a", l3Config.getVariableContext().getVariableName(0));
        assertEquals("b", l3Config.getVariableContext().getVariableName(1));
        assertEquals("c", l3Config.getVariableContext().getVariableName(2));
        BinManager binManager = l3Config.getBinningContext().getBinManager();
        assertEquals(3, binManager.getAggregatorCount());
        assertEquals("MIN_MAX", binManager.getAggregator(0).getName());
        assertEquals(2, binManager.getAggregator(0).getOutputFeatureNames().length);
        assertEquals(-999.9F, binManager.getAggregator(0).getOutputFillValue(), 1E-5F);
    }

    @Test
    public void testGeoRegion() throws ProductionException {
        ProductionRequest productionRequest = createValidL3ProductionRequest();
        Geometry regionOfInterest = productionRequest.getRegionGeometry();
        assertNotNull(regionOfInterest);
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", regionOfInterest.toString());
    }

    /*
    @Test
    public void testCreateFormatterConfig() throws ProductionException {
        L3FormatterConfig formatterConfig = L3ProductionType.
        assertNotNull(formatterConfig);
        assertEquals("NetCDF", formatterConfig.getOutputFormat());
        assertEquals(new File("/opt/tomcat/webapps/calvalus/staging/ewa-A25F/L3_2010-06-03-2010-06-05.nc").getPath(), formatterConfig.getOutputFile());
        assertEquals("Product", formatterConfig.getOutputType());
    }
    */

    @Test
    public void testComputeBinningGridRowCount() {
        assertEquals(2160, L3ProductionType.computeBinningGridRowCount(9.28));
        assertEquals(2160 * 2, L3ProductionType.computeBinningGridRowCount(9.28 / 2));
        assertEquals(2160 / 2, L3ProductionType.computeBinningGridRowCount(9.28 * 2));
        assertEquals(66792, L3ProductionType.computeBinningGridRowCount(0.3)); //MERIS FR equivalent
    }

    static ProductionRequest createValidL3ProductionRequest() {
        return new ProductionRequest("calvalus-level3", "ewa",
                                     // GeneralLevel 3 parameters
                                     "inputProductSetId", "MER_RR__1P/r03/2010",
                                     "outputFormat", "NetCDF",
                                     "autoStaging", "true",
                                     "processorBundleName", "beam",
                                     "processorBundleVersion", "4.9-SNAPSHOT",
                                     "processorName", "BandMaths",
                                     "processorParameters", "<!-- no params -->",
                                     // Special Level 3 parameters
                                     "inputVariables", "a, b, c",
                                     "maskExpr", "NOT INVALID",
                                     "aggregator", "MIN_MAX",
                                     "weightCoeff", "1.0",
                                     "minDate", "2010-06-03",
                                     "maxDate", "2010-06-05",
                                     "periodCount", "1",
                                     "periodLength", "3",
                                     "minLon", "5",
                                     "maxLon", "25",
                                     "minLat", "50",
                                     "maxLat", "60",
                                     "resolution", "4.64",
                                     "fillValue", "-999.9",
                                     "superSampling", "1"
        );
    }

}
