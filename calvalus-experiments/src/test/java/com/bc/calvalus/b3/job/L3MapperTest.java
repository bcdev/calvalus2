package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinManagerImpl;
import com.bc.calvalus.b3.BinningContext;
import com.bc.calvalus.b3.BinningContextImpl;
import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.SpatialBin;
import com.bc.calvalus.b3.SpatialBinProcessor;
import com.bc.calvalus.b3.SpatialBinner;
import com.bc.calvalus.b3.VariableContextImpl;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.TiePointGeoCoding;
import org.esa.beam.framework.datamodel.TiePointGrid;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class L3MapperTest {
    @Test
    public void testThatProductMustHaveAGeoCoding() {
        BinningContext ctx = createValidCtx();

        try {
            MySpatialBinProcessor mySpatialBinProcessor = new MySpatialBinProcessor();
            L3Mapper.processProduct(new Product("p", "t", 32, 256), ctx, new SpatialBinner(ctx, mySpatialBinProcessor));
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void testProcessProduct() {

        BinningContext ctx = createValidCtx();
        Product product = new Product("p", "t", 32, 256);
        final TiePointGrid lat = new TiePointGrid("lat", 2, 2, 0f, 0f, 32f, 256f,
                                                  new float[]{+40f, +40f, -40f, -40f});
        final TiePointGrid lon = new TiePointGrid("lon", 2, 2, 0f, 0f, 32f, 256f,
                                                  new float[]{-80f, +80f, -80f, +80f});
        product.addTiePointGrid(lat);
        product.addTiePointGrid(lon);
        product.setGeoCoding(new TiePointGeoCoding(lat, lon));
        product.setPreferredTileSize(32, 16);

        MySpatialBinProcessor mySpatialBinProcessor = new MySpatialBinProcessor();
        L3Mapper.processProduct(product, ctx, new SpatialBinner(ctx, mySpatialBinProcessor));
        assertEquals(32 * 256, mySpatialBinProcessor.numObs);
    }

    private static BinningContext createValidCtx() {
        VariableContextImpl variableContext = new VariableContextImpl();
        variableContext.setMaskExpr("!invalid");
        variableContext.defineVariable("invalid", "0");
        variableContext.defineVariable("a", "2.4");
        variableContext.defineVariable("b", "1.8");

        IsinBinningGrid binningGrid = new IsinBinningGrid(6);
        BinManager binManager = new BinManagerImpl(new AggregatorAverage(variableContext, "a"),
                                                   new AggregatorAverage(variableContext, "b"));

        return new BinningContextImpl(binningGrid, variableContext, binManager);
    }

    private static class MySpatialBinProcessor implements SpatialBinProcessor {
        int numObs;

        @Override
        public void processSpatialBinSlice(BinningContext ctx, List<SpatialBin> spatialBins) throws Exception {
            // System.out.println("spatialBins = " + Arrays.toString(spatialBins.toArray()));
            for (SpatialBin spatialBin : spatialBins) {
                assertEquals(2.4f, spatialBin.getProperty(0), 0.01f);  // mean of a
                assertEquals(1.8f, spatialBin.getProperty(2), 0.01f);  // mean of b
                numObs += spatialBin.getNumObs();
            }
        }
    }
}
