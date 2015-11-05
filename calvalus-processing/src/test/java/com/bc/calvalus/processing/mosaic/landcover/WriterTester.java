package com.bc.calvalus.processing.mosaic.landcover;

import com.bc.calvalus.processing.mosaic.MosaicGrid;
import com.bc.calvalus.processing.mosaic.MosaicProductFactory;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.awt.Point;
import java.awt.Rectangle;
import java.text.SimpleDateFormat;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class WriterTester {
    private static SimpleDateFormat COMPACT_ISO_FORMAT = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInt("calvalus.mosaic.macroTileSize", 5);
        configuration.setInt("calvalus.mosaic.numTileY", 180);
        configuration.setInt("calvalus.mosaic.tileSize", 360);
        configuration.set("calvalus.minDate", "2009-01-08");
        configuration.set("calvalus.maxDate", "2009-01-14");
        final MosaicGrid mosaicGrid = MosaicGrid.create(configuration);
        final Rectangle macroTileRectangle = mosaicGrid.getMacroTileRectangle(5, 7);

        final MosaicProductFactory lcMosaicProductFactory = new LcL3Nc4MosaicProductFactory(new String[]{"sdr_test"});
        final Product testproduct = lcMosaicProductFactory.createProduct(configuration, 5, 7, macroTileRectangle);
        testproduct.setSceneGeoCoding(mosaicGrid.createMacroCRS(new Point(5, 7)));
        // fill with fake data
        final Band[] bands = testproduct.getBands();
        for (Band band : bands) {
            band.setData(ProductData.createInstance(band.getDataType(),
                                                    macroTileRectangle.width * macroTileRectangle.height));
        }

        ProductIO.writeProduct(testproduct, "./" + testproduct.getName() + ".nc", "NetCDF4-LC");
    }
}
