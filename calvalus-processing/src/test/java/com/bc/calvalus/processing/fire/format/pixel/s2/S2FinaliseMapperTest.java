package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.processing.fire.format.S2Strategy;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static com.bc.calvalus.processing.fire.format.pixel.s2.S2FinaliseMapper.TILE_SIZE;

public class S2FinaliseMapperTest {

    @Ignore
    @Test
    public void testRemap() throws Exception {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        Product lcProduct = ProductIO.readProduct("c:\\ssd\\2010.nc");
        lcProduct.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        String baseFilename = S2FinaliseMapper.createBaseFilename("2016", "02", "fv4.2", new S2Strategy().getArea("AREA_24"));
        Product product = S2FinaliseMapper.remap(new File("C:\\ssd\\L3_2016-02-01_2016-02-29.nc"), baseFilename, lcProduct);

        ProductIO.writeProduct(product, "C:\\ssd\\" + baseFilename + "_test256.tif", BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
    }

    @Test
    public void name() throws Exception {
        String maxDate = String.format("%s-%s-%02d", 2005, 12, 5);
        System.out.println(maxDate);

    }
}