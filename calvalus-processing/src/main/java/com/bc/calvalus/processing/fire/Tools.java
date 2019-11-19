package com.bc.calvalus.processing.fire;

import com.bc.ceres.core.PrintWriterProgressMonitor;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;

import java.io.IOException;

public class Tools {

    public static void main(String[] args) throws IOException {
//        convertToTif();
        merge(args[0], args[1], args[2]);
    }

    private static void convertToTif() throws IOException {
        Product nc = ProductIO.readProduct("d:\\workspace\\c3s\\C3S-LC-L4-LCCS-Map-300m-P1Y-2018-v2.1.1.nc");
        nc.removeBand(nc.getBand("processed_flag"));
        nc.removeBand(nc.getBand("current_pixel_state"));
        nc.removeBand(nc.getBand("observation_count"));
        nc.removeBand(nc.getBand("change_count"));
        ProductIO.writeProduct(nc, "d:\\workspace\\c3s\\C3S-LC-L4-LCCS-Map-300m-P1Y-2018-v2.1.1.tif", "GeoTIFF-BigTIFF", new PrintWriterProgressMonitor(System.out));
    }

    private static void merge(String idepixProductPath, String icorProductPath, String targetPath) throws IOException {
        Product idepixProduct = ProductIO.readProduct(idepixProductPath);
        Product icorProduct = ProductIO.readProduct(icorProductPath);

        Product result = new Product("IdepixIcorProduct", "IdepixIcorProduct", idepixProduct.getSceneRasterWidth(), idepixProduct.getSceneRasterHeight());
        ProductUtils.copyGeoCoding(idepixProduct, result);
        ProductUtils.copyBand("pixel_classif_flags", idepixProduct, result, true);
        ProductUtils.copyBand("band_12", icorProduct, result, true);
        ProductUtils.copyBand("band_14", icorProduct, result, true);

        ProductIO.writeProduct(result, targetPath, "NetCDF4-CF", new PrintWriterProgressMonitor(System.out));
    }

}
