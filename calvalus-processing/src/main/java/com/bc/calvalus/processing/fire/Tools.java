package com.bc.calvalus.processing.fire;

import com.bc.ceres.core.PrintWriterProgressMonitor;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;

import java.io.IOException;

public class Tools {

    public static void main(String[] args) throws IOException {
//        convertToTif();
    }

    private static void convertToTif() throws IOException {
        Product nc = ProductIO.readProduct("d:\\workspace\\c3s\\C3S-LC-L4-LCCS-Map-300m-P1Y-2016-v2.1.1.nc");
        nc.removeBand(nc.getBand("processed_flag"));
        nc.removeBand(nc.getBand("current_pixel_state"));
        nc.removeBand(nc.getBand("observation_count"));
        nc.removeBand(nc.getBand("change_count"));
        ProductIO.writeProduct(nc, "d:\\workspace\\c3s\\C3S-LC-L4-LCCS-Map-300m-P1Y-2016-v2.1.1.tif", "GeoTIFF-BigTIFF", new PrintWriterProgressMonitor(System.out));
    }


}
