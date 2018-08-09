package com.bc.calvalus.processing.fire.format.pixel.modis;

import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;

public class ModisPixelFinaliseMapper extends PixelFinaliseMapper {

    @Override
    public Product collocateWithSource(Product lcProduct, Product source) {
        // no reprojection necessary
        return lcProduct;
    }

    @Override
    protected String getCalvalusSensor() {
        return "MODIS";
    }

    @Override
    protected ClScaler getClScaler() {
        return cl -> cl;
//        return cl -> {
//            if (cl < 5) {
//                return 0;
//            } else if (cl <= 14) {
//                return 50;
//            } else if (cl <= 23) {
//                return 60;
//            } else if (cl <= 32) {
//                return 70;
//            } else if (cl <= 41) {
//                return 80;
//            } else if (cl <= 50) {
//                return 90;
//            } else {
//                return 100;
//            }
//        };
    }

    @Override
    public String createBaseFilename(String year, String month, String version, String areaString) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MODIS-AREA_%s-%s", year, month, areaString.split(";")[0], version);
    }

    @Override
    protected Band getLcBand(Product lcProduct) {
        return lcProduct.getBand("band_1");
    }
}
