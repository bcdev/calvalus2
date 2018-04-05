package com.bc.calvalus.processing.fire.format.pixel.modis;

import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import org.esa.snap.core.datamodel.Product;

public class ModisPixelFinaliseMapper extends PixelFinaliseMapper {

    @Override
    protected Product collocateWithSource(Product lcProduct, Product source) {
        // no reprojection necessary
        return lcProduct;
    }

    @Override
    protected ClScaler getClScaler() {
        // no scaling necessary
        return cl -> cl;
    }

    @Override
    protected String createBaseFilename(String year, String month, String version, String areaString) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MODIS-AREA_%s-%s", year, month, areaString.split(";")[0], version);
    }
}
