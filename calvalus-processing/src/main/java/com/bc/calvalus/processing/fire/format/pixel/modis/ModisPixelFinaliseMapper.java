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
}
