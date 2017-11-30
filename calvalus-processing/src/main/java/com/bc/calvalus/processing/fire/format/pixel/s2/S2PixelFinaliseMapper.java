package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;

public class S2PixelFinaliseMapper extends PixelFinaliseMapper {

    @Override
    protected Product collocateWithSource(Product lcProduct, Product source) {
        ReprojectionOp reprojectionOp = new ReprojectionOp();
        reprojectionOp.setSourceProduct(lcProduct);
        reprojectionOp.setSourceProduct("collocateWith", source);
        reprojectionOp.setParameterDefaultValues();
        return reprojectionOp.getTargetProduct();
    }

    @Override
    protected ClScaler getClScaler() {
        return cl -> cl * 100;
    }
}
