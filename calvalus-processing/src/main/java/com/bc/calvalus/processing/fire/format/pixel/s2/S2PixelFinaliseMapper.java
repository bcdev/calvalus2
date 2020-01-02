package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import org.esa.snap.collocation.CollocateOp;
import org.esa.snap.collocation.ResamplingType;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;

public class S2PixelFinaliseMapper extends PixelFinaliseMapper {

    @Override
    public Product collocateWithSource(Product lcProduct, Product source) {
        CollocateOp collocateOp = new CollocateOp();
        collocateOp.setMasterProduct(source);
        collocateOp.setSlaveProduct(lcProduct);
        collocateOp.setResamplingType(ResamplingType.NEAREST_NEIGHBOUR);

        return collocateOp.getTargetProduct();
    }

    @Override
    protected String getCalvalusSensor() {
        return "S2";
    }

    @Override
    protected ClScaler getClScaler() {
        return cl -> {
            if (cl < 0.05) {
                return 0;
            } else if (cl <= 0.14) {
                return 50;
            } else if (cl <= 0.23) {
                return 60;
            } else if (cl <= 0.32) {
                return 70;
            } else if (cl <= 0.41) {
                return 80;
            } else if (cl <= 0.50) {
                return 90;
            } else {
                return 100;
            }
        };
    }

    @Override
    protected Band getLcBand(Product lcProduct) {
        return lcProduct.getBand("band_1");
    }

    @Override
    public String createBaseFilename(String year, String month, String version, String areaString) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MSI-AREA_%s-%s", year, month, areaString.split(";")[1], version);
    }

}
