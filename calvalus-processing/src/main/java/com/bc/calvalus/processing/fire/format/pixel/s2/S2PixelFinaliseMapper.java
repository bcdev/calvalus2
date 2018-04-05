package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import org.esa.snap.collocation.CollocateOp;
import org.esa.snap.collocation.ResamplingType;
import org.esa.snap.core.datamodel.Product;

public class S2PixelFinaliseMapper extends PixelFinaliseMapper {

    @Override
    protected Product collocateWithSource(Product lcProduct, Product source) {
        CollocateOp collocateOp = new CollocateOp();
        collocateOp.setMasterProduct(source);
        collocateOp.setSlaveProduct(lcProduct);
        collocateOp.setResamplingType(ResamplingType.NEAREST_NEIGHBOUR);

        return collocateOp.getTargetProduct();
    }

    @Override
    protected ClScaler getClScaler() {
        return cl -> cl * 100;
    }

    @Override
    public String createBaseFilename(String year, String month, String version, String areaString) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MSI-AREA_%s-%s", year, month, areaString.split(";")[1], version);
    }
}
