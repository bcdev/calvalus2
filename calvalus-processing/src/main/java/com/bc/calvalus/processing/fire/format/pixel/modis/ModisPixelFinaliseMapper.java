package com.bc.calvalus.processing.fire.format.pixel.modis;

import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import org.esa.snap.collocation.CollocateOp;
import org.esa.snap.collocation.ResamplingType;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;

public class ModisPixelFinaliseMapper extends PixelFinaliseMapper {

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
        return "MODIS";
    }

    @Override
    protected ClScaler getClScaler() {
        return cl -> cl;
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
