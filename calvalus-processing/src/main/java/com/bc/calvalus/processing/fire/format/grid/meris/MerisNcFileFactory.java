package com.bc.calvalus.processing.fire.format.grid.meris;

import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import ucar.nc2.NetcdfFileWriter;

class MerisNcFileFactory extends NcFileFactory {

    @Override
    protected String getTitle() {
        return "Fire_cci Gridded MERIS Burned Area product";
    }

    @Override
    protected String getSensorGlobalAttribute() {
        return "MERIS, MODIS";
    }

    @Override
    protected String getPlatformGlobalAttribute() {
        return "Envisat, Terra, Aqua";
    }


    @Override
    protected String getSource() {
        return "MERIS FSG 1P, MODIS MCD14ML Collection 5, ESA CCI Land Cover dataset v1.6.1";
    }

    @Override
    protected String getDoi() {
        return "doi:10.5285/D80636D4-7DAF-407E-912D-F5BB61C142FA";
    }

    @Override
    protected void addSensorVar(NetcdfFileWriter ncFile) {
        // nothing to do
    }
}
