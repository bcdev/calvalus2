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
    protected void addBurnableAreaFractionVar(NetcdfFileWriter ncFile) {
        // nothing to do
    }

    @Override
    protected String getSummary() {
        return "The grid product is the result of summing up burned " +
                "area pixels within each cell of 0.25 degrees in a regular grid covering the whole Earth in biweekly " +
                "composites. The attributes stored are sum of burned area, standard error, observed area fraction, " +
                "number of patches and the burned area for 18 land cover classes of CCI_LC.";
    }
}
