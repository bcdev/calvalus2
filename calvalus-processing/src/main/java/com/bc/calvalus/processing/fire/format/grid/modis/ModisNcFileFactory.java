package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

class ModisNcFileFactory extends NcFileFactory {

    @Override
    protected String getTitle() {
        return "Fire_cci Gridded MODIS Burned Area product";
    }

    @Override
    protected String getSensorGlobalAttribute() {
        return "MODIS";
    }

    @Override
    protected String getPlatformGlobalAttribute() {
        return "Terra";
    }


    @Override
    protected String getSource() {
        return "MODIS MCD14ML Collection 5, ESA CCI Land Cover dataset v1.6.1";
    }

    @Override
    protected String getDoi() {
        return "tbd";
    }

    @Override
    protected void addSensorVar(NetcdfFileWriter ncFile) {
        Variable burnableAreaFractionVar = ncFile.addVariable(null, "burnable_area_fraction", DataType.FLOAT, "time lat lon");
        burnableAreaFractionVar.addAttribute(new Attribute("units", "1"));
        burnableAreaFractionVar.addAttribute(new Attribute("long_name", "fraction of burnable area"));
        burnableAreaFractionVar.addAttribute(new Attribute("comment", "The fraction of burnable area is the fraction of the cell that corresponds to vegetated land covers that could burn. The land cover classes are those from CCI Land Cover, http://www.esa-landcover-cci.org/"));
    }
}
