package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

public class S2NcFileFactory extends NcFileFactory {

    @Override
    protected void addSensorVar(NetcdfFileWriter ncFile) {
        Variable burnableAreaFractionVar = ncFile.addVariable(null, "burnable_area_fraction", DataType.FLOAT, "time lat lon");
        burnableAreaFractionVar.addAttribute(new Attribute("units", "1"));
        burnableAreaFractionVar.addAttribute(new Attribute("long_name", "fraction of burnable area"));
        burnableAreaFractionVar.addAttribute(new Attribute("comment", "The fraction of burnable area is the fraction of the cell that corresponds to vegetated land covers that could burn. The land cover classes are those from CCI Land Cover, http://www.esa-landcover-cci.org/"));
    }

    @Override
    protected String getSource() {
        return "MSI L1C, MODIS MCD14ML Collection 5, ESA CCI Land Cover dataset v1.6.1";
    }

    @Override
    protected String getDoi() {
        return "FILL_IN";
    }

    @Override
    protected String getPlatformGlobalAttribute() {
        return "Sentinel-2";
    }

    @Override
    protected String getSensorGlobalAttribute() {
        return "MSI";
    }

    @Override
    protected String getTitle() {
        return "Fire_cci Gridded MSI Burned Area product";
    }
}
