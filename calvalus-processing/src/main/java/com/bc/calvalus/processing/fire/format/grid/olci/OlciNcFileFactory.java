package com.bc.calvalus.processing.fire.format.grid.olci;

import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

public class OlciNcFileFactory extends NcFileFactory {

    @Override
    protected String getTitle() {
        return "ECMWF C3S Gridded OLCI Burned Area product";
    }

    @Override
    protected String getSensorGlobalAttribute() {
        return "OLCI";
    }

    @Override
    protected String getPlatformGlobalAttribute() {
        return "Sentinel-3";
    }


    @Override
    protected String getSource() {
        return "ESA Sentinel-3 OLCI, C3S Land Cover dataset v2.1.1";
    }

    @Override
    protected String getDoi() {
        return "TBD";
    }

    @Override
    protected String getReference() {
        return "See https://climate.copernicus.eu/";
    }

    @Override
    protected String getNamingAuthority() {
        return "TBD";
    }

    @Override
    protected String getCreatorUrl() {
        return "https://climate.copernicus.eu/";
    }

    @Override
    protected String getLicense() {
        return "TBD";
    }

    @Override
    protected String getBurnedAreaInVegClassComment() {
        return "Burned area by land cover classes; land cover classes are from C3S Land Cover, https://climate.copernicus.eu/.";
    }

    @Override
    protected void addBurnableAreaFractionVar(NetcdfFileWriter ncFile) {
        Variable burnableAreaFractionVar = ncFile.addVariable(null, "fraction_of_burnable_area", DataType.FLOAT, "time lat lon");
        burnableAreaFractionVar.addAttribute(new Attribute("units", "1"));
        burnableAreaFractionVar.addAttribute(new Attribute("long_name", "fraction of burnable area"));
        burnableAreaFractionVar.addAttribute(new Attribute("comment", "The fraction of burnable area is the fraction of the cell that corresponds to vegetated land covers that could burn. The land cover classes are those from C3S Land Cover, https://climate.copernicus.eu/."));
    }

    @Override
    protected String getSummary() {
        return "The grid product is the result of summing up burned area pixels and their " +
                "attributes, as extracted from their original sinusoidal projection, within each cell of " +
                "0.25 degrees in a regular grid covering the whole Earth in monthly composites. The " +
                "attributes stored are sum of burned area, standard error, fraction of burnable area, " +
                "fraction of observed area, number of patches and the burned area for 18 land cover classes " +
                "of C3S Land Cover.";
    }
}
