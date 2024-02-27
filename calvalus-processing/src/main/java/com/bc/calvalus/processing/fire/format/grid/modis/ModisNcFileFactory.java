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
        return "MODIS MOD09GQ Collection 6, MODIS MOD09GA Collection 6, MODIS MCD14ML Collection 6, ESA CCI Land Cover dataset v2.0.7";
    }

    protected boolean addNumPatches() {
        return true;
    }

    @Override
    protected boolean addCrsVar() {
        return false;
    }

    @Override
    protected String getCFVersion() {
        return "CF-1.6";
    }

    @Override
    protected boolean addFormatVersion() {
        return false;
    }

    @Override
    protected String getCommentMetadata() {
        return "These data were produced as part of the ESA Fire_cci programme.";
    }

    @Override
    protected String getContactMetadata() {
        return null;
    }

    @Override
    protected String getProjectMetadata() {
        return "Climate Change Initiative - European Space Agency";
    }

    @Override
    protected boolean addKeyVariables() {
        return false;
    }

    @Override
    protected String getBoundsVarName() {
        return "nv";
    }

    @Override
    protected DataType getLatLonType() {
        return DataType.FLOAT;
    }

    @Override
    protected String getLatBoundsName() {
        return "lat_bnds";
    }

    @Override
    protected String getLonBoundsName() {
        return "lon_bnds";
    }

    @Override
    protected String getTimeBoundsName() {
        return "time_bnds";
    }

    @Override
    protected boolean addValidRanges() {
        return false;
    }

    @Override
    protected String getObsAreaComment() {
        return "The fraction of the total burnable area in the cell (fraction_of_burnable_area variable of this file) that was observed during the time interval, and was not marked as unsuitable/not observable. The latter refers to the area where it was not possible to obtain observational burned area information for the whole time interval because of the lack of input data (non-existing data for that location and period).";
    }

    @Override
    protected String getSummary() {
        return "The grid product is the result of summing up burned area pixels and their attributes, as extracted from their original sinusoidal projection, within each cell of 0.25 degrees in a regular grid covering the whole Earth in monthly composites. The attributes stored are sum of burned area, standard error, fraction of burnable area, fraction of observed area, number of patches and the burned area for 18 land cover classes of Land Cover CCI.";
    }

    @Override
    protected String getDoi() {
        return "10.5285/3628cb2fdba443588155e15dee8e5352";
    }

    @Override
    protected void addBurnableAreaFractionVar(NetcdfFileWriter ncFile) {
        Variable burnableAreaFractionVar = ncFile.addVariable(null, "fraction_of_burnable_area", DataType.FLOAT, "time lat lon");
        burnableAreaFractionVar.addAttribute(new Attribute("units", "1"));
        burnableAreaFractionVar.addAttribute(new Attribute("long_name", "fraction of burnable area"));
        burnableAreaFractionVar.addAttribute(new Attribute("comment", "The fraction of burnable area is the fraction of the cell that corresponds to vegetated land covers that could burn. The land cover classes are those from CCI Land Cover, http://www.esa-landcover-cci.org/"));
    }
}
