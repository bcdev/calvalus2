package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

public class S2NcFileFactory extends NcFileFactory {

    @Override
    protected void addBurnableAreaFractionVar(NetcdfFileWriter ncFile) {
        Variable burnableAreaFractionVar = ncFile.addVariable(null, "fraction_of_burnable_area", DataType.FLOAT, "time lat lon");
        burnableAreaFractionVar.addAttribute(new Attribute("units", "1"));
        burnableAreaFractionVar.addAttribute(new Attribute("long_name", "fraction of burnable area"));
        burnableAreaFractionVar.addAttribute(new Attribute("comment", "The fraction of burnable area is the fraction of the cell that corresponds to vegetated land covers that could burn. The land cover classes are those from C3S land cover map v2.1.1 of 2018"));
    }

    @Override
    protected String getSource() {
        return "MSI L1C, VIIRS FIRMS (VNP14IMGTML), C3S land cover map v2.1.1 of 2018";
    }

    @Override
    protected String getDoi() {
        return "10.5285/01b00854797d44a59d57c8cce08821eb";
    }

    @Override
    protected String getPublicationDate() {
        return "2021-04-30";
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
        return "ESA Fire_cci Small Fire Database (SFD) Burned Area Grid product";
    }

    @Override
    protected String getSummary() {
        return "The grid product is the result of summing up burned area pixels within each cell of 0.05 degrees in a regular grid covering the whole Earth in monthly composites. For this product, only sub-Saharan Africa has burned area information. The attributes stored are sum of burned area, standard error, fraction of burnable area, fraction of observed area, and the burned area for 18 individual land cover classes.";
    }

    @Override
    protected String getBurnedAreaInVegClassComment() {
        return "Burned area by land cover classes; land cover classes are from the C3S Land Cover map v2.1.1 of 2018.";
    }

    protected String getCommentMetadata() {
        return "These data were produced as part of the ESA CCI programme.";
    }

    protected String getKeywordsMetadata() {
        return "Burned Area, Fire Disturbance, Climate Change, ESA, GCOS";
    }

    protected String getProjectMetadata() {
        return "ESA Fire_cci";
    }

    protected String getContactMetadata() {
        return "https://climate.esa.int/en/projects/fire";
    }

}
