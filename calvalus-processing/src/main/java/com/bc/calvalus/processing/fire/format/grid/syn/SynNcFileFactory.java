package com.bc.calvalus.processing.fire.format.grid.syn;

import com.bc.calvalus.processing.fire.format.grid.NcFileFactory;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

public class SynNcFileFactory extends NcFileFactory {

    @Override
    protected String getTitle() {
        return "Sentinel-3 SYN Burned Area Grid product, version 1.0";
    }

    @Override
    protected String getSensorGlobalAttribute() {
        return "OLCI, SLSTR";
    }

    @Override
    protected String getPlatformGlobalAttribute() {
        return "Sentinel-3A, Sentinel-3B";
    }

    @Override
    protected String getCommentMetadata() {
        return "These data were produced as part of the Climate Change Initiative Programme, Fire Disturbance ECV.";
    }

    @Override
    protected String getKeywordsVocabulary() {
        return "none";
    }

    @Override
    protected String getSource() {
        return "Sentinel-3 Synergy (SYN) product, derived from OLCI+SLSTR Surface Reflectance, VIIRS VNP14IMGML thermal anomalies, C3S Land Cover dataset v2.1.1";
    }

    @Override
    protected String getDoi() {
        return "10.5285/3aaaaf94813e48f18f2b83242a8dacbe";
    }

    @Override
    protected String getReference() {
        return "See https://climate.esa.int/en/projects/fire/";
    }

    @Override
    protected String getNamingAuthority() {
        return "org.esa-cci";
    }

    @Override
    protected String getCreatorUrl() {
        return "https://geogra.uah.es/gita/en/";
    }

    @Override
    protected String getContactMetadata() {
        return "mlucrecia.pettinari@uah.es";
    }

    @Override
    protected String getProjectMetadata() {
        return "Climate Change Initiative - European Space Agency";
    }

    @Override
    protected String getLicense() {
        return "ESA CCI Data Policy: free and open access";
    }

    @Override
    protected String getBurnedAreaInVegClassComment() {
        return "Burned area by land cover classes; land cover classes are from C3S Land Cover, https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-land-cover?tab=overview";
    }

    @Override
    protected void addBurnableAreaFractionVar(NetcdfFileWriter ncFile) {
        Variable burnableAreaFractionVar = ncFile.addVariable(null, "fraction_of_burnable_area", DataType.FLOAT, "time lat lon");
        burnableAreaFractionVar.addAttribute(new Attribute("units", "1"));
        burnableAreaFractionVar.addAttribute(new Attribute("long_name", "fraction of burnable area"));
        burnableAreaFractionVar.addAttribute(new Attribute("comment", "The fraction of burnable area is the fraction of the cell that corresponds to vegetated land covers that could burn. The land cover classes are those from C3S Land Cover, https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-land-cover?tab=overview"));
        burnableAreaFractionVar.addAttribute(new Attribute("valid_range", Array.factory(DataType.FLOAT, new int[]{2}, new float[]{0, 1})));
    }

    @Override
    protected String getSummary() {
        return "The grid product is the result of summing burned area pixels and their attributes within each cell of " +
                "0.25x0.25 degrees in a regular grid covering the whole Earth in monthly composites. The attributes " +
                "stored are sum of burned area, standard error, fraction of burnable area, fraction of observed area, " +
                "and the burned area for 18 land cover classes of C3S Land Cover.";
    }
}
