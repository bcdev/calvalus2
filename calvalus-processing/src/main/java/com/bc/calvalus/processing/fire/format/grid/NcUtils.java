package com.bc.calvalus.processing.fire.format.grid;

import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

class NcUtils {

    static NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException {
        NetcdfFileWriter ncFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, filename);

        ncFile.addDimension(null, "vegetation_class", 18);
        ncFile.addDimension(null, "lat", 720);
        ncFile.addDimension(null, "lon", 1440);
        ncFile.addDimension(null, "nv", 2);
        ncFile.addDimension(null, "strlen", 150);
        ncFile.addUnlimitedDimension("time");

        Variable latVar = ncFile.addVariable(null, "lat", DataType.FLOAT, "lat");
        latVar.addAttribute(new Attribute("units", "degree_north"));
        latVar.addAttribute(new Attribute("standard_name", "latitude"));
        latVar.addAttribute(new Attribute("long_name", "latitude"));
        latVar.addAttribute(new Attribute("bounds", "lat_bnds"));
        ncFile.addVariable(null, "lat_bnds", DataType.FLOAT, "lat nv");
        Variable lonVar = ncFile.addVariable(null, "lon", DataType.FLOAT, "lon");
        lonVar.addAttribute(new Attribute("units", "degree_east"));
        lonVar.addAttribute(new Attribute("standard_name", "longitude"));
        lonVar.addAttribute(new Attribute("long_name", "longitude"));
        lonVar.addAttribute(new Attribute("bounds", "lon_bnds"));
        ncFile.addVariable(null, "lon_bnds", DataType.FLOAT, "lon nv");
        Variable timeVar = ncFile.addVariable(null, "time", DataType.DOUBLE, "time");
        timeVar.addAttribute(new Attribute("units", "days since 1970-01-01 00:00:00"));
        timeVar.addAttribute(new Attribute("standard_name", "time"));
        timeVar.addAttribute(new Attribute("long_name", "time"));
        timeVar.addAttribute(new Attribute("bounds", "time_bnds"));
        timeVar.addAttribute(new Attribute("calendar", "standard"));
        ncFile.addVariable(null, "time_bnds", DataType.FLOAT, "time nv");
        Variable vegetationClassVar = ncFile.addVariable(null, "vegetation_class", DataType.INT, "vegetation_class");
        vegetationClassVar.addAttribute(new Attribute("units", "1"));
        vegetationClassVar.addAttribute(new Attribute("long_name", "vegetation class number"));
        Variable vegetationClassNameVar = ncFile.addVariable(null, "vegetation_class_name", DataType.CHAR, "vegetation_class strlen");
        vegetationClassNameVar.addAttribute(new Attribute("units", "1"));
        vegetationClassNameVar.addAttribute(new Attribute("long_name", "vegetation class name"));
        Variable burnedAreaVar = ncFile.addVariable(null, "burned_area", DataType.FLOAT, "time lat lon");
        burnedAreaVar.addAttribute(new Attribute("units", "m2"));
        burnedAreaVar.addAttribute(new Attribute("standard_name", "burned_area"));
        burnedAreaVar.addAttribute(new Attribute("long_name", "total burned_area"));
        burnedAreaVar.addAttribute(new Attribute("cell_methods", "time: sum"));
        Variable standardErrorVar = ncFile.addVariable(null, "standard_error", DataType.FLOAT, "time lat lon");
        standardErrorVar.addAttribute(new Attribute("units", "m2"));
        standardErrorVar.addAttribute(new Attribute("long_name", "standard error of the estimation of burned area"));
        Variable observedAreaFractionVar = ncFile.addVariable(null, "observed_area_fraction", DataType.FLOAT, "time lat lon");
        observedAreaFractionVar.addAttribute(new Attribute("units", "1"));
        observedAreaFractionVar.addAttribute(new Attribute("long_name", "fraction of observed area"));
        observedAreaFractionVar.addAttribute(new Attribute("comment", "The fraction of observed area is 1 minus the area fraction of unsuitable/not observable pixels in a given grid. The latter refers to the area where it was not possible to obtain observational burned area information for the whole time interval because of lack of input data (non existing images for that location and period), cloud cover, haze or pixels that fell below the quality thresholds of the algorithm."));
        Variable numberOfPatchesVar = ncFile.addVariable(null, "number_of_patches", DataType.FLOAT, "time lat lon");
        numberOfPatchesVar.addAttribute(new Attribute("units", "1"));
        numberOfPatchesVar.addAttribute(new Attribute("long_name", "number of burn patches"));
        numberOfPatchesVar.addAttribute(new Attribute("comment", "Number of contiguous groups of burned pixels."));
        Variable burnedAreaInVegClassVar = ncFile.addVariable(null, "burned_area_in_vegetation_class", DataType.FLOAT, "time vegetation_class lat lon");
        burnedAreaInVegClassVar.addAttribute(new Attribute("units", "m2"));
        burnedAreaInVegClassVar.addAttribute(new Attribute("long_name", "burned area in vegetation class"));
        burnedAreaInVegClassVar.addAttribute(new Attribute("cell_methods", "time: sum"));
        burnedAreaInVegClassVar.addAttribute(new Attribute("comment", "Burned area by land cover classes; land cover classes are from CCI Land Cover, http://www.esa-landcover-cci.org/"));

        addGroupAttributes(version, ncFile, timeCoverageStart, timeCoverageEnd, numberOfDays);
        ncFile.create();
        return ncFile;
    }

    private static void addGroupAttributes(String version, NetcdfFileWriter ncFile, String timeCoverageStart, String timeCoverageEnd, int timeCoverageDuration) {
        ncFile.addGroupAttribute(null, new Attribute("title", "Fire_cci Gridded MERIS Burned Area product"));
        ncFile.addGroupAttribute(null, new Attribute("institution", "University of Alcala"));
        ncFile.addGroupAttribute(null, new Attribute("source", "MERIS FSG 1P, MODIS MCD14ML Collection 5, ESA CCI Land Cover dataset v1.6.1"));
        ncFile.addGroupAttribute(null, new Attribute("history", "Created on " + createTimeString(Instant.now())));
        ncFile.addGroupAttribute(null, new Attribute("references", "See www.esa-fire-cci.org"));
        ncFile.addGroupAttribute(null, new Attribute("tracking_id", UUID.randomUUID().toString()));
        ncFile.addGroupAttribute(null, new Attribute("Conventions", "CF-1.6"));
        ncFile.addGroupAttribute(null, new Attribute("product_version", version));
        ncFile.addGroupAttribute(null, new Attribute("summary", "The grid product is the result of summing up burned " +
                "area pixels within each cell of 0.25 degrees in a regular grid covering the whole Earth in biweekly " +
                "composites. The attributes stored are sum of burned area, standard error, observed area fraction, " +
                "number of patches and the burned area for 18 land cover classes of CCI_LC."));
        ncFile.addGroupAttribute(null, new Attribute("keywords", "Burned Area, Fire Disturbance, Climate Change, ESA, GCOS"));
        ncFile.addGroupAttribute(null, new Attribute("id", "doi:10.5285/D80636D4-7DAF-407E-912D-F5BB61C142FA"));
        ncFile.addGroupAttribute(null, new Attribute("naming_authority", "org.esa-fire-cci"));
        ncFile.addGroupAttribute(null, new Attribute("keywords_vocabulary", ""));
        ncFile.addGroupAttribute(null, new Attribute("cdm_data_type", "Grid"));
        ncFile.addGroupAttribute(null, new Attribute("comment", "These data were produced as part of the ESA Fire_cci programme."));
        ncFile.addGroupAttribute(null, new Attribute("date_created", createTimeString(Instant.now())));
        ncFile.addGroupAttribute(null, new Attribute("creator_name", "University of Alcala"));
        ncFile.addGroupAttribute(null, new Attribute("creator_url", "www.esa-fire-cci.org"));
        ncFile.addGroupAttribute(null, new Attribute("creator_email", "emilio.chuvieco@uah.es"));
        ncFile.addGroupAttribute(null, new Attribute("project", "ESA Climate Change Initiative - ECV Fire Disturbance (Fire_cci)"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_min", "-90"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_max", "90"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_min", "-180"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_max", "180"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_vertical_min", "0"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_vertical_max", "0"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_start", timeCoverageStart));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_end", timeCoverageEnd));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_duration", String.format("P%sD", "" + timeCoverageDuration)));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_resolution", "P01D"));
        ncFile.addGroupAttribute(null, new Attribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Metadata Convention"));
        ncFile.addGroupAttribute(null, new Attribute("licence", "ESA CCI Data Policy: free and open access"));
        ncFile.addGroupAttribute(null, new Attribute("platform", "Envisat"));
        ncFile.addGroupAttribute(null, new Attribute("sensor", "MERIS"));
        ncFile.addGroupAttribute(null, new Attribute("spatial_resolution", "0.25°"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_units", "degrees_east"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_units", "degrees_north"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_resolution", "0.25"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_resolution", "0.25"));
    }

    static String createFilename(String year, String month, String version, boolean firstHalf) {
        return String.format("%s%s%s-ESACCI-L4_FIRE-BA-MERIS-f%s.nc", year, month, firstHalf ? "07" : "22", version);
    }

    static String createTimeString(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyyMMddTHHmmssZ").withZone(ZoneId.systemDefault()).format(instant);
    }
}
