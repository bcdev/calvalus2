package com.bc.calvalus.processing.fire;

import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class FirePixelNcFactory {

    public NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays, int width, int height) throws IOException {
        NetcdfFileWriter ncFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4_classic, filename);

//        ncFile.addUnlimitedDimension("time");
        ncFile.addDimension(null, "time", 1);
        ncFile.addDimension(null, "bounds", 2);
        ncFile.addDimension(null, "lat", height);
        ncFile.addDimension(null, "lon", width);

        Variable latVar = ncFile.addVariable(null, "lat", DataType.FLOAT, "lat");
        latVar.addAttribute(new Attribute("units", "degree_north"));
        latVar.addAttribute(new Attribute("standard_name", "latitude"));
        latVar.addAttribute(new Attribute("long_name", "latitude"));
        latVar.addAttribute(new Attribute("bounds", "lat_bounds"));
        ncFile.addVariable(null, "lat_bounds", DataType.FLOAT, "lat bounds");

        Variable lonVar = ncFile.addVariable(null, "lon", DataType.FLOAT, "lon");
        lonVar.addAttribute(new Attribute("units", "degree_east"));
        lonVar.addAttribute(new Attribute("standard_name", "longitude"));
        lonVar.addAttribute(new Attribute("long_name", "longitude"));
        lonVar.addAttribute(new Attribute("bounds", "lon_bounds"));
        ncFile.addVariable(null, "lon_bounds", DataType.FLOAT, "lon bounds");

        Variable timeVar = ncFile.addVariable(null, "time", DataType.DOUBLE, "time");
        timeVar.addAttribute(new Attribute("units", "days since 1970-01-01 00:00:00"));
        timeVar.addAttribute(new Attribute("standard_name", "time"));
        timeVar.addAttribute(new Attribute("long_name", "time"));
        timeVar.addAttribute(new Attribute("bounds", "time_bounds"));
        timeVar.addAttribute(new Attribute("calendar", "standard"));
        ncFile.addVariable(null, "time_bounds", DataType.FLOAT, "time bounds");

        Variable burnedAreaVar = ncFile.addVariable(null, "JD", DataType.SHORT, "time lat lon");
        burnedAreaVar.addAttribute(new Attribute("units", "m2"));
        burnedAreaVar.addAttribute(new Attribute("standard_name", "burned_area"));
        burnedAreaVar.addAttribute(new Attribute("long_name", "total burned_area"));
        burnedAreaVar.addAttribute(new Attribute("cell_methods", "time: sum"));

        Variable confidenceLevelVar = ncFile.addVariable(null, "CL", DataType.BYTE, "time lat lon");
        confidenceLevelVar.addAttribute(new Attribute("units", "m2"));
        confidenceLevelVar.addAttribute(new Attribute("long_name", "standard error of the estimation of burned area"));

        Variable landCoverVar = ncFile.addVariable(null, "LC", DataType.BYTE, "time lat lon");
        landCoverVar.addAttribute(new Attribute("units", "m2"));
        landCoverVar.addAttribute(new Attribute("long_name", "tbd"));

        addGroupAttributes(filename, version, ncFile, timeCoverageStart, timeCoverageEnd, numberOfDays);
        ncFile.create();
        return ncFile;
    }

    private void addGroupAttributes(String filename, String version, NetcdfFileWriter ncFile, String timeCoverageStart, String timeCoverageEnd, int timeCoverageDuration) {
        ncFile.addGroupAttribute(null, new Attribute("title", "ECMWF C3S Gridded OLCI Burned Area product"));
        ncFile.addGroupAttribute(null, new Attribute("institution", "University of Alcala"));
        ncFile.addGroupAttribute(null, new Attribute("source", "ESA Sentinel-3 A+B OLCI FR, MODIS MCD14ML Collection 6, C3S Land Cover dataset v2.1.1"));
        ncFile.addGroupAttribute(null, new Attribute("history", "Created on " + createNiceTimeString(Instant.now())));
        ncFile.addGroupAttribute(null, new Attribute("references", "See https://climate.copernicus.eu/"));
        ncFile.addGroupAttribute(null, new Attribute("tracking_id", UUID.randomUUID().toString()));
        ncFile.addGroupAttribute(null, new Attribute("Conventions", "CF-1.7"));
        ncFile.addGroupAttribute(null, new Attribute("product_version", version));
        ncFile.addGroupAttribute(null, new Attribute("summary", "The grid product is the result of summing burned area pixels and their " +
                "attributes, within each cell of " +
                "0.25x0.25 degrees in a regular grid covering the whole Earth in monthly composites. The " +
                "attributes stored are sum of burned area, standard error, fraction of burnable area, " +
                "fraction of observed area, and the burned area for 18 land cover classes " +
                "of C3S Land Cover."));
        ncFile.addGroupAttribute(null, new Attribute("keywords", "Burned Area, Fire Disturbance, Climate Change, ESA, C3S, GCOS"));
        ncFile.addGroupAttribute(null, new Attribute("id", filename));
        ncFile.addGroupAttribute(null, new Attribute("naming_authority", "org.esa-cci"));
//        ncFile.addGroupAttribute(null, new Attribute("doi", getDoi()));
        ncFile.addGroupAttribute(null, new Attribute("keywords_vocabulary", "NASA Global Change Master Directory (GCMD) Science keywords"));
        ncFile.addGroupAttribute(null, new Attribute("cdm_data_type", "Grid"));
        ncFile.addGroupAttribute(null, new Attribute("comment", "These data were produced as part of the Copernicus Climate Change Service programme."));
        ncFile.addGroupAttribute(null, new Attribute("date_created", createTimeString(Instant.now())));
        ncFile.addGroupAttribute(null, new Attribute("creator_name", "University of Alcala"));
        ncFile.addGroupAttribute(null, new Attribute("creator_url", "https://www.uah.es/"));
        ncFile.addGroupAttribute(null, new Attribute("creator_email", "emilio.chuvieco@uah.es"));
        ncFile.addGroupAttribute(null, new Attribute("contact", "http://copernicus-support.ecmwf.int"));
        ncFile.addGroupAttribute(null, new Attribute("project", "EC C3S Fire Burned Area"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_min", "-90"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_max", "90"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_min", "-180"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_max", "180"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_vertical_min", "0"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_vertical_max", "0"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_start", timeCoverageStart));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_end", timeCoverageEnd));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_duration", "P1M"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_resolution", "P1M"));
        ncFile.addGroupAttribute(null, new Attribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Metadata Convention"));
        ncFile.addGroupAttribute(null, new Attribute("license", "EC C3S FIRE BURNED AREA Data Policy"));
        ncFile.addGroupAttribute(null, new Attribute("platform", "Sentinel-3"));
        ncFile.addGroupAttribute(null, new Attribute("sensor", "OLCI"));
        ncFile.addGroupAttribute(null, new Attribute("spatial_resolution", "0.25 degrees"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_units", "degrees_east"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_units", "degrees_north"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_resolution", "0.25"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_resolution", "0.25"));
    }

    static String createTimeString(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneId.systemDefault()).format(instant);
    }

    static String createNiceTimeString(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault()).format(instant);
    }

}
