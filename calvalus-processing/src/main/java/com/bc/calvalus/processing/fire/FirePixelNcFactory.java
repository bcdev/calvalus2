package com.bc.calvalus.processing.fire;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.awt.Rectangle;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.esa.snap.dataio.netcdf.NetCDF4Chunking;

public class FirePixelNcFactory {

    public NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numRowsGlobal, Rectangle xyBox) throws IOException {
        NetcdfFileWriter ncFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4_classic, filename, new NetCDF4Chunking());

        ncFile.addUnlimitedDimension("time");
        ncFile.addDimension(null, "bounds", 2);
        ncFile.addDimension(null, "lon", xyBox.width);
        ncFile.addDimension(null, "lat", xyBox.height);

        Variable julianDateVar = ncFile.addVariable(null, "JD", DataType.SHORT, "time lat lon");
        julianDateVar.addAttribute(new Attribute("long_name", "Date of the first detection"));
        julianDateVar.addAttribute(new Attribute("units", "Day of the year"));
        julianDateVar.addAttribute(new Attribute("comment", "Possible values: 0  when the pixel is not burned; 1 to 366 day of the first detection when the pixel is burned; -1 when the pixel is not observed in the month; -2 when pixel is not burnable: water bodies, bare areas, urban areas and permanent snow and ice."));
        julianDateVar.addAttribute(new Attribute("_ChunkSizes", Array.factory(DataType.INT, new int[]{3}, new int[]{1,1200,1200})));

        Variable confidenceLevelVar = ncFile.addVariable(null, "CL", DataType.BYTE, "time lat lon");
        confidenceLevelVar.addAttribute(new Attribute("long_name", "Confidence Level"));
        confidenceLevelVar.addAttribute(new Attribute("units", "percent"));
        confidenceLevelVar.addAttribute(new Attribute("comment", "Probability of detecting a pixel as burned. Possible values: 0 when the pixel is not observed in the month, or it is not burnable; 1 to 100 probability values when the pixel was observed. The closer to 100, the higher the confidence that the pixel is actually burned."));
        confidenceLevelVar.addAttribute(new Attribute("_ChunkSizes", Array.factory(DataType.INT, new int[]{3}, new int[]{1,1200,1200})));

        Variable landCoverVar = ncFile.addVariable(null, "LC", DataType.UBYTE, "time lat lon");
        landCoverVar.addAttribute(new Attribute("long_name", "Land cover of burned pixels"));
        landCoverVar.addAttribute(new Attribute("units", "Land cover code"));
        landCoverVar.addAttribute(new Attribute("comment", "Land cover of the burned pixel, extracted from the C3S LandCover v2.1.1 . N is the number of the land cover category in the reference map. It is only valid when JD > 0. Pixel value is 0 to N under the following codes: 10 = Cropland, rainfed; 20 = Cropland, irrigated or post-flooding; 30 = Mosaic cropland (>50%) / natural vegetation (tree, shrub, herbaceous cover) (<50%); 40 = Mosaic natural vegetation (tree, shrub, herbaceous cover) (>50%) / cropland (<50%); 50 = Tree cover, broadleaved, evergreen, closed to open (>15%); 60 = Tree cover, broadleaved, deciduous, closed to open (>15%); 70 = Tree cover, needleleaved, evergreen, closed to open (>15%); 80 = Tree cover, needleleaved, deciduous, closed to open (>15%); 90 = Tree cover, mixed leaf type (broadleaved and needleleaved); 100 = Mosaic tree and shrub (>50%) / herbaceous cover (<50%); 110 = Mosaic herbaceous cover (>50%) / tree and shrub (<50%); 120 = Shrubland; 130 = Grassland; 140 = Lichens and mosses; 150 = Sparse vegetation (tree, shrub, herbaceous cover) (<15%); 160 = Tree cover, flooded, fresh or brackish water; 170 = Tree cover, flooded, saline water; 180 = Shrub or herbaceous cover, flooded, fresh/saline/brackish water."));
        landCoverVar.addAttribute(new Attribute("_ChunkSizes", Array.factory(DataType.INT, new int[]{3}, new int[]{1,1200,1200})));

        Variable lonVar = ncFile.addVariable(null, "lon", DataType.DOUBLE, "lon");
        lonVar.addAttribute(new Attribute("standard_name", "longitude"));
        lonVar.addAttribute(new Attribute("long_name", "longitude"));
        lonVar.addAttribute(new Attribute("units", "degree_east"));
        lonVar.addAttribute(new Attribute("axis", "X"));
        lonVar.addAttribute(new Attribute("bounds", "lon_bounds"));
        lonVar.addAttribute(new Attribute("valid_min", "-180.0"));
        lonVar.addAttribute(new Attribute("valid_max", "180.0"));
        ncFile.addVariable(null, "lon_bounds", DataType.DOUBLE, "lon bounds");

        Variable latVar = ncFile.addVariable(null, "lat", DataType.DOUBLE, "lat");
        latVar.addAttribute(new Attribute("standard_name", "latitude"));
        latVar.addAttribute(new Attribute("long_name", "latitude"));
        latVar.addAttribute(new Attribute("axis", "Y"));
        latVar.addAttribute(new Attribute("units", "degree_north"));
        latVar.addAttribute(new Attribute("bounds", "lat_bounds"));
        latVar.addAttribute(new Attribute("valid_min", "-90.0"));
        latVar.addAttribute(new Attribute("valid_max", "90.0"));
        ncFile.addVariable(null, "lat_bounds", DataType.DOUBLE, "lat bounds");

        Variable timeVar = ncFile.addVariable(null, "time", DataType.DOUBLE, "time");
        timeVar.addAttribute(new Attribute("standard_name", "time"));
        timeVar.addAttribute(new Attribute("long_name", "time"));
        timeVar.addAttribute(new Attribute("axis", "T"));
        timeVar.addAttribute(new Attribute("calendar", "standard"));
        timeVar.addAttribute(new Attribute("units", "days since 1970-01-01 00:00:00"));
        timeVar.addAttribute(new Attribute("bounds", "time_bounds"));
        timeVar.addAttribute(new Attribute("calendar", "standard"));
        ncFile.addVariable(null, "time_bounds", DataType.DOUBLE, "time bounds");

        addGroupAttributes(filename, version, ncFile, timeCoverageStart, timeCoverageEnd, numRowsGlobal, xyBox);
        ncFile.create();
        return ncFile;
    }

    private void addGroupAttributes(String filename, String version, NetcdfFileWriter ncFile, String timeCoverageStart, String timeCoverageEnd, int numRowsGlobal, Rectangle xyBox) {
        Instant now = Instant.now();
        String uuid = UUID.randomUUID().toString();
        ncFile.addGroupAttribute(null, new Attribute("title", "ECMWF C3S Pixel OLCI Burned Area product"));
        ncFile.addGroupAttribute(null, new Attribute("institution", "University of Alcala"));
        ncFile.addGroupAttribute(null, new Attribute("source", "Sentinel-3 A+B OLCI FR, MODIS MCD14ML Collection 6, C3S Land Cover dataset v2.1.1"));
        ncFile.addGroupAttribute(null, new Attribute("history", "Created on " + createNiceTimeString(now)));
        ncFile.addGroupAttribute(null, new Attribute("references", "https://climate.copernicus.eu/"));
        ncFile.addGroupAttribute(null, new Attribute("tracking_id", uuid));
        ncFile.addGroupAttribute(null, new Attribute("conventions", "CF-1.7"));
        ncFile.addGroupAttribute(null, new Attribute("product_version", version));
        ncFile.addGroupAttribute(null, new Attribute("summary", "The pixel product is a raster dataset consisting of three layers that together describe the attributes of the BA product. It uses the following naming convention: ${Indicative Date}-C3S-L3S_FIRE-BA-${Indicative sensor}[-${Additional Segregator}]-fv${xx.x}.nc. ${Indicative Date} is the identifying date for this data set. Format is YYYYMMDD, where YYYY is the four-digit year, MM is the two-digit month from 01 to 12 and DD is the two-digit day of the month from 01 to 31. For monthly products the date is set to 01. ${Indicative sensor} is OLCI. ${Additional Segregator} is the AREA_${TILE_CODE} being the tile code described in the Product User Guide. ${File Version} is the File version number in the form n{1,}[.n{1,}] (That is 1 or more digits followed by optional . and another 1 or more digits). An example is: 20180101-C3S-L3S_FIRE-BA-OLCI-AREA_1-fv1.0.nc"));
        ncFile.addGroupAttribute(null, new Attribute("keywords", "Burned Area, Fire Disturbance, Climate Change, ESA, C3S, GCOS"));
        ncFile.addGroupAttribute(null, new Attribute("id", filename.substring(0, filename.length()-3)));
        ncFile.addGroupAttribute(null, new Attribute("naming_authority", "org.esa-cci"));
//        outputFile.addGroupAttribute(null, new Attribute("doi", getDoi()));
        ncFile.addGroupAttribute(null, new Attribute("keywords_vocabulary", "NASA Global Change Master Directory (GCMD) Science keywords"));
        ncFile.addGroupAttribute(null, new Attribute("cdm_data_type", "Pixel"));
        ncFile.addGroupAttribute(null, new Attribute("comment", "These data were produced as part of the Copernicus Climate Change Service programme."));
        ncFile.addGroupAttribute(null, new Attribute("creation_date", createTimeString(now)));
        ncFile.addGroupAttribute(null, new Attribute("creator_name", "University of Alcala"));
        ncFile.addGroupAttribute(null, new Attribute("creator_url", "http://www.uah.es/"));
        ncFile.addGroupAttribute(null, new Attribute("creator_email", "emilio.chuvieco@uah.es"));
        ncFile.addGroupAttribute(null, new Attribute("contact", "http://copernicus-support.ecmwf.int"));
        ncFile.addGroupAttribute(null, new Attribute("project", "EC C3S Fire Burned Area"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_min", String.valueOf(90.0-180.0*(xyBox.y + xyBox.height)/numRowsGlobal)));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_max", String.valueOf(90.0-180.0*xyBox.y/numRowsGlobal)));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_min", String.valueOf(-180.0+180.0*xyBox.x/numRowsGlobal)));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_max", String.valueOf(-180.0+180.0*(xyBox.x + xyBox.width)/numRowsGlobal)));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_start", timeCoverageStart.substring(0,4)+timeCoverageStart.substring(5,7)+timeCoverageStart.substring(8,10)+"T000000Z"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_end", timeCoverageEnd.substring(0,4)+timeCoverageEnd.substring(5,7)+timeCoverageEnd.substring(8,10)+"T235959Z"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_duration", "P1M"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_resolution", "P1M"));
        ncFile.addGroupAttribute(null, new Attribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Metadata Convention"));
        ncFile.addGroupAttribute(null, new Attribute("license", "EC C3S FIRE BURNED AREA Data Policy"));
        ncFile.addGroupAttribute(null, new Attribute("platform", "Sentinel-3"));
        ncFile.addGroupAttribute(null, new Attribute("sensor", "OLCI"));
        ncFile.addGroupAttribute(null, new Attribute("spatial_resolution", "0.00277778"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_units", "degrees_east"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_units", "degrees_north"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_resolution", "0.00277778"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_resolution", "0.00277778"));
    }

    static String createTimeString(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneId.systemDefault()).format(instant);
    }

    static String createNiceTimeString(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault()).format(instant);
    }

}
