package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.util.StringUtils;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;
import ucar.nc2.write.Nc4Chunking;

import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public abstract class NcFileFactory {

    private static final float MAX_GRID = 769314629F;

    /**
     * Create Writer with default chunking.
     *
     * @param filename
     * @param version
     * @param timeCoverageStart
     * @param timeCoverageEnd
     * @param numberOfDays
     * @param lcClassesCount
     * @param numRowsGlobal
     * @return
     * @throws IOException
     */
    public NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays, int lcClassesCount, int numRowsGlobal) throws IOException {
        return createNcFile(filename, version, timeCoverageStart, timeCoverageEnd, numberOfDays, lcClassesCount, numRowsGlobal, null);
    }

    public NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays, int lcClassesCount, int numRowsGlobal, Nc4Chunking chunking) throws IOException {
        NetcdfFileWriter ncFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4_classic, filename, chunking);

        ncFile.addDimension(null, "vegetation_class", lcClassesCount);
        ncFile.addDimension(null, "lat", numRowsGlobal);
        ncFile.addDimension(null, "lon", 2 * numRowsGlobal);
        ncFile.addDimension(null, getBoundsVarName(), 2);
        ncFile.addDimension(null, "strlen", 150);
        ncFile.addUnlimitedDimension("time");

        Variable latVar = ncFile.addVariable(null, "lat", getLatLonType(), "lat");
        latVar.addAttribute(new Attribute("units", "degree_north"));
        latVar.addAttribute(new Attribute("standard_name", "latitude"));
        latVar.addAttribute(new Attribute("long_name", "latitude"));
        latVar.addAttribute(new Attribute("bounds", getLatBoundsName()));
        ncFile.addVariable(null, getLatBoundsName(), getLatLonType(), "lat " + getBoundsVarName());
        Variable lonVar = ncFile.addVariable(null, "lon", getLatLonType(), "lon");
        lonVar.addAttribute(new Attribute("units", "degree_east"));
        lonVar.addAttribute(new Attribute("standard_name", "longitude"));
        lonVar.addAttribute(new Attribute("long_name", "longitude"));
        lonVar.addAttribute(new Attribute("bounds", getLonBoundsName()));
        ncFile.addVariable(null, getLonBoundsName(), getLatLonType(), "lon " + getBoundsVarName());
        Variable timeVar = ncFile.addVariable(null, "time", DataType.DOUBLE, "time");
        timeVar.addAttribute(new Attribute("units", "days since 1970-01-01 00:00:00"));
        timeVar.addAttribute(new Attribute("standard_name", "time"));
        timeVar.addAttribute(new Attribute("long_name", "time"));
        timeVar.addAttribute(new Attribute("bounds", getTimeBoundsName()));
        timeVar.addAttribute(new Attribute("calendar", "standard"));
        ncFile.addVariable(null, getTimeBoundsName(), DataType.FLOAT, "time " + getBoundsVarName());

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
        if (addValidRanges()) {
            burnedAreaVar.addAttribute(new Attribute("valid_range", Array.factory(DataType.FLOAT, new int[]{2}, new float[]{0, MAX_GRID})));
        }
        burnedAreaVar.addAttribute(new Attribute("cell_methods", "time: sum"));
        Variable standardErrorVar = ncFile.addVariable(null, "standard_error", DataType.FLOAT, "time lat lon");
        standardErrorVar.addAttribute(new Attribute("units", "m2"));
        standardErrorVar.addAttribute(new Attribute("long_name", "standard error of the estimation of burned area"));
        if (addValidRanges()) {
            standardErrorVar.addAttribute(new Attribute("valid_range", Array.factory(DataType.FLOAT, new int[]{2}, new float[]{0, MAX_GRID})));
        }
        addBurnableAreaFractionVar(ncFile);
        Variable observedAreaFractionVar = ncFile.addVariable(null, "fraction_of_observed_area", DataType.FLOAT, "time lat lon");
        observedAreaFractionVar.addAttribute(new Attribute("units", "1"));
        observedAreaFractionVar.addAttribute(new Attribute("long_name", "fraction of observed area"));
        observedAreaFractionVar.addAttribute(new Attribute("comment", getObsAreaComment()));
        if (addValidRanges()) {
            observedAreaFractionVar.addAttribute(new Attribute("valid_range", Array.factory(DataType.FLOAT, new int[]{2}, new float[]{0, 1})));
        }
        if (addNumPatches()) {
            Variable numberOfPatchesVar = ncFile.addVariable(null, "number_of_patches", DataType.FLOAT, "time lat lon");
            numberOfPatchesVar.addAttribute(new Attribute("units", "1"));
            numberOfPatchesVar.addAttribute(new Attribute("long_name", "number of burn patches"));
            numberOfPatchesVar.addAttribute(new Attribute("comment", "Number of contiguous groups of burned pixels."));
        }
        Variable burnedAreaInVegClassVar = ncFile.addVariable(null, "burned_area_in_vegetation_class", DataType.FLOAT, "time vegetation_class lat lon");
        burnedAreaInVegClassVar.addAttribute(new Attribute("units", "m2"));
        burnedAreaInVegClassVar.addAttribute(new Attribute("long_name", "burned area in vegetation class"));
        burnedAreaInVegClassVar.addAttribute(new Attribute("cell_methods", "time: sum"));
        burnedAreaInVegClassVar.addAttribute(new Attribute("comment", getBurnedAreaInVegClassComment()));
        if (addValidRanges()) {
            burnedAreaInVegClassVar.addAttribute(new Attribute("valid_range", Array.factory(DataType.FLOAT, new int[]{2}, new float[]{0, MAX_GRID})));
        }

        if (addCrsVar()) {
            try {
                GeoCoding geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84,
                        2 * numRowsGlobal, numRowsGlobal,
                        -180, 90,
                        360.0 / 2 / numRowsGlobal, 180.0 / numRowsGlobal,
                        0.0, 0.0);

                addWktAsVariable(ncFile, geoCoding);
            } catch (FactoryException | TransformException e) {
                throw new IOException(e);
            }
        }

        addGroupAttributes(filename, version, ncFile, timeCoverageStart, timeCoverageEnd, numberOfDays, numRowsGlobal);
        ncFile.create();
        return ncFile;
    }

    protected boolean addNumPatches() {
        return false;
    }

    protected boolean addCrsVar() {
        return true;
    }

    protected String getObsAreaComment() {
        return "The fraction of observed area is the fraction of the total burnable area in the cell (fraction_of_burnable_area variable of this file) that was observed during the time interval, and was not marked as unsuitable/not observable. The latter refers to the area where it was not possible to obtain observational burned area information for the whole time interval because of the lack of input data (non-existing data for that location and period).";
    }

    protected boolean addValidRanges() {
        return true;
    }

    protected String getTimeBoundsName() {
        return "time_bounds";
    }

    protected String getLonBoundsName() {
        return "lon_bounds";
    }

    protected String getLatBoundsName() {
        return "lat_bounds";
    }

    protected DataType getLatLonType() {
        return DataType.DOUBLE;
    }

    protected String getBoundsVarName() {
        return "bounds";
    }

    private void addWktAsVariable(NetcdfFileWriter ncFile, GeoCoding geoCoding) throws IOException {
        final CoordinateReferenceSystem crs = geoCoding.getMapCRS();
        final double[] matrix = new double[6];
        final MathTransform transform = geoCoding.getImageToMapTransform();
        if (transform instanceof AffineTransform) {
            ((AffineTransform) transform).getMatrix(matrix);
        }
        final Variable crsVariable = ncFile.addVariable("crs", DataType.INT, "");
        crsVariable.addAttribute(new Attribute("wkt", crs.toWKT()));
        crsVariable.addAttribute(new Attribute("i2m", StringUtils.arrayToCsv(matrix)));
    }


    protected String getBurnedAreaInVegClassComment() {
        return "Burned area by land cover classes; land cover classes are from CCI Land Cover, http://www.esa-landcover-cci.org/";
    }

    private void addGroupAttributes(String filename, String version, NetcdfFileWriter ncFile, String timeCoverageStart, String timeCoverageEnd, int timeCoverageDuration, int numRowsGlobal) {
        ncFile.addGroupAttribute(null, new Attribute("title", getTitle().replace("1.0", version)));
        ncFile.addGroupAttribute(null, new Attribute("institution", "University of Alcala"));
        ncFile.addGroupAttribute(null, new Attribute("source", getSource()));
        ncFile.addGroupAttribute(null, new Attribute("history", "Created on " + createNiceTimeString(Instant.now())));
        ncFile.addGroupAttribute(null, new Attribute("references", getReference()));
        ncFile.addGroupAttribute(null, new Attribute("tracking_id", UUID.randomUUID().toString()));
        ncFile.addGroupAttribute(null, new Attribute("Conventions", getCFVersion()));
        ncFile.addGroupAttribute(null, new Attribute("product_version", version));
        if (addFormatVersion()) {
            ncFile.addGroupAttribute(null, new Attribute("format_version", "CCI Data Standards v2.3"));
        }
        ncFile.addGroupAttribute(null, new Attribute("summary", getSummary()));
        ncFile.addGroupAttribute(null, new Attribute("keywords", getKeywordsMetadata()));
        ncFile.addGroupAttribute(null, new Attribute("id", filename));
        ncFile.addGroupAttribute(null, new Attribute("naming_authority", getNamingAuthority()));
        ncFile.addGroupAttribute(null, new Attribute("doi", getDoi()));
        ncFile.addGroupAttribute(null, new Attribute("keywords_vocabulary", getKeywordsVocabulary()));
        ncFile.addGroupAttribute(null, new Attribute("cdm_data_type", "Grid"));
        ncFile.addGroupAttribute(null, new Attribute("comment", getCommentMetadata()));
        ncFile.addGroupAttribute(null, new Attribute("date_created", createTimeString(Instant.now())));
        ncFile.addGroupAttribute(null, new Attribute("creator_name", "University of Alcala"));
        ncFile.addGroupAttribute(null, new Attribute("creator_url", getCreatorUrl()));
        ncFile.addGroupAttribute(null, new Attribute("creator_email", "emilio.chuvieco@uah.es"));
        if (getContactMetadata() != null) {
            ncFile.addGroupAttribute(null, new Attribute("contact", getContactMetadata()));
        }
        ncFile.addGroupAttribute(null, new Attribute("project", getProjectMetadata()));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_min", "-90"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_max", "90"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_min", "-180"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_max", "180"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_vertical_min", "0"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_vertical_max", "0"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_start", timeCoverageStart.substring(0,4)+timeCoverageStart.substring(5,7)+timeCoverageStart.substring(8,10)+"T000000Z"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_end", timeCoverageEnd.substring(0,4)+timeCoverageEnd.substring(5,7)+timeCoverageEnd.substring(8,10)+"T235959Z"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_duration", "P1M"));
        ncFile.addGroupAttribute(null, new Attribute("time_coverage_resolution", "P1M"));
        ncFile.addGroupAttribute(null, new Attribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Metadata Convention"));
        ncFile.addGroupAttribute(null, new Attribute("license", getLicense()));
        ncFile.addGroupAttribute(null, new Attribute("platform", getPlatformGlobalAttribute()));
        ncFile.addGroupAttribute(null, new Attribute("sensor", getSensorGlobalAttribute()));
        ncFile.addGroupAttribute(null, new Attribute("spatial_resolution", "0.25 degrees"));
        if (addKeyVariables()) {
            ncFile.addGroupAttribute(null, new Attribute("key_variables", "burned_area"));
        }
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_units", "degrees_east"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_units", "degrees_north"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lon_resolution", "0.25"));
        ncFile.addGroupAttribute(null, new Attribute("geospatial_lat_resolution", "0.25"));
    }

    protected boolean addKeyVariables() {
        return true;
    }

    protected boolean addFormatVersion() {
        return true;
    }

    protected String getCFVersion() {
        return "CF-1.7";
    }

    protected String getKeywordsVocabulary() {
        return "burned area, fire";
    }

    protected String getCommentMetadata() {
        return "These data were produced as part of the Copernicus Climate Change Service programme.";
    }

    protected String getKeywordsMetadata() {
        return "Burned Area, Fire Disturbance, Climate Change, ESA, GCOS";
    }

    protected String getProjectMetadata() {
        return "EC C3S Fire Burned Area";
    }

    protected String getContactMetadata() {
        return "http://copernicus-support.ecmwf.int";
    }

    protected String getLicense() {
        return "ESA CCI Data Policy: free and open access";
    }

    protected String getCreatorUrl() {
        return "www.esa-fire-cci.org";
    }

    protected String getNamingAuthority() {
        return "org.esa-fire-cci";
    }

    protected String getReference() {
        return "See www.esa-fire-cci.org";
    }

    protected abstract String getSummary();

    protected abstract String getSource();

    protected abstract String getDoi();

    protected abstract void addBurnableAreaFractionVar(NetcdfFileWriter ncFile);

    protected abstract String getPlatformGlobalAttribute();

    protected abstract String getSensorGlobalAttribute();

    protected abstract String getTitle();

    static String createTimeString(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneId.systemDefault()).format(instant);
    }

    static String createNiceTimeString(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault()).format(instant);
    }
}
