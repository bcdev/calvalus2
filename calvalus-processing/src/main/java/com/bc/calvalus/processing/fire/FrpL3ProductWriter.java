package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.DateUtils;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.AbstractProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNode;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;
import ucar.nc2.write.Nc4Chunking;
import ucar.nc2.write.Nc4ChunkingDefault;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Year;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class FrpL3ProductWriter extends AbstractProductWriter {

    private static final String DIM_STRING = "time lat lon";

    private Map<String, VariableTemplate> variableTemplates;
    private Map<String, Array> variableData;
    private List<String> bandsToIgnore;
    private NetcdfFileWriter fileWriter;

    FrpL3ProductWriter(ProductWriterPlugIn writerPlugIn) {
        super(writerPlugIn);

        fileWriter = null;

        createVariableTemplates();
        createBandNamesToIgnore();
        variableData = new HashMap<>();
    }

    static String getOutputPath(Object output) {
        final String filePath;
        if (output instanceof File) {
            filePath = ((File) output).getAbsolutePath();
        } else if (output instanceof String) {
            filePath = (String) output;
        } else {
            throw new IllegalArgumentException("invalid input type");
        }
        return filePath;
    }

    static void addDimensions(NetcdfFileWriter fileWriter, Product product) {
        fileWriter.addDimension("time", 1);
        fileWriter.addDimension("lon", product.getSceneRasterWidth());
        fileWriter.addDimension("lat", product.getSceneRasterHeight());
        fileWriter.addDimension("bounds", 2);
    }

    static void addGlobalMetadata(NetcdfFileWriter fileWriter, Product product) {
        final SimpleDateFormat COMPACT_ISO_FORMAT = DateUtils.createDateFormat("yyyyMMdd'T'HHmmss'Z'");
        final String dateStringNow = COMPACT_ISO_FORMAT.format(new Date());

        fileWriter.addGlobalAttribute("title", "ECMWF C3S Gridded OLCI Fire Radiative Power product");
        fileWriter.addGlobalAttribute("institution", "King's College London, Brockmann Consult GmbH");
        fileWriter.addGlobalAttribute("source", "ESA Sentinel-3 A+B SLSTR FRP");
        fileWriter.addGlobalAttribute("history", "Created on " + dateStringNow);
        fileWriter.addGlobalAttribute("references", "See https://climate.copernicus.eu/");
        fileWriter.addGlobalAttribute("tracking_id", UUID.randomUUID().toString());
        fileWriter.addGlobalAttribute("Conventions", "CF-1.7");
        fileWriter.addGlobalAttribute("summary", "TODO!");
        fileWriter.addGlobalAttribute("keywords", "Fire Radiative Power, Climate Change, ESA, C3S, GCOS");
        fileWriter.addGlobalAttribute("id", product.getName());
        fileWriter.addGlobalAttribute("naming_authority", "org.esa-cci");
        fileWriter.addGlobalAttribute("keywords_vocabulary", "NASA Global Change Master Directory (GCMD) Science keywords");
        fileWriter.addGlobalAttribute("cdm_data_type", "Grid");
        fileWriter.addGlobalAttribute("comment", "These data were produced as part of the Copernicus Climate Change Service programme.");
        fileWriter.addGlobalAttribute("date_created", dateStringNow);
        fileWriter.addGlobalAttribute("creator_name", "Brockmann Consult GmbH");
        fileWriter.addGlobalAttribute("creator_url", "https://www.brockmann-consult.de");
        fileWriter.addGlobalAttribute("creator_email", "martin.boettcher@brockmann-consult.de");
        fileWriter.addGlobalAttribute("contact", "http://copernicus-support.ecmwf.int");
        fileWriter.addGlobalAttribute("project", "EC C3S Fire Radiative Power");
        fileWriter.addGlobalAttribute("geospatial_lat_min", "-90");
        fileWriter.addGlobalAttribute("geospatial_lat_max", "90");
        fileWriter.addGlobalAttribute("geospatial_lon_min", "-180");
        fileWriter.addGlobalAttribute("geospatial_lon_max", "180");
        fileWriter.addGlobalAttribute("geospatial_vertical_min", "0");
        fileWriter.addGlobalAttribute("geospatial_vertical_max", "0");
        fileWriter.addGlobalAttribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Metadata Convention");
        fileWriter.addGlobalAttribute("platform", "Sentinel-3");
        fileWriter.addGlobalAttribute("sensor", "SLSTR");
        fileWriter.addGlobalAttribute("geospatial_lon_units", "degrees_east");
        fileWriter.addGlobalAttribute("geospatial_lat_units", "degrees_north");
    }

    static void addAxesAndBoundsVariables(NetcdfFileWriter fileWriter) {
        Variable variable = fileWriter.addVariable("lon", DataType.FLOAT, "lon");
        variable.addAttribute(new Attribute(CF.UNITS, "degrees_east"));
        variable.addAttribute(new Attribute(CF.STANDARD_NAME, "longitude"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "longitude"));
        variable.addAttribute(new Attribute("bounds", "lon_bounds"));

        variable = fileWriter.addVariable("lat", DataType.FLOAT, "lat");
        variable.addAttribute(new Attribute(CF.UNITS, "degrees_north"));
        variable.addAttribute(new Attribute(CF.STANDARD_NAME, "latitude"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "latitude"));
        variable.addAttribute(new Attribute("bounds", "lat_bounds"));

        // @todo 2 tb/** is this standard? Double seems to be a too large datatype 2020-09-28
        variable = fileWriter.addVariable("time", DataType.DOUBLE, "time");
        variable.addAttribute(new Attribute(CF.UNITS, "days since 1970-01-01 00:00:00"));
        variable.addAttribute(new Attribute(CF.STANDARD_NAME, "time"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "time"));
        variable.addAttribute(new Attribute("bounds", "time_bounds"));
        variable.addAttribute(new Attribute("calendar", "standard"));

        fileWriter.addVariable("lon_bounds", DataType.FLOAT, "lon bounds");
        fileWriter.addVariable("lat_bounds", DataType.FLOAT, "lat bounds");
        fileWriter.addVariable("time_bounds", DataType.DOUBLE, "time bounds");
    }

    private void createVariableTemplates() {
        variableTemplates = new HashMap<>();
        variableTemplates.put("s3a_day_pixel_sum", new VariableTemplate("s3a_day_pixel", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A daytime pixels"));
        variableTemplates.put("s3a_day_cloud_sum", new VariableTemplate("s3a_day_cloud", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A daytime cloudy pixels"));
        variableTemplates.put("s3a_day_water_sum", new VariableTemplate("s3a_day_water", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A daytime water pixels"));
        variableTemplates.put("s3a_day_fire_sum", new VariableTemplate("s3a_day_fire", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A daytime active fire pixels"));
        variableTemplates.put("s3a_day_frp_mean", new VariableTemplate("s3a_day_frp", DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3A during daytime"));
        variableTemplates.put("s3a_night_pixel_sum", new VariableTemplate("s3a_night_pixel", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime pixels"));
        variableTemplates.put("s3a_night_cloud_sum", new VariableTemplate("s3a_night_cloud", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime cloudy pixels"));
        variableTemplates.put("s3a_night_water_sum", new VariableTemplate("s3a_night_water", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime water pixels"));
        variableTemplates.put("s3a_night_fire_sum", new VariableTemplate("s3a_night_fire", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime active fire pixels"));
        variableTemplates.put("s3a_night_frp_mean", new VariableTemplate("s3a_night_frp", DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3A during nighttime"));
        variableTemplates.put("s3b_day_pixel_sum", new VariableTemplate("s3b_day_pixel", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B daytime pixels"));
        variableTemplates.put("s3b_day_cloud_sum", new VariableTemplate("s3b_day_cloud", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B daytime cloudy pixels"));
        variableTemplates.put("s3b_day_water_sum", new VariableTemplate("s3b_day_water", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B daytime water pixels"));
        variableTemplates.put("s3b_day_fire_sum", new VariableTemplate("s3b_day_fire", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B daytime active fire pixels"));
        variableTemplates.put("s3b_day_frp_mean", new VariableTemplate("s3b_day_frp", DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3B during daytime"));
        variableTemplates.put("s3b_night_pixel_sum", new VariableTemplate("s3b_night_pixel", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime pixels"));
        variableTemplates.put("s3b_night_cloud_sum", new VariableTemplate("s3b_night_cloud", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime cloudy pixels"));
        variableTemplates.put("s3b_night_water_sum", new VariableTemplate("s3b_night_water", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime water pixels"));
        variableTemplates.put("s3b_night_fire_sum", new VariableTemplate("s3b_night_fire", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime active fire pixels"));
        variableTemplates.put("s3b_night_frp_mean", new VariableTemplate("s3b_night_frp", DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3B during nighttime"));
    }

    private void createBandNamesToIgnore() {
        bandsToIgnore = new ArrayList<>();
        bandsToIgnore.add("num_obs");
        bandsToIgnore.add("num_passes");
        bandsToIgnore.add("s3a_day_frp_sigma");
        bandsToIgnore.add("s3a_night_frp_sigma");
        bandsToIgnore.add("s3b_day_frp_sigma");
        bandsToIgnore.add("s3b_night_frp_sigma");
    }

    @Override
    public void writeBandRasterData(Band sourceBand, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData sourceBuffer, ProgressMonitor pm) {
        final String name = sourceBand.getName();
        if (bandsToIgnore.contains(name)) {
            return;
        }

        final VariableTemplate variableTemplate = variableTemplates.get(name);
        final Array variableArray = variableData.get(variableTemplate.name);
        final Index index = variableArray.getIndex();
        int i = 0;
        for (int y = sourceOffsetY; y < sourceOffsetY + sourceHeight; y++) {
            for (int x = sourceOffsetX; x < sourceOffsetX + sourceWidth; x++) {
                index.set(0, y, x);
                variableArray.setFloat(index, sourceBuffer.getElemFloatAt(i));
                ++i;
            }
        }
    }

    @Override
    protected void writeProductNodesImpl() throws IOException {
        final String filePath = getOutputPath(getOutput());

        final Nc4Chunking chunking = Nc4ChunkingDefault.factory(Nc4Chunking.Strategy.standard, 5, true);
        fileWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, filePath, chunking);

        final Product sourceProduct = getSourceProduct();
        addDimensions(fileWriter, sourceProduct);
        addGlobalMetadata(fileWriter, sourceProduct);
        addAxesAndBoundsVariables(fileWriter);

        addProductVariables(sourceProduct);
        addWeightedFRPVariables();

        fileWriter.create();

        writeAxesAndBoundsVariables(sourceProduct);
    }

    private void writeAxesAndBoundsVariables(Product sourceProduct) throws IOException {
        // @todo 3 tb/tb refactor and simplify, add tests 2020-09-30
        final int sceneRasterWidth = sourceProduct.getSceneRasterWidth();
        final double lonStep = 360.0 / sceneRasterWidth;
        final double lonOffset = lonStep * 0.5;
        final float[] longitudes = new float[sceneRasterWidth];
        final float[] lonBounds = new float[2 * sceneRasterWidth];
        for (int i = 0; i < sceneRasterWidth; i++) {
            longitudes[i] = (float) (i * lonStep + lonOffset - 180.0);
            lonBounds[2 * i] = (float) (longitudes[i] - lonOffset);
            lonBounds[2 * i + 1] = (float) (longitudes[i] + lonOffset);
        }

        final Array lonArray = Array.factory(DataType.FLOAT, new int[]{sceneRasterWidth}, longitudes);
        final Array lonBoundsArray = Array.factory(DataType.FLOAT, new int[]{sceneRasterWidth, 2}, lonBounds);
        final Variable lonVariable = fileWriter.findVariable("lon");
        final Variable lonBoundsVariable = fileWriter.findVariable("lon_bounds");
        try {
            fileWriter.write(lonVariable, lonArray);
            fileWriter.write(lonBoundsVariable, lonBoundsArray);
        } catch (InvalidRangeException e) {
            throw new IOException(e.getMessage());
        }

        final int sceneRasterHeight = sourceProduct.getSceneRasterHeight();
        final double latStep = 180.0 / sceneRasterHeight;
        final double latOffset = latStep * 0.5;
        final float[] latitudes = new float[sceneRasterHeight];
        final float[] latBounds = new float[2 * sceneRasterHeight];
        for (int i = 0; i < sceneRasterHeight; i++) {
            latitudes[i] = 90.f - (float) (i * latStep + latOffset);
            latBounds[2 * i] = (float) (latitudes[i] + latOffset);
            latBounds[2 * i + 1] = (float) (latitudes[i] - latOffset);
        }
        final Array latArray = Array.factory(DataType.FLOAT, new int[]{sceneRasterHeight}, latitudes);
        final Array latBoundsArray = Array.factory(DataType.FLOAT, new int[]{sceneRasterHeight, 2}, latBounds);
        final Variable latVariable = fileWriter.findVariable("lat");
        final Variable latBoundsVariable = fileWriter.findVariable("lat_bounds");
        try {
            fileWriter.write(latVariable, latArray);
            fileWriter.write(latBoundsVariable, latBoundsArray);
        } catch (InvalidRangeException e) {
            throw new IOException(e.getMessage());
        }

        final Date startDate = sourceProduct.getStartTime().getAsDate();
        final Date endDate = sourceProduct.getEndTime().getAsDate();
        final LocalDate start = startDate.toInstant().atZone(ZoneId.of("UTC")).toLocalDate();
        final LocalDate end = endDate.toInstant().atZone(ZoneId.of("UTC")).toLocalDate();
        final LocalDate epoch = Year.of(1970).atMonth(1).atDay(1);
        final long startDays = ChronoUnit.DAYS.between(epoch, start);
        final long endDays = ChronoUnit.DAYS.between(epoch, end);

        final Array timeArray = Array.factory(DataType.DOUBLE, new int[]{1}, new double[]{startDays});
        final Array timeBoundsArray = Array.factory(DataType.DOUBLE, new int[]{1, 2}, new double[]{startDays, endDays});
        final Variable timeVariable = fileWriter.findVariable("time");
        final Variable timeBoundsVariable = fileWriter.findVariable("time_bounds");
        try {
            fileWriter.write(timeVariable, timeArray);
            fileWriter.write(timeBoundsVariable, timeBoundsArray);
        } catch (InvalidRangeException e) {
            throw new IOException(e.getMessage());
        }
    }

    private void addWeightedFRPVariables() {
        final Product sourceProduct = getSourceProduct();
        final int sceneRasterWidth = sourceProduct.getSceneRasterWidth();
        final int sceneRasterHeight = sourceProduct.getSceneRasterHeight();

        Variable variable = fileWriter.addVariable("s3a_day_frp_weighted", DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, "MW"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "Mean Fire Radiative Power measured by S3A during daytime, weighted by cloud coverage"));
        Array dataArray = Array.factory(DataType.FLOAT, new int[]{1, sceneRasterHeight, sceneRasterWidth});
        variableData.put("s3a_day_frp_weighted", dataArray);

        variable = fileWriter.addVariable("s3a_night_frp_weighted", DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, "MW"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "Mean Fire Radiative Power measured by S3A during nighttime, weighted by cloud coverage"));
        dataArray = Array.factory(DataType.FLOAT, new int[]{1, sceneRasterHeight, sceneRasterWidth});
        variableData.put("s3a_night_frp_weighted", dataArray);

        variable = fileWriter.addVariable("s3b_day_frp_weighted", DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, "MW"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "Mean Fire Radiative Power measured by S3B during daytime, weighted by cloud coverage"));
        dataArray = Array.factory(DataType.FLOAT, new int[]{1, sceneRasterHeight, sceneRasterWidth});
        variableData.put("s3b_day_frp_weighted", dataArray);

        variable = fileWriter.addVariable("s3b_night_frp_weighted", DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, "MW"));
        variable.addAttribute(new Attribute(CF.LONG_NAME, "Mean Fire Radiative Power measured by S3B during nighttime, weighted by cloud coverage"));
        dataArray = Array.factory(DataType.FLOAT, new int[]{1, sceneRasterHeight, sceneRasterWidth});
        variableData.put("s3b_night_frp_weighted", dataArray);
    }

    private void addProductVariables(Product sourceProduct) {
        final Band[] bands = sourceProduct.getBands();
        final int sceneRasterWidth = sourceProduct.getSceneRasterWidth();
        final int sceneRasterHeight = sourceProduct.getSceneRasterHeight();
        for (final Band band : bands) {
            final String bandName = band.getName();
            if (bandsToIgnore.contains(bandName)) {
                continue;
            }

            final VariableTemplate template = getTemplate(bandName);
            final Variable variable = fileWriter.addVariable(template.name, template.dataType, DIM_STRING);
            variable.addAttribute(new Attribute(CF.FILL_VALUE, template.fillValue, template.dataType.isUnsigned()));
            variable.addAttribute(new Attribute(CF.UNITS, template.units));
            variable.addAttribute(new Attribute(CF.LONG_NAME, template.longName));

            final Array dataArray = Array.factory(template.dataType, new int[]{1, sceneRasterHeight, sceneRasterWidth});
            variableData.put(template.name, dataArray);
        }
    }

    @Override
    public void flush() throws IOException {
        if (fileWriter != null) {
            writevariableData();
            fileWriter.flush();
        }
    }

    private void writevariableData() throws IOException {
        try {
            calculateWeightedFRP("s3a_day_frp_weighted", "s3a_day_frp", "s3a_day_pixel", "s3a_day_water", "s3a_day_cloud");
            calculateWeightedFRP("s3a_night_frp_weighted", "s3a_night_frp", "s3a_night_pixel", "s3a_night_water", "s3a_night_cloud");
            calculateWeightedFRP("s3b_day_frp_weighted", "s3b_day_frp", "s3b_day_pixel", "s3b_day_water", "s3b_day_cloud");
            calculateWeightedFRP("s3b_night_frp_weighted", "s3b_night_frp", "s3b_night_pixel", "s3b_night_water", "s3b_night_cloud");

            final Set<Map.Entry<String, Array>> entries = variableData.entrySet();
            for (final Map.Entry<String, Array> entry : entries) {
                final Variable variable = fileWriter.findVariable(entry.getKey());

                fileWriter.write(variable, entry.getValue());
            }
        } catch (InvalidRangeException e) {
            throw new IOException(e.getMessage());
        }
    }

    private void calculateWeightedFRP(String weightedFrpName, String frpName, String pxName, String waterName, String cloudName) {
        final Array weightedFRPArray = variableData.get(weightedFrpName);
        final Array frpArray = variableData.get(frpName);
        final Array pixelArray = variableData.get(pxName);
        final Array waterArray = variableData.get(waterName);
        final Array cloudArray = variableData.get(cloudName);

        for (int i = 0; i < frpArray.getSize(); i++) {
            float weightedFrp = Float.NaN;

            final float nPx = pixelArray.getFloat(i);
            final float nWater = waterArray.getFloat(i);
            final float nCloud = cloudArray.getFloat(i);
            final float frp = frpArray.getFloat(i);

            final float num = nPx - nWater;
            final float denom = num - nCloud;
            if (denom != 0.f) {
                weightedFrp = frp * num / denom;
            }

            weightedFRPArray.setFloat(i, weightedFrp);
        }
    }

    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            flush();
            fileWriter.close();
            fileWriter = null;
        }
    }

    @Override
    public boolean shouldWrite(ProductNode node) {
        return false;
    }

    @Override
    public boolean isIncrementalMode() {
        return false;
    }

    @Override
    public void setIncrementalMode(boolean enabled) {

    }

    @Override
    public void deleteOutput() throws IOException {

    }

    @Override
    public void removeBand(Band band) {

    }

    @Override
    public void setFormatName(String formatName) {

    }

    VariableTemplate getTemplate(String variableName) {
        final VariableTemplate variableTemplate = variableTemplates.get(variableName);
        if (variableTemplate == null) {
            throw new IllegalArgumentException("Unsupported variable:" + variableName);
        }
        return variableTemplate;
    }

    static class VariableTemplate {
        final DataType dataType;
        final String name;
        final Number fillValue;
        final String units;
        final String longName;

        VariableTemplate(String name, DataType dataType, Number fillValue, String units, String longName) {
            this.name = name;
            this.dataType = dataType;
            this.fillValue = fillValue;
            this.units = units;
            this.longName = longName;
        }
    }
}
