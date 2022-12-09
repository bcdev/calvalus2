package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.DateUtils;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.AbstractProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;
import ucar.nc2.constants.CF;
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

import static com.bc.calvalus.processing.fire.FrpL3ProductFileWriter.ProductTemporalClass.CYCLE;
import static com.bc.calvalus.processing.fire.FrpL3ProductFileWriter.ProductTemporalClass.DAILY;

public class FrpL3ProductFileWriter extends AbstractProductWriter {

    private static final String DIM_STRING = "time lat lon";
    private static final int CF_FILL_UINT = -1;
    private static final float WEIGHTED_THRESHOLD = 0.9f;

    private NetcdfFileWriter fileWriter;
    private Map<String, VariableTemplate> variableTemplates = new HashMap<>();
    private Map<String, Array> variableData = new HashMap<>();
    private ProductTemporalClass temporalClass = ProductTemporalClass.UNKNOWN;
    private boolean dataWritten;

    FrpL3ProductFileWriter(ProductWriterPlugIn writerPlugIn) {
        super(writerPlugIn);
    }

    @Override
    protected void writeProductNodesImpl() throws IOException {
        final Product sourceProduct = getSourceProduct();
        final String filePath = getOutputPath(getOutput());
        final Nc4Chunking chunking = Nc4ChunkingDefault.factory(Nc4Chunking.Strategy.standard, 5, true);
        fileWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, filePath, chunking);
        temporalClass = getTemporalClass(sourceProduct);
        final String platform = sourceProduct.getProductType().split("-")[0];
        final String nightOrDay = sourceProduct.getProductType().split("-")[1];
        addDimensions(fileWriter, sourceProduct);
        addGlobalMetadata(fileWriter, sourceProduct, temporalClass, platform, nightOrDay);
        addAxesAndBoundsVariables(fileWriter);
        initVariableTemplates(platform, nightOrDay);
        addProductVariables(sourceProduct, platform, nightOrDay);
        addFractionAndWeightedVariables(temporalClass, platform, nightOrDay);
        fileWriter.create();
        writeAxesAndBoundsVariables(sourceProduct);
        dataWritten = false;
    }

    @Override
    public void writeBandRasterData(Band sourceBand, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData sourceBuffer, ProgressMonitor pm) {
        final String name = sourceBand.getName();
        final VariableTemplate template = variableTemplates.get(name);
        if (template == null) { return; }
        final Array variableArray = variableData.get(template.name);
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
    public void flush() throws IOException {
        if (fileWriter != null) {
            writeVariableData();
            fileWriter.flush();
            dataWritten = true;
        }
    }

    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            flush();
            fileWriter.close();
            fileWriter = null;
        }
        temporalClass = ProductTemporalClass.UNKNOWN;
    }

    @Override
    public void deleteOutput() throws IOException {
        throw new IllegalStateException("not implemented");
    }

    static ProductTemporalClass getTemporalClass(Product product) {
        final ProductData.UTC startTime = product.getStartTime();
        final ProductData.UTC endTime = product.getEndTime();
        if (startTime != null && endTime != null) {
            final Date startDate = startTime.getAsDate();
            final Date endDate = endTime.getAsDate();
            final long between = ChronoUnit.DAYS.between(startDate.toInstant(), endDate.toInstant());
            if (between >= 0 && between <= 1) {
                return ProductTemporalClass.DAILY;
            } else if (between >= 26 && between <= 27) {
                return CYCLE;
            } else if (between >= 27) {
                return ProductTemporalClass.MONTHLY;
            }
        }
        return ProductTemporalClass.UNKNOWN;
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

    static void addGlobalMetadata(NetcdfFileWriter fileWriter,
                                  Product product,
                                  ProductTemporalClass type,
                                  String platform,
                                  String nightOrDay) {
        final SimpleDateFormat COMPACT_ISO_FORMAT = DateUtils.createDateFormat("yyyyMMdd'T'HHmmss'Z'");
        final String dateStringNow = COMPACT_ISO_FORMAT.format(new Date());
        fileWriter.addGlobalAttribute("title", "ECMWF C3S Gridded OLCI Fire Radiative Power product");
        fileWriter.addGlobalAttribute("institution", "King's College London, Brockmann Consult GmbH");
        fileWriter.addGlobalAttribute("source", "ESA Sentinel-3 A+B SLSTR FRP");
        fileWriter.addGlobalAttribute("history", "Created on " + dateStringNow);
        fileWriter.addGlobalAttribute("references", "See https://climate.copernicus.eu/");
        fileWriter.addGlobalAttribute("tracking_id", UUID.randomUUID().toString());
        fileWriter.addGlobalAttribute("Conventions", "CF-1.7");
        fileWriter.addGlobalAttribute("summary", getSummaryText(type));
        fileWriter.addGlobalAttribute("keywords", "Fire Radiative Power, Climate Change, ESA, C3S, GCOS");
        fileWriter.addGlobalAttribute("id", product.getName());
        fileWriter.addGlobalAttribute("naming_authority", "org.esa-cci");
        fileWriter.addGlobalAttribute("keywords_vocabulary", "NASA Global Change Master Directory (GCMD) Science keywords");
        fileWriter.addGlobalAttribute("cdm_data_type", "Grid");
        fileWriter.addGlobalAttribute("comment", "These data were produced as part of the Copernicus Climate Change Service programme.");
        fileWriter.addGlobalAttribute("date_created", dateStringNow);
        fileWriter.addGlobalAttribute("creator_name", "Brockmann Consult GmbH");
        fileWriter.addGlobalAttribute("creator_url", "https://www.brockmann-consult.de");
        fileWriter.addGlobalAttribute("creator_email", "info@brockmann-consult.de");
        fileWriter.addGlobalAttribute("contact", "http://copernicus-support.ecmwf.int");
        fileWriter.addGlobalAttribute("project", "EC C3S Fire Radiative Power");
        fileWriter.addGlobalAttribute("geospatial_lat_min", "-90");
        fileWriter.addGlobalAttribute("geospatial_lat_max", "90");
        fileWriter.addGlobalAttribute("geospatial_lon_min", "-180");
        fileWriter.addGlobalAttribute("geospatial_lon_max", "180");
        fileWriter.addGlobalAttribute("geospatial_vertical_min", "0");
        fileWriter.addGlobalAttribute("geospatial_vertical_max", "0");
        final ProductData.UTC startTime = product.getStartTime();
        if (startTime != null) {
            fileWriter.addGlobalAttribute("time_coverage_start", COMPACT_ISO_FORMAT.format(startTime.getAsDate()));
        }
        final ProductData.UTC endTime = product.getEndTime();
        if (endTime != null) {
            fileWriter.addGlobalAttribute("time_coverage_end", COMPACT_ISO_FORMAT.format(endTime.getAsDate()));
        }
        final String coverageString = getCoverageString(type);
        fileWriter.addGlobalAttribute("time_coverage_duration", coverageString);
        fileWriter.addGlobalAttribute("time_coverage_resolution", coverageString);
        fileWriter.addGlobalAttribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Metadata Convention");
        fileWriter.addGlobalAttribute("license", "EC C3S FRP Data Policy");
        fileWriter.addGlobalAttribute("platform", platform);
        fileWriter.addGlobalAttribute("night_or_day", nightOrDay);
        fileWriter.addGlobalAttribute("sensor", "SLSTR");
        fileWriter.addGlobalAttribute("spatial_resolution", getResolutionString(type, true));
        fileWriter.addGlobalAttribute("geospatial_lon_units", "degrees_east");
        fileWriter.addGlobalAttribute("geospatial_lat_units", "degrees_north");
        fileWriter.addGlobalAttribute("geospatial_lon_resolution", getResolutionString(type, false));
        fileWriter.addGlobalAttribute("geospatial_lat_resolution", getResolutionString(type, false));
    }

    static String getSummaryText(ProductTemporalClass productType) {
        final StringBuilder summary = new StringBuilder();
        summary.append("The Copernicus Climate Change Service issues three Level 3 Fire Radiative Power (FRP) Products, each generated from Level 2 Sentinel-3 Active Fire Detection and FRP Products issued in NTC mode, which themselves are based on Sentinel 3 SLSTR data. ");
        if (productType == ProductTemporalClass.DAILY) {
            summary.append("The global Level 3 Daily FRP Products synthesise global data from the Level 2 AF Detection and FRP Product granules at 0.1 degree spatial and at 1-day temporal resolution");
        } else if (productType == CYCLE) {
            summary.append("The global Level 3 27-Day FRP Products synthesise global data from the Level 2 AF Detection and FRP Product granules at 0.1 degree spatial and at 27-day temporal resolution");
        } else if (productType == ProductTemporalClass.MONTHLY) {
            summary.append("The global Level 3 Monthly Summary FRP Products synthesise global data from the Level 2 AF Detection and FRP Product granules at 0.25 degree spatial and at 1 month temporal resolution");
        } else {
            throw new IllegalArgumentException("Invalid target product type");
        }
        summary.append(", and also provide some adjustments for unsuitable atmospheric condition since e.g clouds can mask actively burning fires from view. These products are primarily designed for ease of use of the key information coming from individual granule-based Level 2 Products, for example in global modelling, trend analysis and model evaluation.");
        summary.append(" Each product is available in separate files per platform (S3A, S3B, ...) and per nighttime and daytime observations.");
        return summary.toString();
    }

    static String getCoverageString(ProductTemporalClass productType) {
        if (productType == ProductTemporalClass.DAILY) {
            return "P1D";
        } else if (productType == CYCLE) {
            return "P27D";
        } else if (productType == ProductTemporalClass.MONTHLY) {
            return "P1M";
        }
        return "UNKNOWN";
    }

    static String getResolutionString(ProductTemporalClass productType, boolean addUnits) {
        String resolutionString;
        if (productType == ProductTemporalClass.DAILY || productType == CYCLE) {
            resolutionString = "0.1";
        } else if (productType == ProductTemporalClass.MONTHLY) {
            resolutionString = "0.25";
        } else {
            return "UNKNOWN";
        }
        if (addUnits) {
            resolutionString += " degrees";
        }
        return resolutionString;
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

    private void addProductVariables(Product sourceProduct, String platform, String nightOrDay) {
        final int sceneRasterWidth = sourceProduct.getSceneRasterWidth();
        final int sceneRasterHeight = sourceProduct.getSceneRasterHeight();
        for (final Band band : sourceProduct.getBands()) {
            final String bandName = band.getName();
            final VariableTemplate template = getVariableTemplate(bandName);
            if (template == null) { continue; }
            final Variable variable = fileWriter.addVariable(template.name, template.dataType, DIM_STRING);
            variable.addAttribute(new Attribute(CF._FILLVALUE, template.fillValue, template.dataType.isUnsigned()));
            variable.addAttribute(new Attribute(CF.UNITS, template.units));
            variable.addAttribute(new Attribute(CF.LONG_NAME, template.longName));
            final Array dataArray = Array.factory(template.dataType, new int[] {1, sceneRasterHeight, sceneRasterWidth});
            variableData.put(template.name, writeFillValue(dataArray));
        }
    }

    VariableTemplate getVariableTemplate(String name) {
        return variableTemplates.get(name);
    }

    void initVariableTemplates(String platform, String nightOrDay) {
        variableTemplates.put("frp_mean",
                              new VariableTemplate("frp", DataType.FLOAT, Float.NaN, "MW", String.format("Mean Fire Radiative Power measured by %s during %stime", platform, nightOrDay)));
        variableTemplates.put("frp_unc_sum",
                              new VariableTemplate("frp_unc", DataType.FLOAT, Float.NaN, "MW", String.format("Mean Fire Radiative Power uncertainty measured by %s during %stime", platform, nightOrDay)));
        variableTemplates.put("pixel_sum",
                              new VariableTemplate("total_pixels", DataType.UINT, CF_FILL_UINT, "1", String.format("Total number of %s %stime pixels", platform, nightOrDay)));
        variableTemplates.put("fire_sum",
                              new VariableTemplate("fire_pixels", DataType.UINT, CF_FILL_UINT, "1", String.format("Total number of %s %stime active fire pixels", platform, nightOrDay)));
        variableTemplates.put("water_sum",
                              new VariableTemplate("surface_conditions_flag_pixels", DataType.UINT, CF_FILL_UINT, "1", String.format("Total number of %s %stime pixels unprocessed by the AF detection algorithm due to them being considered unsuitable surfaces, e.g. permanent water", platform, nightOrDay)));
        variableTemplates.put("cloud_sum",
                              new VariableTemplate("atmospheric_condition_flag_pixels", DataType.UINT, CF_FILL_UINT, "1", String.format("Total number of %s %stime pixels unprocessed by the AF detection algorithm due to them being considered to have unsuitable atmospheric conditions for FRP product processing, e.g. certain types of cloud", platform, nightOrDay)));
        variableTemplates.put("cloud_fraction_sum",
                              new VariableTemplate("atmospheric_condition_fraction", DataType.FLOAT, Float.NaN, "1", String.format("Mean unsuitable atmospheric condition fraction of %s %stime land pixels in a macro pixel of 1.1 (1.25) degrees", platform, nightOrDay)));
        variableTemplates.put("fire_weighted_sum",
                              new VariableTemplate("fire_weighted_pixels", DataType.FLOAT, Float.NaN, "1", String.format("Number of %s %s time active fire pixels weighted by atmospheric condition fraction", platform, nightOrDay)));
    }

    static Array writeFillValue(Array array) {
        final DataType dataType = array.getDataType();
        if (dataType == DataType.UINT) {
            for (int i = 0; i < array.getSize(); i++) {
                array.setInt(i, CF_FILL_UINT);
            }
        } else if (dataType == DataType.FLOAT) {
            for (int i = 0; i < array.getSize(); i++) {
                array.setFloat(i, Float.NaN);
            }
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
        return array;
    }

    private void addFractionAndWeightedVariables(ProductTemporalClass type, String platform, String nightOrDay) {
        final Product sourceProduct = getSourceProduct();
        final int sceneRasterWidth = sourceProduct.getSceneRasterWidth();
        final int sceneRasterHeight = sourceProduct.getSceneRasterHeight();
        final int[] dimensions = {1, sceneRasterHeight, sceneRasterWidth};
        final String fractionLongName;
        if (type == DAILY || type == CYCLE) {
            fractionLongName = String.format("Mean unsuitable atmospheric condition fraction of %s %stime land pixels in a macro pixel of 1.1 degrees", platform, nightOrDay);
        } else {
            fractionLongName = String.format("Mean unsuitable atmospheric condition fraction of %s %stime land pixels in a macro pixel of 1.25 degrees", platform, nightOrDay);
        }
        final String pixelsLongName = String.format("Number of %s %stime active fire pixels weighted by atmospheric condition fraction", platform, nightOrDay);
        addWeightedVariable(dimensions, "atmospheric_condition_fraction", fractionLongName, "1");
        addWeightedVariable(dimensions, "fire_weighted_pixels", pixelsLongName, "1");
    }

    private void addWeightedVariable(int[] dimensions, String name, String longName, String units) {
        final Variable variable = fileWriter.addVariable(name, DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF._FILLVALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, units));
        variable.addAttribute(new Attribute(CF.LONG_NAME, longName));
        final Array dataArray = Array.factory(DataType.FLOAT, dimensions);
        variableData.put(name, writeFillValue(dataArray));
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
        writeAxisAndBounds(sceneRasterWidth, longitudes, lonBounds, "lon", "lon_bounds");

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
        writeAxisAndBounds(sceneRasterHeight, latitudes, latBounds, "lat", "lat_bounds");

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

    private void writeAxisAndBounds(int size, float[] longitudes, float[] lonBounds, String variableName, String boundsVariableName) throws IOException {
        final Array lonArray = Array.factory(DataType.FLOAT, new int[]{size}, longitudes);
        final Array lonBoundsArray = Array.factory(DataType.FLOAT, new int[]{size, 2}, lonBounds);
        final Variable lonVariable = fileWriter.findVariable(variableName);
        final Variable lonBoundsVariable = fileWriter.findVariable(boundsVariableName);
        try {
            fileWriter.write(lonVariable, lonArray);
            fileWriter.write(lonBoundsVariable, lonBoundsArray);
        } catch (InvalidRangeException e) {
            throw new IOException(e.getMessage());
        }
    }




    private void writeVariableData() throws IOException {
        if (dataWritten) { return; }
        calculateUncertainty("frp_unc", "fire_pixels");
        calculateCloudFraction("atmospheric_condition_fraction",
                               "atmospheric_condition_flag_pixels",
                               "total_pixels",
                               "surface_conditions_flag_pixels",
                               "0.1".equals(getResolutionString(temporalClass, false)) ? 11 : 5);
        calculateFireWeighted("fire_weighted_pixels",
                              "fire_pixels",
                              "atmospheric_condition_fraction");
        for (String name: variableData.keySet()) {
            final Variable variable = fileWriter.findVariable(name);
            try {
                fileWriter.write(variable, variableData.get(name));
            } catch (InvalidRangeException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    private void calculateCloudFraction(String fractionBand, String cloudBand, String pixelBand, String waterBand, int windowSize) {
        final Array fractionArray = variableData.get(fractionBand);
        final Array cloudArray = variableData.get(cloudBand);
        final Array pixelArray = variableData.get(pixelBand);
        final Array waterArray = variableData.get(waterBand);
        final int[] shape = cloudArray.getShape();
        final int width = shape[2];
        final int height = shape[1];
        final int windowOffset = windowSize / 2;
        final Index index = cloudArray.getIndex();
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int sumClouds = 0;
                int sumLand = 0;
                for (int wy = -windowOffset; wy <= windowOffset; wy++) {
                    for (int wx = -windowOffset; wx <= windowOffset; wx++) {
                        final int readY = y + wy;
                        final int readX = x + wx;
                        if (readY >= 0 && readY < height && readX >= 0 && readX < width) {
                            index.set(0, readY, readX);
                            final int clouds = cloudArray.getInt(index);
                            final int pixels = pixelArray.getInt(index);
                            final int waters = waterArray.getInt(index);
                            if (clouds > 0 && waters < pixels) {
                                sumClouds += clouds;
                                sumLand += pixels - waters;
                            }
                        }
                    }
                }
                index.set(0, y, x);
                if (sumClouds > 0 && sumLand > 0) {
                    fractionArray.setFloat(index, sumClouds / (float) sumLand);
                } else {
                    fractionArray.setFloat(index, 0.0f);
                }
            }
        }
    }

    private void calculateFireWeighted(String weightedBand, String fireBand, String fractionBand) {
        final Array weightedArray = variableData.get(weightedBand);
        final Array fireArray = variableData.get(fireBand);
        final Array fractionArray = variableData.get(fractionBand);
        final int[] shape = fireArray.getShape();
        final int width = shape[2];
        final int height = shape[1];
        final Index index = fireArray.getIndex();
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                index.set(0, y, x);
                final int fire = fireArray.getInt(index);
                final float fraction = fractionArray.getFloat(index);
                if (fraction <= WEIGHTED_THRESHOLD) {
                    weightedArray.setFloat(index, fire / (1.0f - fraction));
                } else {
                    weightedArray.setFloat(index, -fraction / fraction);
                }
            }
        }
    }

    private void calculateUncertainty(String frpUncSumName, String fireCountName) {
        final Array uncertainties = variableData.get(frpUncSumName);
        final Array fireCounts = variableData.get(fireCountName);

        for (int i = 0; i < uncertainties.getSize(); i++) {
            float uncertainty = Float.NaN;
            final int numFirePixels = fireCounts.getInt(i);
            if (numFirePixels > 0) {
                final float squaredUncertainty = uncertainties.getInt(i);
                if (squaredUncertainty > 0.f) {
                    uncertainty = (float) (Math.sqrt(squaredUncertainty) / numFirePixels);
                }
            }

            uncertainties.setFloat(i, uncertainty);
        }
    }

    enum ProductTemporalClass {
        UNKNOWN,
        DAILY,
        CYCLE,
        MONTHLY
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
