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
    private static final float WEIGHTED_THRESHOLD = 0.9f;

    private Map<String, VariableTemplate> variableTemplates;
    private Map<String, Array> variableData;
    private List<String> bandsToIgnore;
    private List<String> bandsNotToBeWritten;
    private NetcdfFileWriter fileWriter;
    private ProductType type;
    private boolean dataWritten;

    FrpL3ProductWriter(ProductWriterPlugIn writerPlugIn) {
        super(writerPlugIn);

        fileWriter = null;
        type = ProductType.UNKNOWN;

        createVariableTemplates();
        //initializeBandNameLists();
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

    static void addGlobalMetadata(NetcdfFileWriter fileWriter, Product product, ProductType type) {
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
        fileWriter.addGlobalAttribute("platform", "Sentinel-3");
        fileWriter.addGlobalAttribute("sensor", "SLSTR");
        fileWriter.addGlobalAttribute("spatial_resolution", getResolutionString(type, true));
        fileWriter.addGlobalAttribute("geospatial_lon_units", "degrees_east");
        fileWriter.addGlobalAttribute("geospatial_lat_units", "degrees_north");
        fileWriter.addGlobalAttribute("geospatial_lon_resolution", getResolutionString(type, false));
        fileWriter.addGlobalAttribute("geospatial_lat_resolution", getResolutionString(type, false));
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

    static Array writeFillValue(Array array) {
        final DataType dataType = array.getDataType();
        if (dataType == DataType.UINT) {
            for (int i = 0; i < array.getSize(); i++) {
                array.setInt(i, CF.FILL_UINT);
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

    static ProductType getProductType(Product product) {
        final ProductData.UTC startTime = product.getStartTime();
        final ProductData.UTC endTime = product.getEndTime();
        if (startTime != null && endTime != null) {
            final Date startDate = startTime.getAsDate();
            final Date endDate = endTime.getAsDate();

            final long between = ChronoUnit.DAYS.between(startDate.toInstant(), endDate.toInstant());
            if (between >= 0 && between <= 1) {
                return ProductType.DAILY;
            } else if (between >= 26 && between <= 27) {
                return ProductType.CYCLE;
            } else if (between >= 27) {
                return ProductType.MONTHLY;
            }
        }
        return ProductType.UNKNOWN;
    }

    static String getCoverageString(ProductType productType) {
        if (productType == ProductType.DAILY) {
            return "P1D";
        } else if (productType == ProductType.CYCLE) {
            return "P27D";
        } else if (productType == ProductType.MONTHLY) {
            return "P1M";
        }
        return "UNKNOWN";
    }

    static String getResolutionString(ProductType productType, boolean addUnits) {
        String resolutionString;

        if (productType == ProductType.DAILY || productType == ProductType.CYCLE) {
            resolutionString = "0.1";
        } else if (productType == ProductType.MONTHLY) {
            resolutionString = "0.25";
        } else {
            return "UNKNOWN";
        }

        if (addUnits) {
            resolutionString += " degrees";
        }

        return resolutionString;
    }

    static int calculateSum(int[] windowData) {
        int sum = 0;
        boolean hasData = false;
        for (int windowValue : windowData) {
            if (windowValue >= 0) {
                sum += windowValue;
                hasData = true;
            }
        }
        if (hasData) {
            return sum;
        }
        return -1;
    }

    static String getSummaryText(ProductType productType) {
        final StringBuilder summary = new StringBuilder();
        summary.append("The Copernicus Climate Change Service issues three Level 3 Fire Radiative Power (FRP) Products, each generated from Level 2 Sentinel-3 Active Fire Detection and FRP Products issued in NTC mode, which themselves are based on Sentinel 3 SLSTR data. ");

        if (productType == ProductType.DAILY) {
            summary.append("The global Level 3 Daily FRP Products synthesise global data from the Level 2 AF Detection and FRP Product granules at 0.1 degree spatial and at 1-day temporal resolution");
        } else if (productType == ProductType.CYCLE){
            summary.append("The global Level 3 27-Day FRP Products synthesise global data from the Level 2 AF Detection and FRP Product granules at 0.1 degree spatial and at 27-day temporal resolution");
        } else if (productType == ProductType.MONTHLY){
            summary.append("The global Level 3 Monthly Summary FRP Products synthesise global data from the Level 2 AF Detection and FRP Product granules at 0.25 degree spatial and at 1 month temporal resolution");
        } else {
            throw new IllegalArgumentException("Invalid target product type");
        }

        summary.append(", and also provide some adjustments for cloud cover variation since clouds can mask actively burning fires from view. These products are primarily designed for ease of use of the key information coming from individual granule-based Level 2 Products, for example in global modelling, trend analysis and model evaluation.");
        return summary.toString();
    }

    private void createVariableTemplates() {
        variableTemplates = new HashMap<>();
        variableTemplates.put("s3a_night_pixel_sum", new VariableTemplate("s3a_night_pixel", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime pixels"));
        variableTemplates.put("s3a_night_cloud_sum", new VariableTemplate("s3a_night_cloud", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime cloudy pixels"));
        variableTemplates.put("s3a_night_water_sum", new VariableTemplate("s3a_night_water", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime water pixels"));
        variableTemplates.put("s3a_night_fire_sum", new VariableTemplate("s3a_night_fire", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3A nighttime active fire pixels"));
        variableTemplates.put("s3a_night_frp_mean", new VariableTemplate("s3a_night_frp", DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3A during nighttime"));
        variableTemplates.put("s3a_night_frp_unc_sum", new VariableTemplate("s3a_night_frp_unc", DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power uncertainty measured by S3A during nighttime"));
        variableTemplates.put("s3a_night_cloud_fraction_sum", new VariableTemplate("s3a_night_cloud_fraction", DataType.FLOAT, Float.NaN, "1", "Mean cloud fraction of S3A land pixels in a macro pixel of 1.1 (1.25) degrees"));
        variableTemplates.put("s3a_night_fire_weighted_sum", new VariableTemplate("s3a_night_fire_weighted", DataType.FLOAT, Float.NaN, "1", "Number of S3A nighttime active fire pixels weighted by cloud fraction"));
        variableTemplates.put("s3b_night_pixel_sum", new VariableTemplate("s3b_night_pixel", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime pixels"));
        variableTemplates.put("s3b_night_cloud_sum", new VariableTemplate("s3b_night_cloud", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime cloudy pixels"));
        variableTemplates.put("s3b_night_water_sum", new VariableTemplate("s3b_night_water", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime water pixels"));
        variableTemplates.put("s3b_night_fire_sum", new VariableTemplate("s3b_night_fire", DataType.UINT, CF.FILL_UINT, "1", "Total number of S3B nighttime active fire pixels"));
        variableTemplates.put("s3b_night_frp_mean", new VariableTemplate("s3b_night_frp", DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power measured by S3B during nighttime"));
        variableTemplates.put("s3b_night_frp_unc_sum", new VariableTemplate("s3b_night_frp_unc", DataType.FLOAT, Float.NaN, "MW", "Mean Fire Radiative Power uncertainty measured by S3B during nighttime"));
        variableTemplates.put("s3b_night_cloud_fraction_sum", new VariableTemplate("s3b_night_cloud_fraction", DataType.FLOAT, Float.NaN, "1", "Mean cloud fraction of S3B land pixels in a macro pixel of 1.1 (1.25) degrees"));
        variableTemplates.put("s3b_night_fire_weighted_sum", new VariableTemplate("s3b_night_fire_weighted", DataType.FLOAT, Float.NaN, "1", "Number of S3B nighttime active fire pixels weighted by cloud fraction"));
    }

    private void initializeBandNameLists() {
        bandsToIgnore = new ArrayList<>();
        bandsToIgnore.add("num_obs");
        bandsToIgnore.add("num_passes");
        bandsToIgnore.add("s3a_night_frp_sigma");
        bandsToIgnore.add("s3b_night_frp_sigma");

        bandsNotToBeWritten = new ArrayList<>();
        if (type == ProductType.CYCLE || type == ProductType.MONTHLY) {
            bandsNotToBeWritten.add("s3a_night_frp_unc");
            bandsNotToBeWritten.add("s3b_night_frp_unc");
            bandsNotToBeWritten.add("s3a_night_frp_unc_sum");
            bandsNotToBeWritten.add("s3b_night_frp_unc_sum");
        }
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
        type = getProductType(sourceProduct);
        addDimensions(fileWriter, sourceProduct);
        addGlobalMetadata(fileWriter, sourceProduct, type);
        addAxesAndBoundsVariables(fileWriter);

        initializeBandNameLists();
        addProductVariables(sourceProduct);
        addFractionAndWeightedVariables();

        fileWriter.create();

        writeAxesAndBoundsVariables(sourceProduct);

        dataWritten = false;
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

    private void addFractionAndWeightedVariables() {
        final Product sourceProduct = getSourceProduct();
        final int sceneRasterWidth = sourceProduct.getSceneRasterWidth();
        final int sceneRasterHeight = sourceProduct.getSceneRasterHeight();
        final int[] dimensions = {1, sceneRasterHeight, sceneRasterWidth};
        addWeightedVariable(dimensions, "s3a_night_cloud_fraction", "Mean cloud fraction of S3A land pixels in a macro pixel of 1.1 (1.25) degrees", "1");
        addWeightedVariable(dimensions, "s3b_night_cloud_fraction", "Mean cloud fraction of S3B land pixels in a macro pixel of 1.1 (1.25) degrees", "1");
        addWeightedVariable(dimensions, "s3a_night_fire_weighted", "Number of S3A nighttime active fire pixels weighted by cloud fraction", "1");
        addWeightedVariable(dimensions, "s3b_night_fire_weighted", "Number of S3B nighttime active fire pixels weighted by cloud fraction", "1");
    }

    private void addWeightedVariable(int[] dimensions, String name, String longName, String units) {
        final Variable variable = fileWriter.addVariable(name, DataType.FLOAT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, Float.NaN));
        variable.addAttribute(new Attribute(CF.UNITS, units));
        variable.addAttribute(new Attribute(CF.LONG_NAME, longName));
        final Array dataArray = Array.factory(DataType.FLOAT, dimensions);
        variableData.put(name, writeFillValue(dataArray));
    }

    private void addWeightedVariable_uint(int[] dimensions, String name, String longName, String units) {
        final Variable variable = fileWriter.addVariable(name, DataType.UINT, DIM_STRING);
        variable.addAttribute(new Attribute(CF.FILL_VALUE, CF.FILL_UINT, true));
        variable.addAttribute(new Attribute(CF.UNITS, units));
        variable.addAttribute(new Attribute(CF.LONG_NAME, longName));
        final Array dataArray = Array.factory(DataType.UINT, dimensions);
        variableData.put(name, writeFillValue(dataArray));
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
            if (!bandsNotToBeWritten.contains(bandName)) {
                final Variable variable = fileWriter.addVariable(template.name, template.dataType, DIM_STRING);
                variable.addAttribute(new Attribute(CF.FILL_VALUE, template.fillValue, template.dataType.isUnsigned()));
                variable.addAttribute(new Attribute(CF.UNITS, template.units));
                variable.addAttribute(new Attribute(CF.LONG_NAME, template.longName));
            }

            final Array dataArray = Array.factory(template.dataType, new int[]{1, sceneRasterHeight, sceneRasterWidth});
            variableData.put(template.name, writeFillValue(dataArray));
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

    private void writeVariableData() throws IOException {
        if (dataWritten) {
            return;
        }

        for (String sensor : new String[] { "s3a_", "s3b_" }) {
            if (variableData.containsKey(sensor + "night_frp_unc")) {
                calculateUncertainty(sensor + "night_frp_unc", sensor + "night_fire");
            }
            if (variableData.containsKey(sensor + "night_cloud_fraction")) {
                calculateCloudFraction(sensor + "night_cloud_fraction",
                                       sensor + "night_cloud",
                                       sensor + "night_pixel",
                                       sensor + "night_water",
                                       "0.1".equals(getResolutionString(type, false)) ? 11 : 5);
                if (variableData.containsKey(sensor + "night_fire_weighted")) {
                    calculateFireWeighted(sensor + "night_fire_weighted",
                                          sensor + "night_fire",
                                          sensor + "night_cloud_fraction");
                }
            }
        }

        try {
            final Set<Map.Entry<String, Array>> entries = variableData.entrySet();
            for (final Map.Entry<String, Array> entry : entries) {
                final String variableName = entry.getKey();
                if (bandsToIgnore.contains(variableName) || bandsNotToBeWritten.contains(variableName)) {
                    continue;
                }

                final Variable variable = fileWriter.findVariable(variableName);

                fileWriter.write(variable, entry.getValue());
            }
        } catch (InvalidRangeException e) {
            throw new IOException(e.getMessage());
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
                    uncertainty = (float) (Math.sqrt(squaredUncertainty)/ numFirePixels);
                }
            }

            uncertainties.setFloat(i, uncertainty);
        }
    }

    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            flush();
            fileWriter.close();
            fileWriter = null;
        }
        type = ProductType.UNKNOWN;
    }

    @Override
    public boolean shouldWrite(ProductNode node) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public boolean isIncrementalMode() {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void setIncrementalMode(boolean enabled) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void deleteOutput() throws IOException {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void removeBand(Band band) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void setFormatName(String formatName) {
        throw new IllegalStateException("not implemented");
    }

    VariableTemplate getTemplate(String variableName) {
        final VariableTemplate variableTemplate = variableTemplates.get(variableName);
        if (variableTemplate == null) {
            throw new IllegalArgumentException("Unsupported variable: " + variableName);
        }
        return variableTemplate;
    }

    enum ProductType {
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
