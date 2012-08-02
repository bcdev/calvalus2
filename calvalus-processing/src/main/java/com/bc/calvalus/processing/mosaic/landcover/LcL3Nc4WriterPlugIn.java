/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.mosaic.landcover;

import org.esa.beam.dataio.netcdf.AbstractNetCdfWriterPlugIn;
import org.esa.beam.dataio.netcdf.ProfileWriteContext;
import org.esa.beam.dataio.netcdf.metadata.ProfileInitPartWriter;
import org.esa.beam.dataio.netcdf.metadata.ProfilePartWriter;
import org.esa.beam.dataio.netcdf.metadata.profiles.cf.CfGeocodingPart;
import org.esa.beam.dataio.netcdf.nc.NFileWriteable;
import org.esa.beam.dataio.netcdf.nc.NVariable;
import org.esa.beam.dataio.netcdf.nc.NWritableFactory;
import org.esa.beam.dataio.netcdf.util.Constants;
import org.esa.beam.dataio.netcdf.util.DataTypeUtils;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.jai.ImageManager;
import ucar.ma2.ArrayByte;

import java.awt.Dimension;
import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

public class LcL3Nc4WriterPlugIn extends AbstractNetCdfWriterPlugIn {

    @Override
    public String[] getFormatNames() {
        return new String[]{"NetCDF4-LC"};
    }

     @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{Constants.FILE_EXTENSION_NC};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Landcover-CCI NetCDF4 products";
    }

    @Override
    public ProfilePartWriter createGeoCodingPartWriter() {
        return new CfGeocodingPart();
    }

    @Override
    public ProfileInitPartWriter createInitialisationPartWriter() {
        return new LCMainPart();
    }

    @Override
    public NFileWriteable createWritable(String outputPath) throws IOException {
        return NWritableFactory.create(outputPath, "netcdf4");
    }

    private class LCMainPart implements ProfileInitPartWriter {

        private final SimpleDateFormat COMPACT_ISO_FORMAT = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        final String[] COUNTER_NAMES = {"valid", "clear_land", "clear_water", "clear_snow_ice", "cloud", "cloud_shadow"};
        final float[] WAVELENGTH = new float[]{
            412.691f, 442.55902f, 489.88202f, 509.81903f, 559.69403f,
            619.601f, 664.57306f, 680.82104f, 708.32904f, 753.37103f,
            761.50806f, 778.40906f, 864.87604f, 884.94403f, 900.00006f};

        LCMainPart() {
            COMPACT_ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        @Override
        public void writeProductBody(ProfileWriteContext ctx, Product product) throws IOException {
            String sensor = product.getMetadataRoot().getAttributeString("sensor");
            String platform = product.getMetadataRoot().getAttributeString("platform");
            String spatialResolution = product.getMetadataRoot().getAttributeString("spatialResolution");
            String temporalResolution = product.getMetadataRoot().getAttributeString("temporalResolution");
            String version = product.getMetadataRoot().getAttributeString("version");
            int tileY = product.getMetadataRoot().getAttributeInt("tileY");
            int tileX = product.getMetadataRoot().getAttributeInt("tileX");
            float latMax = 90.0f - 5.0f * tileY;
            float latMin = latMax - 5.0f;
            float lonMin = -180.0f + 5.0f * tileX;
            float lonMax = lonMin + 5.0f;

            String tileName = LcL3Nc4MosaicProductFactory.tileName(tileY, tileX);
            String source = "MERIS".equals(sensor) ? "300".equals(spatialResolution) ? "MERIS FR L1B" : "MERIS RR L1B" : "SPOT VGT";
            String spatialResolutionDegrees = "300".equals(spatialResolution) ? "0.002778" : "0.011112";
            NFileWriteable writeable = ctx.getNetcdfFileWriteable();

            // global attributes
            writeable.addGlobalAttribute("title", "ESA CCI land cover surface reflectance " + temporalResolution + " day composite");
            writeable.addGlobalAttribute("summary", "This dataset contains a tile of a Level-3 " + temporalResolution + "-day global surface reflectance composite from satellite observations placed onto a regular grid.");
            writeable.addGlobalAttribute("project", "Climate Change Initiative - European Space Agency");
            writeable.addGlobalAttribute("references", "http://www.esa-landcover-cci.org/");
            writeable.addGlobalAttribute("institution", "Brockmann Consult");
            writeable.addGlobalAttribute("contact", "lc-cci-info@brockmann-consult.de");
            writeable.addGlobalAttribute("source", source);
            writeable.addGlobalAttribute("history", "amorgos-4,0, lc-sdr-1.0, lc-sr-1.0");  // versions
            writeable.addGlobalAttribute("comment", "");

            writeable.addGlobalAttribute("Conventions", "CF-1.6");
            writeable.addGlobalAttribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Standard Names version 18");
            writeable.addGlobalAttribute("keywords", "satellite,observation,reflectance");
            writeable.addGlobalAttribute("keywords_vocabulary", "NASA Global Change Master Directory (GCMD) Science Keywords");
            writeable.addGlobalAttribute("license", "ESA CCI Data Policy: free and open access");
            writeable.addGlobalAttribute("naming_authority", "org.esa-cci");
            writeable.addGlobalAttribute("cdm_data_type", "grid");

            writeable.addGlobalAttribute("platform", platform);
            writeable.addGlobalAttribute("sensor", sensor);
            writeable.addGlobalAttribute("type", "SR-" + spatialResolution + "m-" + temporalResolution + "d");
            writeable.addGlobalAttribute("id", product.getName());
            writeable.addGlobalAttribute("tracking_id", UUID.randomUUID().toString());
            writeable.addGlobalAttribute("tile", tileName);
            writeable.addGlobalAttribute("product_version", version);
            writeable.addGlobalAttribute("date_created", COMPACT_ISO_FORMAT.format(new Date()));
            writeable.addGlobalAttribute("creator_name", "Brockmann Consult");
            writeable.addGlobalAttribute("creator_url", "http://www.brockmann-consult.de/");
            writeable.addGlobalAttribute("creator_email", "info@brockmann-consult.de");

            writeable.addGlobalAttribute("time_coverage_start", COMPACT_ISO_FORMAT.format(product.getStartTime().getAsDate()));
            writeable.addGlobalAttribute("time_coverage_end", COMPACT_ISO_FORMAT.format(product.getEndTime().getAsDate()));
            writeable.addGlobalAttribute("time_coverage_duration", "P" + temporalResolution + "D");

            writeable.addGlobalAttribute("geospatial_lat_min", String.valueOf(latMin));
            writeable.addGlobalAttribute("geospatial_lat_max", String.valueOf(latMax));
            writeable.addGlobalAttribute("geospatial_lon_min", String.valueOf(lonMin));
            writeable.addGlobalAttribute("geospatial_lon_max", String.valueOf(lonMax));
            writeable.addGlobalAttribute("spatial_resolution", spatialResolution + "m");
            writeable.addGlobalAttribute("geospatial_lat_units", "degrees_north");
            writeable.addGlobalAttribute("geospatial_lat_resolution", spatialResolutionDegrees);
            writeable.addGlobalAttribute("geospatial_lon_units", "degrees_east");
            writeable.addGlobalAttribute("geospatial_lon_resolution", spatialResolutionDegrees);

            final Dimension tileSize = ImageManager.getPreferredTileSize(product);
            writeable.addGlobalAttribute("TileSize", tileSize.height + ":" + tileSize.width);
            //TODO writeable.addDimension("time", 1);
            writeable.addDimension("lat", product.getSceneRasterHeight());
            writeable.addDimension("lon", product.getSceneRasterWidth());
            final String dimensions = writeable.getDimensions();

            NVariable variable;
            variable = writeable.addVariable("current_pixel_state", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_INT8), tileSize, dimensions);
            variable.addAttribute("long_name", "LC pixel type mask");
            variable.addAttribute("standard_name", "surface_reflectance status_flag");
            final ArrayByte.D1 valids = new ArrayByte.D1(6);
            valids.set(0, (byte) 0);
            valids.set(1, (byte) 1);
            valids.set(2, (byte) 2);
            valids.set(3, (byte) 3);
            valids.set(4, (byte) 4);
            valids.set(5, (byte) 5);
            variable.addAttribute("flag_values", valids);
            variable.addAttribute("flag_meanings", "invalid clear_land clear_water clear_snow_ice cloud cloud_shadow");
            variable.addAttribute("valid_min", 0);
            variable.addAttribute("valid_max", 5);
            variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, -1);

            for (String counter : COUNTER_NAMES) {
                variable = writeable.addVariable(counter + "_count", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_INT16), tileSize, dimensions);
                variable.addAttribute("long_name", "number of " + counter + " observations");
                variable.addAttribute("standard_name", "surface_reflectance number_of_observations");
                variable.addAttribute("valid_min", 0);
                variable.addAttribute("valid_max", 150);
                variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, -1);
            }

            for (int i=1; i<=15; ++i) {
                float wavelength = WAVELENGTH[i-1];
                variable = writeable.addVariable("sr_" + i + "_mean", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
                variable.addAttribute("long_name", "normalised surface reflectance of channel " + i);
                variable.addAttribute("standard_name", "surface_reflectance");
                variable.addAttribute("wavelength", wavelength);
                variable.addAttribute("valid_min", 0.0f);
                variable.addAttribute("valid_max", 1.0f);
                variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, -999.0f);
                variable = writeable.addVariable("sr_" + i + "_sigma", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
                variable.addAttribute("long_name", "standard deviation of normalised surface reflectance of channel " + i);
                variable.addAttribute("standard_name", "surface_reflectance standard_error");
                variable.addAttribute("wavelength", wavelength);
                variable.addAttribute("valid_min", 0.0f);
                variable.addAttribute("valid_max", 0.5f);
                variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, -999.0f);
            }

            variable = writeable.addVariable("vegetation_index_mean", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
            variable.addAttribute("long_name", "mean of vegetation index");
            variable.addAttribute("standard_name", "normalized_difference_vegetation_index");
            variable.addAttribute("valid_min", -1.0f);
            variable.addAttribute("valid_max", 1.0f);
            variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, -999.0f);

            /*
            for (RasterDataNode rasterDataNode : product.getBands()) {
                String variableName = ReaderUtils.getVariableName(rasterDataNode);

                int dataType;
                if (rasterDataNode.isLog10Scaled()) {
                    dataType = rasterDataNode.getGeophysicalDataType();
                } else {
                    dataType = rasterDataNode.getDataType();
                }
                DataType netcdfDataType = DataTypeUtils.getNetcdfDataType(dataType);
                Dimension tileSize1 = ImageManager.getPreferredTileSize(rasterDataNode.getProduct());
                final NVariable var = writeable.addVariable(variableName, netcdfDataType, tileSize1, dimensions);
                final String description = rasterDataNode.getDescription();
                if (description != null) {
                    var.addAttribute("long_name", description);
                }
//                String unit = rasterDataNode.getUnit();
//                if (unit != null) {
//                    unit = CfCompliantUnitMapper.tryFindUnitString(unit);
//                    variable.addAttribute("units", unit);
//                }
//                final boolean unsigned = CfBandPart.isUnsigned(rasterDataNode);
//                if (unsigned) {
//                    var.addAttribute("_Unsigned", String.valueOf(unsigned));
//                }

                double noDataValue;
                if (!rasterDataNode.isLog10Scaled()) {
                    final double scalingFactor = rasterDataNode.getScalingFactor();
                    if (scalingFactor != 1.0) {
                        var.addAttribute(Constants.SCALE_FACTOR_ATT_NAME, scalingFactor);
                    }
                    final double scalingOffset = rasterDataNode.getScalingOffset();
                    if (scalingOffset != 0.0) {
                        var.addAttribute(Constants.ADD_OFFSET_ATT_NAME, scalingOffset);
                    }
                    noDataValue = rasterDataNode.getNoDataValue();
                } else {
                    // scaling information is not written anymore for log10 scaled bands
                    // instead we always write geophysical values
                    // we do this because log scaling is not supported by NetCDF-CF conventions
                    noDataValue = rasterDataNode.getGeophysicalNoDataValue();
                }
                if (rasterDataNode.isNoDataValueUsed()) {
                    Number fillValue = DataTypeUtils.convertTo(noDataValue, var.getDataType());
                    var.addAttribute(Constants.FILL_VALUE_ATT_NAME, fillValue);
                }
                var.addAttribute("coordinates", "lat lon");
            }
            */

        }

        private void writeDimensions(NFileWriteable writeable, Product p, String dimY, String dimX) throws IOException {
            //writeable.addDimension("time", 1);
            writeable.addDimension(dimY, p.getSceneRasterHeight());
            writeable.addDimension(dimX, p.getSceneRasterWidth());
        }
    }
}
