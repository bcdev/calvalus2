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
import org.esa.beam.dataio.netcdf.metadata.profiles.beam.BeamGeocodingPart;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

public class LcL3Nc4WriterPlugIn extends AbstractNetCdfWriterPlugIn {

    public static final String[] COUNTER_NAMES = { "clear_land", "clear_water", "clear_snow_ice", "cloud", "cloud_shadow" /*, "valid"*/ };

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
//        return new CfGeocodingPart();
        return new BeamGeocodingPart();
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
            String source = "MERIS".equals(sensor) ? "300".equals(spatialResolution) ? "MERIS FR L1B r02" : "MERIS RR L1B r03" : "SPOT VGT P format V1.7";
            String spatialResolutionDegrees = "300".equals(spatialResolution) ? "0.002778" : "0.011112";
            NFileWriteable writeable = ctx.getNetcdfFileWriteable();

            // global attributes
            writeable.addGlobalAttribute("title", "ESA CCI land cover surface reflectance " + temporalResolution + " day composite");
            writeable.addGlobalAttribute("summary", "This dataset contains a tile of a Level-3 " + temporalResolution + "-day global surface reflectance composite from satellite observations placed onto a regular grid.");
            writeable.addGlobalAttribute("project", "Climate Change Initiative - European Space Agency");
            writeable.addGlobalAttribute("references", "http://www.esa-landcover-cci.org/");
            writeable.addGlobalAttribute("institution", "Brockmann Consult GmbH");
            writeable.addGlobalAttribute("contact", "info@brockmann-consult.de");
            writeable.addGlobalAttribute("source", source);
            writeable.addGlobalAttribute("history", "amorgos-4,0, lc-sdr-2.0, lc-sr-2.0");  // versions
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
            writeable.addGlobalAttribute("time_coverage_resolution", "P" + temporalResolution + "D");

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
            variable.addAttribute("standard_name", "surface_bidirectional_reflectance status_flag");
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
            variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, (byte)0);

            StringBuffer ancillaryVariables = new StringBuffer("current_pixel_state");
            for (String counter : COUNTER_NAMES) {
                variable = writeable.addVariable(counter + "_count", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_INT16), tileSize, dimensions);
                variable.addAttribute("long_name", "number of " + counter + " observations");
                variable.addAttribute("standard_name", "surface_bidirectional_reflectance number_of_observations");
                variable.addAttribute("valid_min", 0);
                variable.addAttribute("valid_max", 150);
                variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, (short)-1);
                ancillaryVariables.append(' ');
                ancillaryVariables.append(counter);
                ancillaryVariables.append("_count");
            }

            String[] bandNames = product.getBandNames();
            List<String> srMeanBandNames = new ArrayList<String>(bandNames.length);
            List<String> srSigmaBandNames = new ArrayList<String>(bandNames.length);
            for (String bandName : bandNames) {
                if (bandName.startsWith("sr_")) {
                    if (bandName.endsWith("_mean")) {
                        srMeanBandNames.add(bandName);
                    } else if (bandName.endsWith("_uncertainty")) {
                        srSigmaBandNames.add(bandName);
                    }
                }
            }


            for (int i=0; i<srMeanBandNames.size(); i++) {
                String meanBandName = srMeanBandNames.get(i);
                String sigmaBandName = srSigmaBandNames.get(i);
                int bandIndex = product.getBand(meanBandName).getSpectralBandIndex();
                float wavelength = product.getBand(meanBandName).getSpectralWavelength();
                variable = writeable.addVariable(meanBandName, DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
                variable.addAttribute("long_name", "normalised (averaged) surface reflectance of channel " + bandIndex);
                variable.addAttribute("standard_name", "surface_bidirectional_reflectance");
                variable.addAttribute("wavelength", wavelength);
                variable.addAttribute("valid_min", 0.0f);
                variable.addAttribute("valid_max", 1.0f);
                variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, Float.NaN);
                variable.addAttribute("ancillary_variables", sigmaBandName + " " + ancillaryVariables.toString());
                variable = writeable.addVariable(sigmaBandName, DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
                variable.addAttribute("long_name", "uncertainty of normalised surface reflectance of channel " + bandIndex);
                variable.addAttribute("standard_name", "surface_bidirectional_reflectance standard_error");
                variable.addAttribute("wavelength", wavelength);
                variable.addAttribute("valid_min", 0.0f);
                variable.addAttribute("valid_max", 1.0f);
                variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, Float.NaN);
            }

            variable = writeable.addVariable("vegetation_index_mean", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
            variable.addAttribute("long_name", "mean of vegetation index");
            variable.addAttribute("standard_name", "normalized_difference_vegetation_index");
            variable.addAttribute("valid_min", -1.0f);
            variable.addAttribute("valid_max", 1.0f);
            variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, Float.NaN);
            variable.addAttribute("ancillary_variables", ancillaryVariables.toString());
        }

        private void writeDimensions(NFileWriteable writeable, Product p, String dimY, String dimX) throws IOException {
            //writeable.addDimension("time", 1);
            writeable.addDimension(dimY, p.getSceneRasterHeight());
            writeable.addDimension(dimX, p.getSceneRasterWidth());
        }
    }
}
