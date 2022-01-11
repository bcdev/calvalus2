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

import com.bc.calvalus.commons.DateUtils;
import org.esa.snap.core.dataio.EncodeQualification;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.image.ImageManager;
import org.esa.snap.dataio.netcdf.AbstractNetCdfWriterPlugIn;
import org.esa.snap.dataio.netcdf.ProfileWriteContext;
import org.esa.snap.dataio.netcdf.metadata.ProfileInitPartWriter;
import org.esa.snap.dataio.netcdf.metadata.ProfilePartWriter;
import org.esa.snap.dataio.netcdf.metadata.profiles.beam.BeamGeocodingPart;
import org.esa.snap.dataio.netcdf.nc.NFileWriteable;
import org.esa.snap.dataio.netcdf.nc.NVariable;
import org.esa.snap.dataio.netcdf.nc.NWritableFactory;
import org.esa.snap.dataio.netcdf.util.Constants;
import org.esa.snap.dataio.netcdf.util.DataTypeUtils;
import ucar.ma2.ArrayByte;

import java.awt.Dimension;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
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

    @Override
    public EncodeQualification getEncodeQualification(Product product) {
        return EncodeQualification.FULL;
    }

    private class LCMainPart implements ProfileInitPartWriter {

        private final SimpleDateFormat COMPACT_ISO_FORMAT = DateUtils.createDateFormat("yyyyMMdd'T'HHmmss'Z'");

        private LcL3SensorConfig sensorConfig = null;

        @Override
        public void writeProductBody(ProfileWriteContext ctx, Product product) throws IOException {
            String sensor = product.getMetadataRoot().getAttributeString("sensor");
            String platform = product.getMetadataRoot().getAttributeString("platform");
            String spatialResolution = product.getMetadataRoot().getAttributeString("spatialResolution");
            String temporalResolution = product.getMetadataRoot().getAttributeString("temporalResolution");
            String version = product.getMetadataRoot().getAttributeString("version");
            if (sensorConfig == null) {
                sensorConfig = LcL3SensorConfig.create(sensor, spatialResolution);
            }
            int tileY = product.getMetadataRoot().getAttributeInt("tileY");
            int tileX = product.getMetadataRoot().getAttributeInt("tileX");
            //float macroTileSize = "MSI".equals(sensor) ? 2.5f : 5.0f;
            float macroTileSize = "MSI".equals(sensor) || "AGRI".equals(sensor) ? 1.0f : 5.0f;
            float latMax = 90.0f - macroTileSize * tileY;
            float latMin = latMax - macroTileSize;
            float lonMin = -180.0f + macroTileSize * tileX;
            float lonMax = lonMin + macroTileSize;

            String tileName = "MSI".equals(sensor) || "AGRI".equals(sensor) ? LcL3Nc4MosaicProductFactory.tileName3(tileY, tileX) : LcL3Nc4MosaicProductFactory.tileName(tileY, tileX);
            String source = "MERIS".equals(sensor) ? "300m".equals(spatialResolution) ? "MERIS FR L1B v2013" : "MERIS RR L1B r03" : "SPOT".equals(sensor) ? "SPOT VGT P format V1.7" : "VEGETATION".equals(sensor) ? "PROBA-V S1 TOC V101" : "PROBAV".equals(sensor) ? "PROBA-V S1 TOC V101" : "MSI".equals(sensor) || "AGRI".equals(sensor) ? "Sentinel 2 MSI L1C" : "NOAA AVHRR HRPT L1B";
            String spatialResolutionDegrees = "300m".equals(spatialResolution) ? "0.002778" : "333m".equals(spatialResolution) ? "0.002976" : "20m".equals(spatialResolution) ? "0.0001852" : "1000m".equals(spatialResolution) ? "0.0133344" : "0.011112";
            NFileWriteable writeable = ctx.getNetcdfFileWriteable();

            // global attributes
            writeable.addGlobalAttribute("title", "ESA CCI land cover surface reflectance " + temporalResolution.substring(1, temporalResolution.length()-1) + " day composite");
            writeable.addGlobalAttribute("summary", "This dataset contains a tile of a Level-3 " + temporalResolution.substring(1, temporalResolution.length()-1) + "-day global surface reflectance composite from satellite observations placed onto a regular grid.");
            writeable.addGlobalAttribute("project", "Climate Change Initiative - European Space Agency");
            writeable.addGlobalAttribute("references", "http://www.esa-landcover-cci.org/");
            writeable.addGlobalAttribute("institution", "Brockmann Consult GmbH");
            writeable.addGlobalAttribute("contact", "info@brockmann-consult.de");
            writeable.addGlobalAttribute("source", source);
            if ("AVHRR".equals(sensor)) {
                writeable.addGlobalAttribute("history", "INPUT AVHRR_HRPT_L1B_NOAA11+14\n" +
                        "QA beam-watermask 1.3.4 watermask resolution=150\n" +
                        "QA lc-l3 2.0 lc.avhrr.qa\n" +
                        "QA beam 5.0.1\n" +
                        "SDR cdo 1.6.2 cdo mergetime,inttime,merge,remapbil era-interim\n" +
                        "SDR cdo 1.6.2 cdo mergetime -shifttime,inttime,remapbil aerosol-climatology\n" +
                        "SDR lc-l3 2.0 AddElevation\n" +
                        "SDR idepix 2.2.16-SNAPSHOT idepix.avhrrac \n" +
                        "SDR beam-avhrr-ac 1.0.8 py_avhrr_ac_lccci_oo era-interim,aerosol,processing_mask,uncertainty_climatology \n" +
                        "SDR beam 5.0.1\n" +
                        "SR Calvalus 2.7-SNAPSHOT LCL3 temporalCloudRadius=10d,mainBorderWidth=700\n" +
                        "SR beam 5.0.1\n" +
                        "SR netcdf-bin 4.1.3 nccopy -k 4");
            } else if ("MSI".equals(sensor)) {
                writeable.addGlobalAttribute("history", "INPUT Sentinel 2 MSI L1C\n" +
                        "Resample referenceBand=B5,downsampling=Mean\n" +
                        "IdePix 2.2 computeCloudBufferForCloudAmbiguous=false\n" +
                        "S2AC 1.0\n" +
                        "S2TBX 5.0-SNAPSHOT\n" +
                        "SNAP 5.0-SNAPSHOT\n" +
                        "SR Calvalus 2.10-SNAPSHOT LCL3\n" +
                        "SR SNAP 5.0-SNAPSHOT\n" +
                        "SR netcdf-bin 4.1.3 nccopy -k 4");
            } else if ("AGRI".equals(sensor)) {
                writeable.addGlobalAttribute("history", "INPUT Sentinel 2 MSI L1C\n" +
                        "Sen2Agri 1.7\n" +
                        "Resample referenceBand=B2,upsampling=Nearest\n" +
                        "S2TBX 6.0-SNAPSHOT\n" +
                        "SNAP 6.0-SNAPSHOT\n" +
                        "SR Calvalus 2.15-SNAPSHOT LCL3\n" +
                        "SR SNAP 6.0\n" +
                        "SR netcdf-bin 4.1.3 nccopy -k 4");
            } else if ("VEGETATION".equals(sensor) || "PROBAV".equals(sensor)) {
                writeable.addGlobalAttribute("history", "INPUT PROBA-V S1 TOC\n" +
//                        "Resample resolution="+spatialResolution+",upsampling=Nearest\n" +
                        "SNAP 7.0-SNAPSHOT\n" +
                        "SR Calvalus 2.15-SNAPSHOT LCL3\n" +
                        "SR SNAP 7.0\n" +
                        "SR netcdf-bin 4.1.3 nccopy -k 4");
            } else {
                writeable.addGlobalAttribute("history", "amorgos-4,0, sen2agri-1.7, lc-sr-2.1");  // versions
            }
            writeable.addGlobalAttribute("comment", "");

            writeable.addGlobalAttribute("Conventions", "CF-1.6");
            writeable.addGlobalAttribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Standard Names version 18");
            writeable.addGlobalAttribute("keywords", "satellite,observation,reflectance");
            writeable.addGlobalAttribute("keywords_vocabulary", "NASA Global Change Master Directory (GCMD) Science Keywords");
            writeable.addGlobalAttribute("license", "ESA CCI Data Policy: free and open access");
            writeable.addGlobalAttribute("naming_authority", "org.esa-cci");
            writeable.addGlobalAttribute("cdm_data_type", "grid");

            writeable.addGlobalAttribute("platform", platform);
            writeable.addGlobalAttribute("sensor", "AGRI".equals(sensor) ? "MSI" : "PROBAV".equals(sensor) ? "VEGETATION" : sensor);
            writeable.addGlobalAttribute("type", "SR-" + spatialResolution + "-" + temporalResolution);
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
            writeable.addGlobalAttribute("time_coverage_duration", temporalResolution);
            writeable.addGlobalAttribute("time_coverage_resolution", temporalResolution);

            writeable.addGlobalAttribute("geospatial_lat_min", String.valueOf(latMin));
            writeable.addGlobalAttribute("geospatial_lat_max", String.valueOf(latMax));
            writeable.addGlobalAttribute("geospatial_lon_min", String.valueOf(lonMin));
            writeable.addGlobalAttribute("geospatial_lon_max", String.valueOf(lonMax));
            writeable.addGlobalAttribute("spatial_resolution", spatialResolution);
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
            final ArrayByte.D1 valids = new ArrayByte.D1(10, false);
            valids.set(0, (byte) 0);
            valids.set(1, (byte) 1);
            valids.set(2, (byte) 2);
            valids.set(3, (byte) 3);
            valids.set(4, (byte) 4);
            valids.set(5, (byte) 5);
            valids.set(6, (byte) 11);
            valids.set(7, (byte) 12);
            valids.set(8, (byte) 14);
            valids.set(9, (byte) 15);
            variable.addAttribute("flag_values", valids);
            variable.addAttribute("flag_meanings", "invalid clear_land clear_water clear_snow_ice cloud cloud_shadow haze bright temporal_cloud dark");
            variable.addAttribute("valid_min", 0);
            variable.addAttribute("valid_max", 15);
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
            List<String> srMeanBandNames = sensorConfig.getMeanBandNames();
            List<String> srSigmaBandNames = sensorConfig.getUncertaintyBandNames();

            for (int i=0; i<srMeanBandNames.size(); i++) {
                String meanBandName = srMeanBandNames.get(i);
                int bandIndex = product.getBand(meanBandName).getSpectralBandIndex();
                float wavelength = product.getBand(meanBandName).getSpectralWavelength();
                variable = writeable.addVariable(meanBandName, DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
                if (! meanBandName.startsWith("bt_")) {
                    variable.addAttribute("long_name", "normalised (averaged) surface reflectance of channel " + bandIndex);
                    variable.addAttribute("standard_name", "surface_bidirectional_reflectance");
                    variable.addAttribute("wavelength", wavelength);
                    variable.addAttribute("valid_min", 0.0f);
                    variable.addAttribute("valid_max", 1.0f);
                    variable.addAttribute("units", "1");
                } else {
                    variable.addAttribute("long_name", "top-of-atmosphere brightness temperature of channel " + bandIndex);
                    variable.addAttribute("standard_name", "toa_brightness_temperature");
                    variable.addAttribute("wavelength", wavelength);
                    variable.addAttribute("valid_min", 0.0f);
                    variable.addAttribute("valid_max", 400.0f);
                    variable.addAttribute("units", "K");

                }
                variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, Float.NaN);
                if (i < srSigmaBandNames.size()) {
                    String sigmaBandName = srSigmaBandNames.get(i);
                    variable.addAttribute("ancillary_variables", sigmaBandName + " " + ancillaryVariables.toString());
                    variable = writeable.addVariable(sigmaBandName, DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
                    variable.addAttribute("long_name", "uncertainty of normalised surface reflectance of channel " + bandIndex);
                    variable.addAttribute("standard_name", "surface_bidirectional_reflectance standard_error");
                    variable.addAttribute("wavelength", wavelength);
                    variable.addAttribute("valid_min", 0.0f);
                    variable.addAttribute("valid_max", 1.0f);
                    variable.addAttribute("units", "1");
                    variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, Float.NaN);
                }
                variable.addAttribute("ancillary_variables", ancillaryVariables.toString());
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
