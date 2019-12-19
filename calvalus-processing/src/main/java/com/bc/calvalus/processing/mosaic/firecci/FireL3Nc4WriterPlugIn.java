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

package com.bc.calvalus.processing.mosaic.firecci;

import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.mosaic.landcover.LcL3Nc4MosaicProductFactory;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class FireL3Nc4WriterPlugIn extends AbstractNetCdfWriterPlugIn {

    @Override
    public String[] getFormatNames() {
        return new String[]{"NetCDF4-Fire"};
    }

     @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{Constants.FILE_EXTENSION_NC};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Fire-CCI NetCDF4 products";
    }

    @Override
    public ProfilePartWriter createGeoCodingPartWriter() {
//        return new CfGeocodingPart();
        return new BeamGeocodingPart();
    }

    @Override
    public ProfileInitPartWriter createInitialisationPartWriter() {
        return new FireMainPart();
    }

    @Override
    public NFileWriteable createWritable(String outputPath) throws IOException {
        return NWritableFactory.create(outputPath, "netcdf4");
    }

    @Override
    public EncodeQualification getEncodeQualification(Product product) {
        int bandCount = getSrMeanBandNames(product.getBandNames()).size();
        bandCount += getSrMeanBandNames(product.getBandNames()).size();
        if (bandCount == 26) {
            return EncodeQualification.FULL;
        }
        return new EncodeQualification(EncodeQualification.Preservation.UNABLE);
    }

    private class FireMainPart implements ProfileInitPartWriter {

        private final SimpleDateFormat COMPACT_ISO_FORMAT = DateUtils.createDateFormat("yyyyMMdd'T'HHmmss'Z'");

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
            String source = "MERIS".equals(sensor) ? "300m".equals(spatialResolution) ? "MERIS FR L1B v2013" : "MERIS RR L1B r03" : "SPOT".equals(sensor) ? "SPOT VGT P format V1.7" : "NOAA AVHRR HRPT L1B";
            String spatialResolutionDegrees = "300m".equals(spatialResolution) ? "0.002778" : "0.011112";
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

            variable = writeable.addVariable("status", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_INT8), tileSize, dimensions);
            variable.addAttribute("long_name", "LC pixel type mask");
            variable.addAttribute("standard_name", "surface_bidirectional_reflectance status_flag");
            final ArrayByte.D1 valids = new ArrayByte.D1(6, false);
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

            String[] bandNames = product.getBandNames();
            List<String> srMeanBandNames = getSrMeanBandNames(bandNames);
            List<String> srSigmaBandNames = getSrSigmaBandNames(bandNames);

            for (int i = 0; i < srMeanBandNames.size(); i++) {
                String meanBandName = srMeanBandNames.get(i);
                int bandIndex = product.getBand(meanBandName).getSpectralBandIndex();
                float wavelength = product.getBand(meanBandName).getSpectralWavelength();
                variable = writeable.addVariable(meanBandName, DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
                variable.addAttribute("long_name", "normalised (averaged) surface reflectance of channel " + bandIndex);
                variable.addAttribute("standard_name", "surface_bidirectional_reflectance");
                variable.addAttribute("wavelength", wavelength);
                variable.addAttribute("valid_min", 0.0f);
                variable.addAttribute("valid_max", 1.0f);
                variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, Float.NaN);
                if (i < srSigmaBandNames.size()) {
                    String sigmaBandName = srSigmaBandNames.get(i);
//                    variable.addAttribute("ancillary_variables", sigmaBandName + " " + ancillaryVariables.toString());
                    variable = writeable.addVariable(sigmaBandName, DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
                    variable.addAttribute("long_name", "uncertainty of normalised surface reflectance of channel " + bandIndex);
                    variable.addAttribute("standard_name", "surface_bidirectional_reflectance standard_error");
                    variable.addAttribute("wavelength", wavelength);
                    variable.addAttribute("valid_min", 0.0f);
                    variable.addAttribute("valid_max", 1.0f);
                    variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, Float.NaN);
                }
            }

            variable = writeable.addVariable("ndvi", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
            variable.addAttribute(Constants.FILL_VALUE_ATT_NAME, Float.NaN);
            writeable.addVariable("sun_zenith", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
            writeable.addVariable("sun_azimuth", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
            writeable.addVariable("view_zenith", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
            writeable.addVariable("view_azimuth", DataTypeUtils.getNetcdfDataType(ProductData.TYPE_FLOAT32), tileSize, dimensions);
        }

    }

    private static List<String> getSrSigmaBandNames(String[] bandNames) {
        return getMatchingBandNames(bandNames, "sdr_error_\\d{1,2}");
    }

    private static List<String> getSrMeanBandNames(String[] bandNames) {
        return getMatchingBandNames(bandNames, "sdr_\\d{1,2}");
    }

    private static List<String> getMatchingBandNames(String[] bandNames, String pattern) {
        List<String> srBandNames = new ArrayList<>();
        for (String bandName : bandNames) {
            if (bandName.matches(pattern)) {
                srBandNames.add(bandName);
            }
        }
        return srBandNames;
    }
}
