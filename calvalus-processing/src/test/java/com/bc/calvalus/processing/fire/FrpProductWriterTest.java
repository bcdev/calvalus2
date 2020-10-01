package com.bc.calvalus.processing.fire;

import org.esa.snap.core.datamodel.Product;
import org.junit.Test;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class FrpProductWriterTest {

    @Test
    public void testGetOutputPath() {
        final String expected = "/the/output/path/a_file.nc";

        String outputPath = FrpL3ProductWriter.getOutputPath(expected);
        assertEquals(expected, outputPath);

        final File expectedFile = new File(expected);
        outputPath = FrpL3ProductWriter.getOutputPath(expectedFile);
        assertEquals(expectedFile.getAbsolutePath(), outputPath);
    }

    @Test
    public void testGetOutputPath_invalid_class() {
        try {
            FrpL3ProductWriter.getOutputPath(34.0);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testAddDimensions() {
        final NetcdfFileWriter fileWriter = mock(NetcdfFileWriter.class);
        final Product product = new Product("what", "ever", 5, 7);

        FrpL3ProductWriter.addDimensions(fileWriter, product);

        verify(fileWriter, times(1)).addDimension("time", 1);
        verify(fileWriter, times(1)).addDimension("lon", 5);
        verify(fileWriter, times(1)).addDimension("lat", 7);
        verify(fileWriter, times(1)).addDimension("bounds", 2);
        verifyNoMoreInteractions(fileWriter);
    }

    @Test
    public void testAddGlobalMetadata() {
        final NetcdfFileWriter fileWriter = mock(NetcdfFileWriter.class);
        final Product product = new Product("C3S-FRP-L3-Map-0.25deg-P1D-2020-09-16-v1.0.nc", "whatever", 5, 7);

        FrpL3ProductWriter.addGlobalMetadata(fileWriter, product);

        verify(fileWriter, times(1)).addGlobalAttribute("title", "ECMWF C3S Gridded OLCI Fire Radiative Power product");
        verify(fileWriter, times(1)).addGlobalAttribute("institution", "King's College London, Brockmann Consult GmbH");
        verify(fileWriter, times(1)).addGlobalAttribute("source", "ESA Sentinel-3 A+B SLSTR FRP");
        verify(fileWriter, times(1)).addGlobalAttribute(eq("history"), anyString());
        verify(fileWriter, times(1)).addGlobalAttribute("references", "See https://climate.copernicus.eu/");
        verify(fileWriter, times(1)).addGlobalAttribute(eq("tracking_id"), anyString());
        verify(fileWriter, times(1)).addGlobalAttribute("Conventions", "CF-1.7");
        verify(fileWriter, times(1)).addGlobalAttribute("summary", "TODO!");
        verify(fileWriter, times(1)).addGlobalAttribute("keywords", "Fire Radiative Power, Climate Change, ESA, C3S, GCOS");
        verify(fileWriter, times(1)).addGlobalAttribute("id", "C3S-FRP-L3-Map-0.25deg-P1D-2020-09-16-v1.0.nc");
        verify(fileWriter, times(1)).addGlobalAttribute("naming_authority", "org.esa-cci");
        verify(fileWriter, times(1)).addGlobalAttribute("keywords_vocabulary", "NASA Global Change Master Directory (GCMD) Science keywords");
        verify(fileWriter, times(1)).addGlobalAttribute("cdm_data_type", "Grid");
        verify(fileWriter, times(1)).addGlobalAttribute("comment", "These data were produced as part of the Copernicus Climate Change Service programme.");
        verify(fileWriter, times(1)).addGlobalAttribute(eq("date_created"), anyString());
        verify(fileWriter, times(1)).addGlobalAttribute("creator_name", "Brockmann Consult GmbH");
        verify(fileWriter, times(1)).addGlobalAttribute("creator_url", "https://www.brockmann-consult.de");
        verify(fileWriter, times(1)).addGlobalAttribute("creator_email", "martin.boettcher@brockmann-consult.de");
        verify(fileWriter, times(1)).addGlobalAttribute("contact", "http://copernicus-support.ecmwf.int");
        verify(fileWriter, times(1)).addGlobalAttribute("project", "EC C3S Fire Radiative Power");
        verify(fileWriter, times(1)).addGlobalAttribute("geospatial_lat_min", "-90");
        verify(fileWriter, times(1)).addGlobalAttribute("geospatial_lat_max", "90");
        verify(fileWriter, times(1)).addGlobalAttribute("geospatial_lon_min", "-180");
        verify(fileWriter, times(1)).addGlobalAttribute("geospatial_lon_max", "180");
        verify(fileWriter, times(1)).addGlobalAttribute("geospatial_vertical_min", "0");
        verify(fileWriter, times(1)).addGlobalAttribute("geospatial_vertical_max", "0");
        // @todo 1 tb/tb implement 2020-09-25
//        time_coverage_start = 20190701T000000Z
//        time_coverage_end = 20190731T235959Z
//        time_coverage_duration = P1M
//        time_coverage_resolution = P1M
        verify(fileWriter, times(1)).addGlobalAttribute("standard_name_vocabulary", "NetCDF Climate and Forecast (CF) Metadata Convention");
        // @todo 1 tb/tb implement 2020-09-25
//        license = EC C3S FIRE BURNED AREA Data Policy
        verify(fileWriter, times(1)).addGlobalAttribute("platform", "Sentinel-3");
        verify(fileWriter, times(1)).addGlobalAttribute("sensor", "SLSTR");
        // @todo 1 tb/tb implement 2020-09-25
//        spatial_resolution = 0.25 degrees
        verify(fileWriter, times(1)).addGlobalAttribute("geospatial_lon_units", "degrees_east");
        verify(fileWriter, times(1)).addGlobalAttribute("geospatial_lat_units", "degrees_north");
        // @todo 1 tb/tb implement 2020-09-25
//        geospatial_lon_resolution = 0.25
//        geospatial_lat_resolution = 0.25

        verifyNoMoreInteractions(fileWriter);
    }

    @Test
    public void testAddAxesAndBoundsVariables() {
        final NetcdfFileWriter fileWriter = mock(NetcdfFileWriter.class);
        final Variable lonVariable = mock(Variable.class);
        when(fileWriter.addVariable("lon", DataType.FLOAT, "lon")).thenReturn(lonVariable);
        final Variable latVariable = mock(Variable.class);
        when(fileWriter.addVariable("lat", DataType.FLOAT, "lat")).thenReturn(latVariable);
        final Variable timeVariable = mock(Variable.class);
        when(fileWriter.addVariable("time", DataType.DOUBLE, "time")).thenReturn(timeVariable);

        FrpL3ProductWriter.addAxesAndBoundsVariables(fileWriter);

        verify(fileWriter, times(1)).addVariable("lon", DataType.FLOAT, "lon");
        verify(lonVariable, times(4)).addAttribute(anyObject());
        verify(fileWriter, times(1)).addVariable("lat", DataType.FLOAT, "lat");
        verify(latVariable, times(4)).addAttribute(anyObject());
        verify(fileWriter, times(1)).addVariable("time", DataType.DOUBLE, "time");
        verify(timeVariable, times(5)).addAttribute(anyObject());
        verify(fileWriter, times(1)).addVariable("lon_bounds", DataType.FLOAT, "lon bounds");
        verify(fileWriter, times(1)).addVariable("lat_bounds", DataType.FLOAT, "lat bounds");
        verify(fileWriter, times(1)).addVariable("time_bounds", DataType.DOUBLE, "time bounds");

        verifyNoMoreInteractions(fileWriter);
    }

    @Test
    public void testGetVariableTemplate() {
        final FrpL3ProductWriter writer = (FrpL3ProductWriter) new FrpL3ProductWriterPlugIn().createWriterInstance();

        FrpL3ProductWriter.VariableTemplate template = writer.getTemplate("s3a_day_pixel_sum");
        assertTemplate(DataType.UINT, "s3a_day_pixel", -1, "1", "Total number of S3A daytime pixels", template);

        template = writer.getTemplate("s3a_day_cloud_sum");
        assertTemplate(DataType.UINT, "s3a_day_cloud", -1, "1", "Total number of S3A daytime cloudy pixels", template);

        template = writer.getTemplate("s3a_day_water_sum");
        assertTemplate(DataType.UINT, "s3a_day_water", -1, "1", "Total number of S3A daytime water pixels", template);

        template = writer.getTemplate("s3a_day_fire_sum");
        assertTemplate(DataType.UINT, "s3a_day_fire", -1, "1", "Total number of S3A daytime active fire pixels", template);

        template = writer.getTemplate("s3a_day_frp_mean");
        assertTemplate(DataType.FLOAT, "s3a_day_frp", Float.NaN, "MW", "Mean Fire Radiative Power measured by S3A during daytime", template);

        template = writer.getTemplate("s3a_night_pixel_sum");
        assertTemplate(DataType.UINT, "s3a_night_pixel", -1, "1", "Total number of S3A nighttime pixels", template);

        template = writer.getTemplate("s3a_night_cloud_sum");
        assertTemplate(DataType.UINT, "s3a_night_cloud", -1, "1", "Total number of S3A nighttime cloudy pixels", template);

        template = writer.getTemplate("s3a_night_water_sum");
        assertTemplate(DataType.UINT, "s3a_night_water", -1, "1", "Total number of S3A nighttime water pixels", template);

        template = writer.getTemplate("s3a_night_fire_sum");
        assertTemplate(DataType.UINT, "s3a_night_fire", -1, "1", "Total number of S3A nighttime active fire pixels", template);

        template = writer.getTemplate("s3a_night_frp_mean");
        assertTemplate(DataType.FLOAT, "s3a_night_frp", Float.NaN, "MW", "Mean Fire Radiative Power measured by S3A during nighttime", template);

        template = writer.getTemplate("s3b_day_pixel_sum");
        assertTemplate(DataType.UINT, "s3b_day_pixel", -1, "1", "Total number of S3B daytime pixels", template);

        template = writer.getTemplate("s3b_day_cloud_sum");
        assertTemplate(DataType.UINT, "s3b_day_cloud", -1, "1", "Total number of S3B daytime cloudy pixels", template);

        template = writer.getTemplate("s3b_day_water_sum");
        assertTemplate(DataType.UINT, "s3b_day_water", -1, "1", "Total number of S3B daytime water pixels", template);

        template = writer.getTemplate("s3b_day_fire_sum");
        assertTemplate(DataType.UINT, "s3b_day_fire", -1, "1", "Total number of S3B daytime active fire pixels", template);

        template = writer.getTemplate("s3b_day_frp_mean");
        assertTemplate(DataType.FLOAT, "s3b_day_frp", Float.NaN, "MW", "Mean Fire Radiative Power measured by S3B during daytime", template);

        template = writer.getTemplate("s3b_night_pixel_sum");
        assertTemplate(DataType.UINT, "s3b_night_pixel", -1, "1", "Total number of S3B nighttime pixels", template);

        template = writer.getTemplate("s3b_night_cloud_sum");
        assertTemplate(DataType.UINT, "s3b_night_cloud", -1, "1", "Total number of S3B nighttime cloudy pixels", template);

        template = writer.getTemplate("s3b_night_water_sum");
        assertTemplate(DataType.UINT, "s3b_night_water", -1, "1", "Total number of S3B nighttime water pixels", template);

        template = writer.getTemplate("s3b_night_fire_sum");
        assertTemplate(DataType.UINT, "s3b_night_fire", -1, "1", "Total number of S3B nighttime active fire pixels", template);

        template = writer.getTemplate("s3b_night_frp_mean");
        assertTemplate(DataType.FLOAT, "s3b_night_frp", Float.NaN, "MW", "Mean Fire Radiative Power measured by S3B during nighttime", template);
    }

    private void assertTemplate(DataType dataType, String name, Number fillValue, String units, String longName, FrpL3ProductWriter.VariableTemplate template) {
        assertEquals(dataType, template.dataType);
        assertEquals(name, template.name);
        assertEquals(fillValue, template.fillValue);
        assertEquals(units, template.units);
        assertEquals(longName, template.longName);
    }

    @Test
    public void testGetVariableTemplate_invalidName() {
        final FrpL3ProductWriter writer = (FrpL3ProductWriter) new FrpL3ProductWriterPlugIn().createWriterInstance();

        try {
            writer.getTemplate("Heffalump");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testWriteFillValue() {
        final Array uintArray = Array.factory(DataType.UINT, new int[]{1, 3, 4});
        Array filledArray = FrpL3ProductWriter.writeFillValue(uintArray);
        for(int i = 0; i < uintArray.getSize(); i++) {
            assertEquals(CF.FILL_UINT, filledArray.getInt(i));
        }

        final Array floatArray = Array.factory(DataType.FLOAT, new int[]{1, 4, 5});
        filledArray = FrpL3ProductWriter.writeFillValue(floatArray);
        for(int i = 0; i < uintArray.getSize(); i++) {
            assertEquals(Float.NaN, filledArray.getFloat(i), 1e-8);
        }
    }

    @Test
    public void testWriteFillValue_unsupportedType() {
        final Array byteArray = Array.factory(DataType.BYTE, new int[]{1, 2, 2});

        try {
            FrpL3ProductWriter.writeFillValue(byteArray);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) {
        }
    }
}
