package com.bc.calvalus.processing.fire;

import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import static org.junit.Assert.*;

public class FrpProductWriterIntegrationTest {

    private File targetFile;

    @Before
    public void setUp() throws IOException {
        targetFile = File.createTempFile("frp", ".nc");
    }

    @After
    public void tearDown() {
        if (targetFile != null) {
            if (!targetFile.delete()) {
                fail("unable to delete test file");
            }
        }
    }

    @Test
    public void testWriteProductNodes_daily_cycle() throws IOException, ParseException {
        final Product product = createTestProduct_daily_cycle();

        final ProductWriter productWriter = new FrpL3ProductWriterPlugIn().createWriterInstance();

        try {
            productWriter.writeProductNodes(product, targetFile);

            final Band[] bands = product.getBands();
            for (final Band band : bands) {
                productWriter.writeBandRasterData(band, 0, 0, 8, 4, band.getRasterData(), ProgressMonitor.NULL);
            }
        } finally {
            productWriter.flush();
            productWriter.close();
            product.dispose();
        }

        try (NetcdfFile netcdfFile = NetcdfFile.open(targetFile.getAbsolutePath())) {
            ensureDimensions(netcdfFile);
            ensureGlobalAttributes_daily(netcdfFile);
            ensureAxesAndBoundsVariables_daily(netcdfFile);

            ensureVariables_daily(netcdfFile);
            ensureUncertainties_daily(netcdfFile);
            ensureWeightedFireVariables(netcdfFile);
        }
    }

    @Test
    public void testWriteProductNodes_monthly() throws IOException, ParseException {
        final Product product = createTestProduct_monthly();

        final ProductWriter productWriter = new FrpL3ProductWriterPlugIn().createWriterInstance();

        try {
            productWriter.writeProductNodes(product, targetFile);

            final Band[] bands = product.getBands();
            for (final Band band : bands) {
                productWriter.writeBandRasterData(band, 0, 0, 8, 4, band.getRasterData(), ProgressMonitor.NULL);
            }
        } finally {
            productWriter.flush();
            productWriter.close();
            product.dispose();
        }

        try (NetcdfFile netcdfFile = NetcdfFile.open(targetFile.getAbsolutePath())) {
            ensureDimensions(netcdfFile);
            ensureGlobalAttributes_monthly(netcdfFile);
            ensureAxesAndBoundsVariables_monthly(netcdfFile);

            ensureVariables_monthly(netcdfFile);
            ensureWindowedVariables(netcdfFile);
            ensureTempVariablesDropped(netcdfFile);
        }
    }

    private void ensureWeightedFireVariables(NetcdfFile netcdfFile) throws IOException {
        Variable variable = netcdfFile.findVariable("s3a_night_fire_weighted");
        assertEquals(DataType.FLOAT, variable.getDataType());
        assertEquals("1", variable.findAttribute(CF.UNITS).getStringValue());
        Array data = variable.read();
        Index index = data.getIndex();
        assertEquals(18.f, data.getFloat(index.set(0, 1, 2)), 1e-8);
        assertEquals(27.f, data.getFloat(index.set(0, 2, 3)), 1e-8);

        variable = netcdfFile.findVariable("s3b_night_fire_weighted");
        int[] shape = variable.getShape();
        assertEquals(3, shape.length);
        assertEquals(1, shape[0]);
        assertEquals(4, shape[1]);
        assertEquals(8, shape[2]);
        assertEquals("1", variable.findAttribute(CF.UNITS).getStringValue());
        data = variable.read();
        index = data.getIndex();
        assertEquals(24.f, data.getFloat(index.set(0, 0, 6)), 1e-8);
        assertEquals(33.f, data.getFloat(index.set(0, 1, 7)), 1e-8);
    }

    private void ensureVariables_daily(NetcdfFile netcdfFile) throws IOException {
        Variable variable = netcdfFile.findVariable("s3a_night_fire");
        assertEquals(DataType.UINT, variable.getDataType());
        assertEquals("1", variable.findAttribute(CF.UNITS).getStringValue());
        Array data = variable.read();
        Index index = data.getIndex();
        assertEquals(36, data.getInt(index.set(0, 3, 4)));
        assertEquals(13, data.getInt(index.set(0, 0, 5)));

        variable = netcdfFile.findVariable("s3b_night_frp");
        assertEquals(DataType.FLOAT, variable.getDataType());
        assertEquals("Mean Fire Radiative Power measured by S3B during nighttime", variable.findAttribute(CF.LONG_NAME).getStringValue());
        int[] shape = variable.getShape();
        assertEquals(3, shape.length);
        assertEquals(1, shape[0]);
        assertEquals(4, shape[1]);
        assertEquals(8, shape[2]);
        data = variable.read();
        index = data.getIndex();
        assertEquals(33.f, data.getFloat(index.set(0, 1, 6)), 1e-8);
        assertEquals(42.f, data.getFloat(index.set(0, 2, 7)), 1e-8);

        variable = netcdfFile.findVariable("s3b_night_water");
        assertEquals(DataType.UINT, variable.getDataType());
        assertEquals(-1, variable.findAttribute(CF.FILL_VALUE).getNumericValue());
        data = variable.read();
        index = data.getIndex();
        assertEquals(41, data.getInt(index.set(0, 3, 0)));
        assertEquals(18, data.getInt(index.set(0, 0, 1)));
    }

    private void ensureUncertainties_daily(NetcdfFile netcdfFile) throws IOException {
        Variable variable = netcdfFile.findVariable("s3a_night_frp_unc");
        assertEquals(DataType.FLOAT, variable.getDataType());
        assertEquals("MW", variable.findAttribute(CF.UNITS).getStringValue());
        Array data = variable.read();
        Index index = data.getIndex();
        assertEquals(0.30932024121284485, data.getFloat(index.set(0, 1, 2)), 1e-8);
        assertEquals(0.23424279689788818, data.getFloat(index.set(0, 2, 3)), 1e-8);

        variable = netcdfFile.findVariable("s3b_night_frp_unc");
        assertEquals(DataType.FLOAT, variable.getDataType());
        assertEquals("Mean Fire Radiative Power uncertainty measured by S3B during nighttime", variable.findAttribute(CF.LONG_NAME).getStringValue());
        data = variable.read();
        index = data.getIndex();
        assertEquals(0.23006533086299896, data.getFloat(index.set(0, 0, 5)), 1e-8);
        assertEquals(0.190086334943771360, data.getFloat(index.set(0, 1, 6)), 1e-8);
    }

    private void ensureVariables_monthly(NetcdfFile netcdfFile) throws IOException {
        Variable variable = netcdfFile.findVariable("s3a_night_fire");
        assertEquals(DataType.UINT, variable.getDataType());
        assertEquals(-1, variable.findAttribute(CF.FILL_VALUE).getNumericValue());
        int[] shape = variable.getShape();
        assertEquals(3, shape.length);
        assertEquals(1, shape[0]);
        assertEquals(4, shape[1]);
        assertEquals(8, shape[2]);
        Array data = variable.read();
        Index index = data.getIndex();
        assertEquals(18, data.getInt(index.set(0, 1, 2)));
        assertEquals(27, data.getInt(index.set(0, 2, 3)));

        variable = netcdfFile.findVariable("s3a_night_frp");
        assertEquals(DataType.FLOAT, variable.getDataType());
        assertEquals("MW", variable.findAttribute(CF.UNITS).getStringValue());
        data = variable.read();
        index = data.getIndex();
        assertEquals(37.f, data.getFloat(index.set(0, 3, 4)), 1e-8);
        assertEquals(14.f, data.getFloat(index.set(0, 0, 5)), 1e-8);

        variable = netcdfFile.findVariable("s3a_night_pixel");
        assertEquals(DataType.UINT, variable.getDataType());
        assertEquals("1", variable.findAttribute(CF.UNITS).getStringValue());
        data = variable.read();
        index = data.getIndex();
        assertEquals(19, data.getInt(index.set(0, 1, 6)));
        assertEquals(28, data.getInt(index.set(0, 2, 7)));

        variable = netcdfFile.findVariable("s3a_night_water");
        assertEquals(DataType.UINT, variable.getDataType());
        assertEquals("1", variable.findAttribute(CF.UNITS).getStringValue());
        data = variable.read();
        index = data.getIndex();
        assertEquals(21, data.getInt(index.set(0, 1, 6)));
        assertEquals(30, data.getInt(index.set(0, 2, 7)));
    }

    private void ensureWindowedVariables(NetcdfFile netcdfFile) throws IOException {
        Variable variable = netcdfFile.findVariable("s3a_night_fire_weighted");
        assertEquals(DataType.FLOAT, variable.getDataType());
        assertEquals(Float.NaN, variable.findAttribute(CF.FILL_VALUE).getNumericValue());
        Array data = variable.read();
        Index index = data.getIndex();
        assertEquals(32.f, data.getFloat(index.set(0, 3, 0)), 1e-8);
        assertEquals(9.f, data.getFloat(index.set(0, 0, 1)), 1e-8);

        variable = netcdfFile.findVariable("s3a_night_cloud_fraction");
        assertEquals(DataType.FLOAT, variable.getDataType());
        assertEquals(Float.NaN, variable.findAttribute(CF.FILL_VALUE).getNumericValue());
        data = variable.read();
        index = data.getIndex();
        assertEquals(0.f, data.getFloat(index.set(0, 3, 0)), 1e-8);
        assertEquals(0.f, data.getFloat(index.set(0, 0, 1)), 1e-8);

        variable = netcdfFile.findVariable("s3b_night_fire_weighted");
        assertEquals(DataType.FLOAT, variable.getDataType());
        assertEquals(Float.NaN, variable.findAttribute(CF.FILL_VALUE).getNumericValue());
        data = variable.read();
        index = data.getIndex();
        assertEquals(42.f, data.getFloat(index.set(0, 3, 0)), 1e-8);
        assertEquals(35.f, data.getFloat(index.set(0, 2, 1)), 1e-8);
    }

    private void ensureTempVariablesDropped(NetcdfFile netcdfFile) {
        Variable variable = netcdfFile.findVariable("s3a_cloud_land_pixel");
        assertNull(variable);

        variable = netcdfFile.findVariable("s3a_slstr_pixel");
        assertNull(variable);

        variable = netcdfFile.findVariable("s3a_water_pixel");
        assertNull(variable);
    }

    private Product createTestProduct_daily_cycle() throws ParseException {
        final Product product = new Product("frp-test", "test-type", 8, 4);

        final Band s3a_night_pixel = product.addBand("s3a_night_pixel_sum", ProductData.TYPE_FLOAT32);
        s3a_night_pixel.setData(createDataBuffer(5));

        final Band s3a_night_cloud = product.addBand("s3a_night_cloud_sum", ProductData.TYPE_FLOAT32);
        s3a_night_cloud.setData(createDataBuffer(6));

        final Band s3a_night_water = product.addBand("s3a_night_water_sum", ProductData.TYPE_FLOAT32);
        s3a_night_water.setData(createDataBuffer(7));

        final Band s3a_night_fire = product.addBand("s3a_night_fire_sum", ProductData.TYPE_FLOAT32);
        s3a_night_fire.setData(createDataBuffer(8));

        final Band s3a_night_frp = product.addBand("s3a_night_frp_mean", ProductData.TYPE_FLOAT32);
        s3a_night_frp.setData(createDataBuffer(9));

        final Band s3a_night_frp_unc = product.addBand("s3a_night_frp_unc_sum", ProductData.TYPE_FLOAT32);
        s3a_night_frp_unc.setData(createDataBuffer(21));

        final Band s3b_night_pixel = product.addBand("s3b_night_pixel_sum", ProductData.TYPE_FLOAT32);
        s3b_night_pixel.setData(createDataBuffer(15));

        final Band s3b_night_cloud = product.addBand("s3b_night_cloud_sum", ProductData.TYPE_FLOAT32);
        s3b_night_cloud.setData(createDataBuffer(16));

        final Band s3b_night_water = product.addBand("s3b_night_water_sum", ProductData.TYPE_FLOAT32);
        s3b_night_water.setData(createDataBuffer(17));

        final Band s3b_night_fire = product.addBand("s3b_night_fire_sum", ProductData.TYPE_FLOAT32);
        s3b_night_fire.setData(createDataBuffer(18));

        final Band s3b_night_frp = product.addBand("s3b_night_frp_mean", ProductData.TYPE_FLOAT32);
        s3b_night_frp.setData(createDataBuffer(19));

        final Band s3b_night_frp_unc = product.addBand("s3b_night_frp_unc_sum", ProductData.TYPE_FLOAT32);
        s3b_night_frp_unc.setData(createDataBuffer(23));

        product.setStartTime(ProductData.UTC.parse("22-MAR-2020 00:00:00"));
        product.setEndTime(ProductData.UTC.parse("22-MAR-2020 23:59:59"));
        return product;
    }

    private Product createTestProduct_monthly() throws ParseException {
        final Product product = new Product("frp-monthly", "test-type", 8, 4);

        final Band s3a_night_pixel = product.addBand("s3a_night_pixel_sum", ProductData.TYPE_FLOAT32);
        s3a_night_pixel.setData(createDataBuffer(5));

        final Band s3a_night_cloud = product.addBand("s3a_night_cloud_sum", ProductData.TYPE_FLOAT32);
        s3a_night_cloud.setData(createDataBuffer(6));

        final Band s3a_night_water = product.addBand("s3a_night_water_sum", ProductData.TYPE_FLOAT32);
        s3a_night_water.setData(createDataBuffer(7));

        final Band s3a_night_fire = product.addBand("s3a_night_fire_sum", ProductData.TYPE_FLOAT32);
        s3a_night_fire.setData(createDataBuffer(8));

        final Band s3a_night_frp = product.addBand("s3a_night_frp_mean", ProductData.TYPE_FLOAT32);
        s3a_night_frp.setData(createDataBuffer(9));

        final Band s3a_night_frp_unc = product.addBand("s3a_night_frp_unc_sum", ProductData.TYPE_FLOAT32);
        s3a_night_frp_unc.setData(createDataBuffer(21));

        final Band s3b_night_pixel = product.addBand("s3b_night_pixel_sum", ProductData.TYPE_FLOAT32);
        s3b_night_pixel.setData(createDataBuffer(15));

        final Band s3b_night_cloud = product.addBand("s3b_night_cloud_sum", ProductData.TYPE_FLOAT32);
        s3b_night_cloud.setData(createDataBuffer(16));

        final Band s3b_night_water = product.addBand("s3b_night_water_sum", ProductData.TYPE_FLOAT32);
        s3b_night_water.setData(createDataBuffer(17));

        final Band s3b_night_fire = product.addBand("s3b_night_fire_sum", ProductData.TYPE_FLOAT32);
        s3b_night_fire.setData(createDataBuffer(18));

        final Band s3b_night_frp = product.addBand("s3b_night_frp_mean", ProductData.TYPE_FLOAT32);
        s3b_night_frp.setData(createDataBuffer(19));

        final Band s3b_night_frp_unc = product.addBand("s3b_night_frp_unc_sum", ProductData.TYPE_FLOAT32);
        s3b_night_frp_unc.setData(createDataBuffer(23));

        product.setStartTime(ProductData.UTC.parse("01-APR-2020 00:00:00"));
        product.setEndTime(ProductData.UTC.parse("30-APR-2020 23:59:59"));

        return product;
    }

    private ProductData createDataBuffer(int offset) {
        final float[] data = new float[32];
        for (int i = 0; i < data.length; i++) {
            data[i] = i + offset;
        }
        return ProductData.createInstance(ProductData.TYPE_FLOAT32, data);
    }

    private void ensureAxesAndBoundsVariables_daily(NetcdfFile netcdfFile) throws IOException {
        ensureAxesAndBoundsVariables(netcdfFile, 18343.0, 18343.0);
    }

    private void ensureAxesAndBoundsVariables_monthly(NetcdfFile netcdfFile) throws IOException {
        ensureAxesAndBoundsVariables(netcdfFile, 18353.0, 18382.0);
    }

    private void ensureAxesAndBoundsVariables(NetcdfFile netcdfFile, double timeDimensionValue, double timeDimUpper) throws IOException {
        final Variable lon = netcdfFile.findVariable("lon");
        assertEquals(8, lon.getShape(0));

        List<Attribute> attributes = lon.getAttributes();
        assertEquals(4, attributes.size());
        Attribute attribute = lon.findAttribute("units");
        assertEquals("units", attribute.getShortName());
        assertEquals("degrees_east", attribute.getStringValue());

        final Array lonData = lon.read();
        assertEquals(-157.5, lonData.getFloat(0), 1e-8);
        assertEquals(-112.5, lonData.getFloat(1), 1e-8);

        final Variable lon_bounds = netcdfFile.findVariable("lon_bounds");
        assertEquals(8, lon_bounds.getShape(0));
        assertEquals(2, lon_bounds.getShape(1));

        attributes = lon_bounds.getAttributes();
        assertEquals(0, attributes.size());

        final Array lonBoundsArray = lon_bounds.read();
        Index index = lonBoundsArray.getIndex();
        index.set(2, 0);
        assertEquals(-90.0, lonBoundsArray.getFloat(index), 1e-8);
        index.set(2, 1);
        assertEquals(-45.0, lonBoundsArray.getFloat(index), 1e-8);

        final Variable lat = netcdfFile.findVariable("lat");
        assertEquals(4, lat.getShape(0));

        attributes = lat.getAttributes();
        assertEquals(4, attributes.size());
        attribute = lat.findAttribute("standard_name");
        assertEquals("standard_name", attribute.getShortName());
        assertEquals("latitude", attribute.getStringValue());

        final Array latData = lat.read();
        assertEquals(-22.5, latData.getFloat(2), 1e-8);
        assertEquals(-67.5, latData.getFloat(3), 1e-8);

        final Variable lat_bounds = netcdfFile.findVariable("lat_bounds");
        assertEquals(4, lat_bounds.getShape(0));
        assertEquals(2, lat_bounds.getShape(1));

        final Array latBoundsArray = lat_bounds.read();
        index = latBoundsArray.getIndex();
        index.set(1, 0);
        assertEquals(45.0, latBoundsArray.getFloat(index), 1e-8);
        index.set(1, 1);
        assertEquals(0.0, latBoundsArray.getFloat(index), 1e-8);

        attributes = lat_bounds.getAttributes();
        assertEquals(0, attributes.size());

        final Variable time = netcdfFile.findVariable("time");
        assertEquals(1, time.getShape(0));

        attributes = time.getAttributes();
        assertEquals(5, attributes.size());
        attribute = time.findAttribute("long_name");
        assertEquals("long_name", attribute.getShortName());
        assertEquals("time", attribute.getStringValue());
        final Array timeData = time.read();
        assertEquals(timeDimensionValue, timeData.getDouble(0), 1e-8);

        final Variable time_bounds = netcdfFile.findVariable("time_bounds");
        assertEquals(1, time_bounds.getShape(0));
        assertEquals(2, time_bounds.getShape(1));

        attributes = time_bounds.getAttributes();
        assertEquals(0, attributes.size());
        final Array timeBoundsData = time_bounds.read();
        assertEquals(timeDimensionValue, timeBoundsData.getDouble(0), 1e-8);
        assertEquals(timeDimUpper, timeBoundsData.getDouble(1), 1e-8);
    }

    private void ensureGlobalAttributes_daily(NetcdfFile netcdfFile) {
        final List<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();
        assertEquals(39, globalAttributes.size());
        Attribute attribute = globalAttributes.get(0);
        assertEquals("title", attribute.getShortName());
        assertEquals("ECMWF C3S Gridded OLCI Fire Radiative Power product", attribute.getStringValue());

        attribute = globalAttributes.get(7);
        assertEquals("summary", attribute.getShortName());
        assertEquals("The Copernicus Climate Change Service issues three Level 3 Fire Radiative Power (FRP) Products, each generated from Level 2 Sentinel-3 Active Fire Detection and FRP Products issued in NTC mode, which themselves are based on Sentinel 3 SLSTR data. The global Level 3 Daily FRP Products synthesise global data from the Level 2 AF Detection and FRP Product granules at 0.1 degree spatial and at 1-day temporal resolution, and also provide some adjustments for cloud cover variation since clouds can mask actively burning fires from view. These products are primarily designed for ease of use of the key information coming from individual granule-based Level 2 Products, for example in global modelling, trend analysis and model evaluation.",
                attribute.getStringValue());

        attribute = globalAttributes.get(12);
        assertEquals("cdm_data_type", attribute.getShortName());
        assertEquals("Grid", attribute.getStringValue());

        attribute = globalAttributes.get(19);
        assertEquals("project", attribute.getShortName());
        assertEquals("EC C3S Fire Radiative Power", attribute.getStringValue());

        attribute = globalAttributes.get(24);
        assertEquals("geospatial_vertical_min", attribute.getShortName());
        assertEquals("0", attribute.getStringValue());

        attribute = globalAttributes.get(31);
        assertEquals("license", attribute.getShortName());
        assertEquals("EC C3S FRP Data Policy", attribute.getStringValue());

        attribute = globalAttributes.get(35);
        assertEquals("geospatial_lon_units", attribute.getShortName());
        assertEquals("degrees_east", attribute.getStringValue());
    }

    private void ensureGlobalAttributes_monthly(NetcdfFile netcdfFile) {
        final List<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();
        assertEquals(39, globalAttributes.size());
        Attribute attribute = globalAttributes.get(1);
        assertEquals("institution", attribute.getShortName());
        assertEquals("King's College London, Brockmann Consult GmbH", attribute.getStringValue());

        attribute = globalAttributes.get(13);
        assertEquals("comment", attribute.getShortName());
        assertEquals("These data were produced as part of the Copernicus Climate Change Service programme.", attribute.getStringValue());

        attribute = globalAttributes.get(25);
        assertEquals("geospatial_vertical_max", attribute.getShortName());
        assertEquals("0", attribute.getStringValue());

        attribute = globalAttributes.get(32);
        assertEquals("platform", attribute.getShortName());
        assertEquals("Sentinel-3", attribute.getStringValue());

        attribute = globalAttributes.get(36);
        assertEquals("geospatial_lat_units", attribute.getShortName());
        assertEquals("degrees_north", attribute.getStringValue());
    }

    private void ensureDimensions(NetcdfFile netcdfFile) {
        ensureDimension(netcdfFile, "time", 1);
        ensureDimension(netcdfFile, "lon", 8);
        ensureDimension(netcdfFile, "lat", 4);
        ensureDimension(netcdfFile, "bounds", 2);
    }

    private void ensureDimension(NetcdfFile netcdfFile, String dimName, int expectedLength) {
        final Dimension dimension = netcdfFile.findDimension(dimName);
        assertNotNull(dimName);
        assertEquals(expectedLength, dimension.getLength());
    }
}
