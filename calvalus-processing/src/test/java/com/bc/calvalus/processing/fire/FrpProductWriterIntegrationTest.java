package com.bc.calvalus.processing.fire;

import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Product;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
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
    public void testWriteProductNodes() throws IOException {
        final Product product = new Product("frp-test", "test-type", 8, 4);

        final ProductWriter productWriter = new FrpL3ProductWriterPlugIn().createWriterInstance();

        try {
            productWriter.writeProductNodes(product, targetFile);
        } finally {
            product.dispose();
            productWriter.flush();
            productWriter.close();
        }

        try (NetcdfFile netcdfFile = NetcdfFile.open(targetFile.getAbsolutePath())) {
            ensureDimensions(netcdfFile);
            ensureGlobalAttributes(netcdfFile);

            ensureAxesAndBoundsVariables(netcdfFile);
        }
    }

    private void ensureAxesAndBoundsVariables(NetcdfFile netcdfFile) {
        final Variable lon = netcdfFile.findVariable("lon");
        assertEquals(8, lon.getShape(0));

        List<Attribute> attributes = lon.getAttributes();
        assertEquals(4, attributes.size());
        Attribute attribute = attributes.get(0);
        assertEquals("units", attribute.getShortName());
        assertEquals("degrees_east", attribute.getStringValue());

        final Variable lon_bounds = netcdfFile.findVariable("lon_bounds");
        assertEquals(8, lon_bounds.getShape(0));
        assertEquals(2, lon_bounds.getShape(1));

        attributes = lon_bounds.getAttributes();
        assertEquals(0, attributes.size());

        final Variable lat = netcdfFile.findVariable("lat");
        assertEquals(4, lat.getShape(0));

        attributes = lat.getAttributes();
        assertEquals(4, attributes.size());
        attribute = attributes.get(1);
        assertEquals("standard_name", attribute.getShortName());
        assertEquals("latitude", attribute.getStringValue());

        final Variable lat_bounds = netcdfFile.findVariable("lat_bounds");
        assertEquals(4, lat_bounds.getShape(0));
        assertEquals(2, lat_bounds.getShape(1));

        attributes = lat_bounds.getAttributes();
        assertEquals(0, attributes.size());

        final Variable time = netcdfFile.findVariable("time");
        assertEquals(1, time.getShape(0));

        attributes = time.getAttributes();
        assertEquals(5, attributes.size());
        attribute = attributes.get(2);
        assertEquals("long_name", attribute.getShortName());
        assertEquals("time", attribute.getStringValue());

        final Variable time_bounds = netcdfFile.findVariable("time_bounds");
        assertEquals(1, time_bounds.getShape(0));
        assertEquals(2, time_bounds.getShape(1));

        attributes = time_bounds.getAttributes();
        assertEquals(0, attributes.size());
    }

    private void ensureGlobalAttributes(NetcdfFile netcdfFile) {
        final List<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();
        assertEquals(31, globalAttributes.size());
        Attribute attribute = globalAttributes.get(0);
        assertEquals("title", attribute.getShortName());
        assertEquals("ECMWF C3S Gridded OLCI Fire Radiative Power product", attribute.getStringValue());

        attribute = globalAttributes.get(12);
        assertEquals("cdm_data_type", attribute.getShortName());
        assertEquals("Grid", attribute.getStringValue());

        attribute = globalAttributes.get(24);
        assertEquals("geospatial_vertical_min", attribute.getShortName());
        assertEquals("0", attribute.getStringValue());

        attribute = globalAttributes.get(30);
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
