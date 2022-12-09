package com.bc.calvalus.processing.fire;

import org.esa.snap.core.dataio.EncodeQualification;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.io.SnapFileFilter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class FrpL3ProductWriterPlugInTest {

    private FrpL3ProductWriterPlugIn plugIn;

    @Before
    public void setUp() {
        plugIn = new FrpL3ProductWriterPlugIn();
    }

    @Test
    public void testGetEncodeQualification() {
        final EncodeQualification encodeQualification = plugIn.getEncodeQualification(new Product("testing", "test_type", 4, 5));
        assertEquals(EncodeQualification.FULL, encodeQualification);
    }

    @Test
    public void testGetOutputTypes() {
        final Class[] outputTypes = plugIn.getOutputTypes();
        assertEquals(2, outputTypes.length);
        assertEquals(String.class, outputTypes[0]);
        assertEquals(File.class, outputTypes[1]);
    }

    @Test
    public void testCreateWriterInstance() {
        final ProductWriter writer = plugIn.createWriterInstance();
        assertNotNull(writer);
        assertTrue(writer instanceof FrpL3ProductFileWriter);
        assertSame(plugIn, writer.getWriterPlugIn());
    }

    @Test
    public void testGetFormatNames() {
        final String[] formatNames = plugIn.getFormatNames();
        assertEquals(1, formatNames.length);
        assertEquals("NetCDF4-FRP-L3", formatNames[0]);
    }

    @Test
    public void testGetDefaultFileExtensiona() {
        final String[] fileExtensions = plugIn.getDefaultFileExtensions();
        assertEquals(1, fileExtensions.length);
        assertEquals(".nc", fileExtensions[0]);
    }

    @Test
    public void testGetDescription() {
        assertEquals("C3S FRP Level 3 products", plugIn.getDescription(null));
    }

    @Test
    public void testGetProductFileFilter() {
        final SnapFileFilter productFileFilter = plugIn.getProductFileFilter();
        assertNotNull(productFileFilter);

        assertEquals("C3S FRP Level 3 products (*.nc)", productFileFilter.getDescription());
        assertEquals("NetCDF4-FRP-L3", productFileFilter.getFormatName());
        assertEquals(".nc", productFileFilter.getDefaultExtension());
    }
}
