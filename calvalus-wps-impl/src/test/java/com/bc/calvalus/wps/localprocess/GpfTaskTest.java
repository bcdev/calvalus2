package com.bc.calvalus.wps.localprocess;

import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.junit.*;

import java.io.File;
import java.util.HashMap;

/**
 * @author hans
 */
public class GpfTaskTest {

    @Ignore("test takes too long")
    @Test
    public void testCreateLcCciSubset() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        final Product sourceProduct;
        sourceProduct = ProductIO.readProduct("C:\\Personal\\CabLab\\EO data\\ESACCI-LC-L4-LCCS-Map-300m-P5Y-2000-v1.3.nc");
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("targetDir", new File("."));
//        parameters.put("predefinedRegion", PredefinedRegion.GREENLAND);
        parameters.put("north", 20f);
        parameters.put("west", -10f);
        parameters.put("east", 10f);
        parameters.put("south", 0f);
        GPF.createProduct("LCCCI.Subset", parameters, sourceProduct);
    }

    @Ignore("test takes too long")
    @Test
    public void testCreateSubset() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        final Product sourceProduct;
        sourceProduct = ProductIO.readProduct(new File("C:\\Personal\\UrbanTEP\\ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.tif"), "GeoTIFF");
        HashMap<String, Object> subsetParameters = new HashMap<>();

        subsetParameters.put("geoRegion", "POLYGON((100 -10,100 0,110 0,110 -10,100 -10))");
        Product subset = GPF.createProduct("Subset", subsetParameters, sourceProduct);
        GPF.writeProduct(subset, new File("target.nc"), "Netcdf-BEAM", false, ProgressMonitor.NULL);
    }

}