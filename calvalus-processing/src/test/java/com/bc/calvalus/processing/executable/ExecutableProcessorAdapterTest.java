package com.bc.calvalus.processing.executable;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.beam.PathConfiguration;
import com.bc.calvalus.processing.beam.Sentinel2CalvalusReaderPlugin;
import com.bc.ceres.core.NullProgressMonitor;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class ExecutableProcessorAdapterTest {

    public void testS2Reader() throws IOException {
        Product product = CalvalusProductIO.readProduct(
                new PathConfiguration(new Path("/home/boe/tmp/S2A_MSIL2A_20170622T104021_N0205_R008_T32UPF_20170622T104021.zip"), new Configuration()),
                                                Sentinel2CalvalusReaderPlugin.FORMAT_20M);
        System.out.println(product.getProductReader().toString());
        File productFileLocation = product.getFileLocation();
        Map<String, Object> params = new HashMap<>();
        params.put("referenceBand", "B5");
        product = GPF.createProduct("Resample", params, product);
        product.setFileLocation(productFileLocation);
        ProductIO.writeProduct(product, new File("/home/boe/tmp/output.nc"), "NetCDF4-BEAM", false, new NullProgressMonitor());
    }

    public static void main(String[] args) throws IOException {
        new ExecutableProcessorAdapterTest().testS2Reader();
    }
}
