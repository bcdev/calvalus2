package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ModisGridMapperTest {

    @Ignore
    @Test
    public void acceptanceTestComputeGridCell() throws Exception {
        ModisGridMapper mapper = new ModisGridMapper();
        Product product = ProductIO.readProduct("D:\\workspace\\fire-cci\\temp\\BA_PIX_MER_v04h24_200806_v4.0.tif");
        Product lcProduct = ProductIO.readProduct("D:\\workspace\\fire-cci\\temp\\lc-2005-v04h24.nc");
        File[] srFiles = new File("D:\\workspace\\fire-cci\\temp").listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("CCI-Fire-MERIS-SDR-");
            }
        });
        List<File> srProducts = new ArrayList<>();
        Collections.addAll(srProducts, srFiles);

//        mapper.setDataSource(new ModisFireGridDataSource("", product, lcProduct, srProducts));
        GridCells gridCells = mapper.computeGridCells(2008, 1);
        System.out.println(gridCells);
    }

    @Test
    @Ignore
    public void acceptanceTestStandardError() throws Exception {
        Product product = ProductIO.readProduct("c:\\ssd\\modis-analysis\\burned_2006_1_h19v09.nc");
        Band uncertainty = product.getBand("uncertainty");
        uncertainty.readRasterDataFully(ProgressMonitor.NULL);
        short[] p0 = (short[]) uncertainty.getData().getElems();
        double[] p = new double[p0.length];
        for (int i = 0; i < p.length; i++) {
            p[i] = p0[i];
        }

        float errorPerPixel = new ModisGridMapper().getErrorPerPixel(p, 0, 2085, 0);
        assertEquals(10.0, errorPerPixel, 6.7479032E7);
    }

}