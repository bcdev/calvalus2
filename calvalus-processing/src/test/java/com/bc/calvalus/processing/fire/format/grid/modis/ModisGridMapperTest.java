package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.grid.GridCell;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.io.CsvReader;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        GridCell gridCell = mapper.computeGridCell(2008, 1);
        System.out.println(gridCell);
    }

    @Test
    public void testStandardError() throws Exception {
        Product product = ProductIO.readProduct("c:\\ssd\\modis-analysis\\burned_2006_1_h19v09.nc");


        double[] p = new double[14255];
        try (CsvReader r = new CsvReader(new FileReader(new File("c:\\Users\\Thomas\\Desktop\\Mappe3.csv")), new char[]{';'})) {
            List<double[]> doubles = r.readDoubleRecords();
            for (int i = 0; i < doubles.size(); i++) {
                double[] aDouble = doubles.get(i);
                p[i] = aDouble[0] / 100;
            }
        }

        float errorPerPixel = new ModisGridMapper().getErrorPerPixel(p, 746);

    }
}