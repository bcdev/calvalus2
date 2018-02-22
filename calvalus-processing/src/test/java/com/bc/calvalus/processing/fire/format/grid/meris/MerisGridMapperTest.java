package com.bc.calvalus.processing.fire.format.grid.meris;

import com.bc.calvalus.processing.fire.format.grid.GridCells;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MerisGridMapperTest {

    @Ignore
    @Test
    public void acceptanceTestComputeGridCell() throws Exception {
        MerisGridMapper mapper = new MerisGridMapper();
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

        mapper.setDataSource(new MerisDataSource(product, lcProduct, srProducts));
        GridCells gridCells = mapper.computeGridCells(2008, 1);
        System.out.println(gridCells);
    }

    @Test
    public void testGetTile() throws Exception {
        assertEquals("v04h07", MerisGridMapper.getTile("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2008/v04h07/2008/2008-06-01-fire-nc/CCI-Fire-MERIS-SDR-L3-300m-v1.0-2008-06-01-v04h07.nc"));
    }
}
