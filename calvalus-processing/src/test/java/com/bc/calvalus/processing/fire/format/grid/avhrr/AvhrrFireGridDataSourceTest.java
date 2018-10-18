package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.snap.collocation.CollocateOp;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Ignore;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;

public class AvhrrFireGridDataSourceTest {

    @Test
    public void testGetPixelIndex() throws Exception {

        // first target grid cell, first index

        assertEquals(0, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 0, 0));
        assertEquals(1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 1, 0, 0));
        assertEquals(4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 0, 0));
        assertEquals(7200, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 1, 0));
        assertEquals(7204, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 1, 0));
        assertEquals(28804, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 4, 0));

        // second target grid cell, first index

        assertEquals(5, AvhrrFireGridDataSource.getPixelIndex(1, 0, 0, 0, 0));
        assertEquals(6, AvhrrFireGridDataSource.getPixelIndex(1, 0, 1, 0, 0));
        assertEquals(9, AvhrrFireGridDataSource.getPixelIndex(1, 0, 4, 0, 0));
        assertEquals(7205, AvhrrFireGridDataSource.getPixelIndex(1, 0, 0, 1, 0));
        assertEquals(7209, AvhrrFireGridDataSource.getPixelIndex(1, 0, 4, 1, 0));
        assertEquals(28809, AvhrrFireGridDataSource.getPixelIndex(1, 0, 4, 4, 0));

        // rightmost target grid cell, first index

        assertEquals(5 * 79, AvhrrFireGridDataSource.getPixelIndex(79, 0, 0, 0, 0));
        assertEquals(5 * 79 + 1, AvhrrFireGridDataSource.getPixelIndex(79, 0, 1, 0, 0));
        assertEquals(5 * 79 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 0, 4, 0, 0));
        assertEquals(1 * 7200 + 5 * 79, AvhrrFireGridDataSource.getPixelIndex(79, 0, 0, 1, 0));
        assertEquals(1 * 7200 + 5 * 79 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 0, 4, 1, 0));
        assertEquals(4 * 7200 + 5 * 79 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 0, 4, 4, 0));

        // target grid cell at x0, y1, first index

        assertEquals(5 * 7200, AvhrrFireGridDataSource.getPixelIndex(0, 1, 0, 0, 0));
        assertEquals(5 * 7200 + 1, AvhrrFireGridDataSource.getPixelIndex(0, 1, 1, 0, 0));
        assertEquals(5 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 1, 4, 0, 0));
        assertEquals(5 * 7200 + 1 * 7200, AvhrrFireGridDataSource.getPixelIndex(0, 1, 0, 1, 0));
        assertEquals(5 * 7200 + 1 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 1, 4, 1, 0));
        assertEquals(5 * 7200 + 4 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 1, 4, 4, 0));


        // first target grid cell, second index

        assertEquals(400, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 0, 1));
        assertEquals(401, AvhrrFireGridDataSource.getPixelIndex(0, 0, 1, 0, 1));
        assertEquals(404, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 0, 1));
        assertEquals(7600, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 1, 1));
        assertEquals(7604, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 1, 1));
        assertEquals(29204, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 4, 1));

        // last target grid cell, second index

        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200, AvhrrFireGridDataSource.getPixelIndex(79, 79, 0, 0, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 1, AvhrrFireGridDataSource.getPixelIndex(79, 79, 1, 0, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 79, 4, 0, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 1 * 7200 + 0, AvhrrFireGridDataSource.getPixelIndex(79, 79, 0, 1, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 1 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 79, 4, 1, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 4 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 79, 4, 4, 1));


        // first target grid cell, upper rightmost index

        assertEquals(17 * 400 + 0, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 0, 17));
        assertEquals(17 * 400 + 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 1, 0, 17));
        assertEquals(17 * 400 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 0, 17));
        assertEquals(17 * 400 + 0 + 7200 * 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 1, 17));
        assertEquals(17 * 400 + 4 + 7200 * 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 1, 17));
        assertEquals(17 * 400 + 4 + 7200 * 4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 4, 17));

        // first target grid cell, second from above, left most index

        assertEquals(400 * 18 * 400 + 0, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 0, 18));
        assertEquals(7200 * 400 + 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 1, 0, 18));
        assertEquals(7200 * 400 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 0, 18));
        assertEquals(7200 * 400 + 0 + 7200 * 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 1, 18));
        assertEquals(7200 * 400 + 4 + 7200 * 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 1, 18));
        assertEquals(7200 * 400 + 4 + 7200 * 4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 4, 18));

        // last target grid cell, right-bottom index

        assertEquals(7200 * 3600 - 1, AvhrrFireGridDataSource.getPixelIndex(79, 79, 4, 4, 161));
    }

    @Test
    @Ignore
    public void acceptanceTestGetData() throws Exception {
        Product porcProduct = ProductIO.readProduct("C:\\ssd\\avhrr\\BA_2000_2_Porcentage.tif");
        Product uncProduct = ProductIO.readProduct("C:\\ssd\\avhrr\\BA_2000_2_Uncertainty.tif");
        Product lcProduct = ProductIO.readProduct("C:\\ssd\\avhrr\\lc-avhrr.nc");
        AvhrrFireGridDataSource source = new AvhrrFireGridDataSource(porcProduct, lcProduct, uncProduct, 81);
        SourceData data = source.readPixels(15, 3);

        float baValue = 0.0F;
        double coverageValue = 0.0F;
        double burnableFractionValue = 0.0;

        int numberOfBurnedPixels = 0;

        for (int i = 0; i < data.burnedPixels.length; i++) {
            float burnedPixel = data.burnedPixels[i];
            if (burnedPixel > 0.0 && LcRemapping.isInBurnableLcClass(data.lcClasses[i])) {
                numberOfBurnedPixels++;
                double burnedArea = burnedPixel * data.areas[i];
                baValue += burnedArea;
//                addBaInLandCover(baInLc, targetGridCellIndex, burnedArea, data.lcClasses[i]);
            }

            burnableFractionValue += LcRemapping.isInBurnableLcClass(data.lcClasses[i]) ? data.areas[i] : 0.0;
            coverageValue += (data.statusPixels[i] == 1 && LcRemapping.isInBurnableLcClass(data.lcClasses[i])) ? data.areas[i] : 0.0;
        }

        System.out.println(baValue);

    }

    @Test
    @Ignore
    public void writeTestProduct() throws IOException, FactoryException, TransformException {
        Product product = ProductIO.readProduct("C:\\ssd\\avhrr\\BA_2000_2_Porcentage.tif");
        ReprojectionOp reprojectionOp = new ReprojectionOp();
        reprojectionOp.setSourceProduct(product);
        reprojectionOp.setParameterDefaultValues();
        reprojectionOp.setParameter("crs", "EPSG:4326");

        Product product1 = new Product("name", "type", 7200, 3600);
        product1.addBand("name", "1");
        product1.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 7200, 3600, -180.0, 90.0, 360.0 / 7200.0, 180.0 / 3600.0));
        CollocateOp collocateOp = new CollocateOp();
        collocateOp.setMasterProduct(product1);
        collocateOp.setSlaveProduct(product);
        collocateOp.setParameterDefaultValues();

        Product reprojectedProduct = collocateOp.getTargetProduct();

        ProductIO.writeProduct(reprojectedProduct, "c:\\ssd\\avhrr\\porc-repro2.nc", "NetCDF4-CF");
    }


    @Test
    public void name() throws IOException, ParseException {
        Product product = ProductIO.readProduct("C:\\ssd\\avhrr\\BA_2000_2_Porcentage_WGS.tif");
        Product product2 = ProductIO.readProduct("C:\\ssd\\avhrr\\BA_2000_2_Uncertainty_WGS.tif");
        SubsetOp subsetOp = new SubsetOp();
        subsetOp.setGeoRegion(new WKTReader().read("POLYGON ((-64.25 -40.75, -64. -40.75, -64. -41, -64.25 -41, -64.25 -40.75))"));
        subsetOp.setSourceProduct(product);
        Product subset1 = subsetOp.getTargetProduct();

        SubsetOp subsetOp2 = new SubsetOp();
        subsetOp2.setGeoRegion(new WKTReader().read("POLYGON ((-64.25 -40.75, -64. -40.75, -64. -41, -64.25 -41, -64.25 -40.75))"));
        subsetOp2.setSourceProduct(product2);
        Product subset2 = subsetOp2.getTargetProduct();

        File lcFile = new File("C:\\ssd\\avhrr\\ESACCI-LC-L4-LCCS-Map-300m-P1Y-1999-v2.0.7.tif");
        Product lcProduct = ProductIO.readProduct(lcFile);
        CollocationOp collocateOp = new CollocationOp();
        collocateOp.setMasterProduct(subset1);
        collocateOp.setSlaveProduct(lcProduct);
        collocateOp.setParameterDefaultValues();

        Product reprojectedProduct = collocateOp.getTargetProduct();
        reprojectedProduct.getBand("band_1_S").setName("band_1");

        int sourcePixelIndex = 0;
        int width = subset1.getSceneRasterWidth();
        int height = subset1.getSceneRasterHeight();

        float[] porc = new float[width * height];
        subset1.getBand("band_1").readPixels(0, 0, width, height, porc);
        float[] confidence = new float[width * height];
        subset2.getBand("band_1").readPixels(0, 0, width, height, confidence);
        int[] lc = new int[width * height];
        reprojectedProduct.getBand("band_1").readPixels(0, 0, width, height, lc);

        AreaCalculator areaCalculator = new AreaCalculator(subset1.getSceneGeoCoding());
        SourceData data = new SourceData(width, height);
        for (int sourceY = 0; sourceY < data.height; sourceY++) {
            for (int sourceX = 0; sourceX < data.width; sourceX++) {
                float sourcePC = porc[sourcePixelIndex];
                if (isValidPixel(sourcePC)) {
                    float sourceCL = confidence[sourcePixelIndex] / 100.0F;
                    data.burnedPixels[sourcePixelIndex] = sourcePC;
                    data.probabilityOfBurn[sourcePixelIndex] = sourceCL;
                }
                int sourceLC = lc[sourcePixelIndex];
                data.lcClasses[sourcePixelIndex] = sourceLC;
                data.burnable[sourcePixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);

                if (sourcePC >= 0) { // has data -> observed pixel
                    data.statusPixels[sourcePixelIndex] = 1;
                }

                int x1 = sourcePixelIndex % width;
                int y1 = sourcePixelIndex / width;
                data.areas[sourcePixelIndex] = areaCalculator.calculatePixelSize(x1, y1, width, height);
                sourcePixelIndex++;
            }
        }

        Product sourceData = data.makeProduct();
        System.out.println(Arrays.stream(data.areas).sum());
        System.out.println(Arrays.toString(data.probabilityOfBurn));

//        ProductIO.writeProduct(sourceData, "C:\\ssd\\avhrr.nc", "NetCDF4-CF");

    }


    private boolean isValidPixel(float sourcePC) {
        return sourcePC != -1.0 && sourcePC != -2.0;
    }
}