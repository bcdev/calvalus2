package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_AREA;
import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_DATA;

/**
 * @author thomas
 */
public class SourceData {

    public int width;
    public int height;
    public float[] burnedPixels;
    public double[] areas;
    public int[] lcClasses;
    public boolean[] burnable;
    public int patchCount;
    public int[] statusPixels;
    public double[] probabilityOfBurn;

    public SourceData(int width, int height) {
        this.width = width;
        this.height = height;
        burnedPixels = new float[width * height];
        areas = new double[width * height];
        statusPixels = new int[width * height];
        lcClasses = new int[width * height];
        burnable = new boolean[width * height];
        probabilityOfBurn = new double[width * height];
    }

    public void reset() {
        Arrays.fill(burnedPixels, NO_DATA);
        Arrays.fill(lcClasses, 0);
        Arrays.fill(areas, NO_AREA);
        Arrays.fill(statusPixels, 0);
        Arrays.fill(burnable, false);
        Arrays.fill(probabilityOfBurn, 0.0);

    }

    public Product makeProduct() {
        Product product = new Product("sourceData", "debug", width, height);
        Band burnedPixels = product.addBand("burnedPixels", ProductData.TYPE_FLOAT32);
        Band areas = product.addBand("areas", ProductData.TYPE_FLOAT64);
        Band lcClasses = product.addBand("lcClasses", ProductData.TYPE_INT32);
        Band statusPixels = product.addBand("statusPixels", ProductData.TYPE_INT32);
        Band probabilityOfBurn = product.addBand("probabilityOfBurn", ProductData.TYPE_FLOAT64);
        Band burnable = product.addBand("burnable", ProductData.TYPE_FLOAT64);

        burnedPixels.setRasterData(new ProductData.Float(this.burnedPixels));
        areas.setRasterData(new ProductData.Double(this.areas));
        lcClasses.setRasterData(new ProductData.Int((this.lcClasses)));
        statusPixels.setRasterData(new ProductData.Int(this.statusPixels));
        probabilityOfBurn.setRasterData(new ProductData.Double(this.probabilityOfBurn));

        double[] burnableDoubles = new double[width * height];
        for (int i = 0; i < this.burnable.length; i++) {
            burnableDoubles[i] = this.burnable[i] ? 1 : 0;
        }
        burnable.setRasterData(new ProductData.Double(burnableDoubles));

        return product;
    }

}
