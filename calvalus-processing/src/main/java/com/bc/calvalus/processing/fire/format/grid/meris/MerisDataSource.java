package com.bc.calvalus.processing.fire.format.grid.meris;

import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.dataio.netcdf.util.NetcdfFileOpener;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;

import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Offers the standard implementation for source reading pixels
 *
 * @author thomas
 */
public class MerisDataSource extends AbstractFireGridDataSource {

    private final Product sourceProduct;
    private final Product lcProduct;
    private final List<File> srProducts;
    private final boolean computeBA;
    private final int sourceWidth;
    private final int sourceHeight;
    private GeoCoding geoCoding;

    MerisDataSource(Product sourceProduct, Product lcProduct, List<File> srProducts) {
        this(sourceProduct, lcProduct, srProducts, 90, 90);
    }

    MerisDataSource(Product sourceProduct, Product lcProduct, List<File> srProducts, int sourceWidth, int sourceHeight) {
        this.sourceProduct = sourceProduct;
        this.lcProduct = lcProduct;
        this.srProducts = srProducts;
        this.computeBA = sourceProduct != null;
        this.sourceWidth = sourceWidth;
        this.sourceHeight = sourceHeight;

        if (computeBA) {
            geoCoding = sourceProduct.getSceneGeoCoding();
        } else {
            try {
                geoCoding = ProductIO.readProduct(srProducts.get(0)).getSceneGeoCoding();
            } catch (IOException e) {
                throw new IllegalStateException("product '" + srProducts.get(0).getName() + "' does not have a geo-coding");
            }
        }
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        SourceData data = new SourceData(sourceWidth, sourceHeight);
        Rectangle sourceRect = new Rectangle(x * sourceWidth, y * sourceHeight, sourceWidth, sourceHeight);
        if (computeBA) {
            Band baBand = sourceProduct.getBand("band_1");
            baBand.readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, data.pixels);
            data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels), true);
            data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels), false);
            Band lcClassification = lcProduct.getBand("lcclass");
            lcClassification.readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, data.lcClasses);
        }
        getAreas(geoCoding, sourceWidth, sourceHeight, data.areas);

        byte[] statusPixelsFirstHalf = new byte[sourceRect.width * sourceRect.height];
        byte[] statusPixelsSecondHalf = new byte[sourceRect.width * sourceRect.height];
        for (File srProduct : srProducts) {
            NetcdfFile netcdfFile = NetcdfFileOpener.open(srProduct);
            int startIndex = "CCI-Fire-MERIS-SDR-L3-300m-v1.0-2002-07-".length();
            int day = Integer.parseInt(srProduct.getName().substring(startIndex, startIndex + 2));
            boolean firstHalf = day <= 15;
            Array status;
            try {
                status = netcdfFile.findVariable(null, "status").read(new int[]{sourceRect.y, sourceRect.x}, new int[]{sourceRect.height, sourceRect.width});
            } catch (InvalidRangeException e) {
                throw new IOException(e);
            } finally {
                netcdfFile.close();
            }

            if (firstHalf) {
                statusPixelsFirstHalf = (byte[]) status.get1DJavaArray(byte.class);
            } else {
                statusPixelsSecondHalf = (byte[]) status.get1DJavaArray(byte.class);
            }
            collectStatusPixels(statusPixelsFirstHalf, data.statusPixelsFirstHalf);
            collectStatusPixels(statusPixelsSecondHalf, data.statusPixelsSecondHalf);
        }
        return data;
    }


    static void collectStatusPixels(byte[] statusPixels, int[] statusPixelsTarget) {
        for (int i = 0; i < statusPixels.length; i++) {
            if (statusPixels[i] == 1) {
                statusPixelsTarget[i] = 1;
            }
        }
    }

}
