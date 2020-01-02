package com.bc.calvalus.processing.fire.format.grid.olci;

import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.Product;

import java.awt.*;
import java.io.IOException;

public class OlciDataSource extends AbstractFireGridDataSource {

    private final Product classificationProduct;
    private final Product foaProduct;
    private final Product uncertaintyProduct;
    private final Product lcProduct;
    private final int sourceWidth;
    private final int sourceHeight;
    private GeoCoding geoCoding;

    OlciDataSource(Product classificationProduct, Product foaProduct, Product uncertaintyProduct, Product lcProduct) {
        super(-1, -1);
        this.classificationProduct = classificationProduct;
        this.uncertaintyProduct = uncertaintyProduct;
        this.foaProduct = foaProduct;
        this.lcProduct = lcProduct;
        this.sourceWidth = 90;
        this.sourceHeight = 90;
        this.geoCoding = classificationProduct.getSceneGeoCoding();
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        SourceData data = new SourceData(sourceWidth, sourceHeight);
        Rectangle sourceRect = new Rectangle(x * sourceWidth, y * sourceHeight, sourceWidth, sourceHeight);

        Band baBand = classificationProduct.getBand("band_1");
        baBand.readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, data.burnedPixels);

        Band lcClassification = lcProduct.getBand("lccs_class");
        lcClassification.readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, data.lcClasses);

        for (int i = 0; i < data.lcClasses.length; i++) {
            data.burnable[i] = LcRemapping.isInBurnableLcClass(data.lcClasses[i]);
        }

        Band pbBand = uncertaintyProduct.getBand("band_1");
        pbBand.readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, data.probabilityOfBurn);

        Band statusBand = foaProduct.getBand("band_1");
        statusBand.readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, data.statusPixels);
        remodelStatusPixels(data.statusPixels);

        setAreas(geoCoding, sourceRect, data.areas);

        return data;
    }


    private static void remodelStatusPixels(int[] statusPixels) {
        for (int i = 0; i < statusPixels.length; i++) {
            if (statusPixels[i] == 0 || statusPixels[i] == 4) { // 0=observed, 3=not observed, 4=unburnable
                statusPixels[i] = 1;
            } else {
                statusPixels[i] = 0;
            }
        }
    }

}
