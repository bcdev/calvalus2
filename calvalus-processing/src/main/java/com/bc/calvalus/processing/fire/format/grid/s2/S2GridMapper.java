package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.ErrorPredictor;
import com.bc.calvalus.processing.fire.format.grid.GridCell;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;

import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * Runs the fire S2 formatting grid mapper.
 *
 * @author thomas
 */
public class S2GridMapper extends AbstractGridMapper {

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));
        String tile = context.getConfiguration().get("calvalus.tile");

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        LOG.info("paths=" + Arrays.toString(paths));

        boolean computeBA = !paths[0].getName().equals("dummy");
        LOG.info(computeBA ? "Computing BA" : "Only computing coverage");

        Product[] sourceProducts = new Product[10]; // todo - continue here
        Product lcProduct;
        if (computeBA) {
            File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[0], context.getConfiguration());
            sourceProducts[0] = ProductIO.readProduct(sourceProductFile);

            File lcTile = CalvalusProductIO.copyFileToLocal(paths[1], context.getConfiguration());
            lcProduct = ProductIO.readProduct(lcTile);
        } else {
            // because coverage is computed in reducer
            return;
        }

        setDataSource(new S2FireGridDataSource(tile, sourceProducts, lcProduct));
        ErrorPredictor errorPredictor = new ErrorPredictor();
        GridCell gridCell = computeGridCell(year, month, errorPredictor);

        context.progress();

        context.write(new Text(String.format("%d-%02d-%s", year, month, tile)), gridCell);
        errorPredictor.dispose();
    }

    @Override
    protected boolean maskUnmappablePixels() {
        return false;
    }

    private static class S2FireGridDataSource extends AbstractFireGridDataSource {

        private final String tile;
        private final Product[] sourceProducts;
        private final Product lcProduct;

        public S2FireGridDataSource(String tile, Product sourceProducts[], Product lcProduct) {
            this.tile = tile;
            this.sourceProducts = sourceProducts;
            this.lcProduct = lcProduct;
        }

        @Override
        public void readPixels(SourceData data, int rasterWidth, int x, int y) throws IOException {
            for (Product sourceProduct : sourceProducts) {
                GeoCoding gc = sourceProduct.getSceneGeoCoding();
                Rectangle sourceRect = getRectangle(gc, x, y);
                if (sourceRect == null) {
                    continue;
                }
                Band baBand = sourceProduct.getBand("band_1");
                int[] buffer = new int[data.pixels.length];
                baBand.readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, buffer);
                double[] areaBuffer = new double[data.areas.length];
                getAreas(gc, rasterWidth, areaBuffer);

                merge(buffer, data.pixels);
                merge(areaBuffer, data.areas);
            }
            Band lcClassification = lcProduct.getBand("lcclass");
            lcClassification.readPixels(x * 90, y * 90, 90, 90, data.lcClasses);
            data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels), true);
            data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels), false);
        }

        private void merge(double[] areaBuffer, double[] areas) {
            // todo - create test and implement; goal: valid pixels shall be preserved
        }

        private void merge(int[] buffer, int[] pixels) {
            // todo - create test and implement; goal: valid pixels shall be preserved
        }

        private Rectangle getRectangle(GeoCoding gc, int x, int y) {

            // todo - use maximum pixel value if necessary

            int tileX = Integer.parseInt(tile.substring(1, 2));
            int tileY = Integer.parseInt(tile.substring(4, 5));
            GeoPos UL = new GeoPos(tileX * 10 + x / 40, tileY * 10 - 90 + y / 40);
            GeoPos LR = new GeoPos(tileX * 10 + (x + 1) / 40, tileY * 10 - 90 + (y + 1) / 40);
            PixelPos ULpp = gc.getPixelPos(UL, null);
            PixelPos LRpp = gc.getPixelPos(LR, null);
            if (!ULpp.isValid() || !LRpp.isValid()) {
                return null;
            }
            int rectX = (int) ULpp.x;
            int rectY = (int) ULpp.y;
            int rectWidth = (int) (LRpp.x - ULpp.x) - 1;
            int rectHeight = (int) (LRpp.y - ULpp.y) - 1;
            return new Rectangle(rectX, rectY, rectWidth, rectHeight);
        }
    }
}