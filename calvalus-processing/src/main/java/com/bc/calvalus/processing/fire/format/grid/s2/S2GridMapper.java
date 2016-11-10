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
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;
import org.esa.snap.core.util.ProductUtils;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.S2_GRID_PIXELSIZE;
import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.filter;

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

        Product[] sourceProducts = new Product[paths.length - 1];
        for (int i = 0; i < paths.length - 1; i++) {
            File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
            Product currentProduct = ProductIO.readProduct(sourceProductFile);
            ReprojectionOp reprojectionOp = new ReprojectionOp();
            reprojectionOp.setParameter("crs", "EPSG:4326");
            reprojectionOp.setSourceProduct(currentProduct);
            sourceProducts[i] = reprojectionOp.getTargetProduct();
        }

        File lcTile = CalvalusProductIO.copyFileToLocal(paths[paths.length - 1], context.getConfiguration());
        Product lcProduct = ProductIO.readProduct(lcTile);

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

        S2FireGridDataSource(String tile, Product sourceProducts[], Product lcProduct) {
            this.tile = tile;
            this.sourceProducts = sourceProducts;
            this.lcProduct = lcProduct;
        }

        @Override
        public SourceData readPixels(int x, int y) throws IOException {

            Product[] products = filter(tile, sourceProducts, x, y);

            GridFormatUtils.ProductSpec productSpec = GridFormatUtils.getTargetSpec(products);
            Product temp = new Product("temp", "temp", productSpec.width, productSpec.height);
            temp.setSceneGeoCoding(getSceneGeoCoding(productSpec));
            Band jd = temp.addBand("JD", ProductData.TYPE_INT32);
            int[] jdBuffer = new int[productSpec.width * productSpec.height];
            jd.setData(new ProductData.Int(jdBuffer));

            for (Product product : products) {
                for (int pixelIndex = 0; pixelIndex < jdBuffer.length; pixelIndex++) {
                    int targetPixelX = pixelIndex % productSpec.width;
                    int targetPixelY = pixelIndex / productSpec.height;
                    GeoPos geoPos = temp.getSceneGeoCoding().getGeoPos(new PixelPos(targetPixelX, targetPixelY), null);
                    PixelPos sourcePixelPos = product.getSceneGeoCoding().getPixelPos(geoPos, null);
                    long sourceJD = ProductUtils.getGeophysicalSampleAsLong(product.getBand("JD"), (int) sourcePixelPos.x, (int) sourcePixelPos.y, 0);
                    if (jdBuffer[pixelIndex] == 0) {
                        jdBuffer[pixelIndex] = (int) sourceJD;
                    }
                }
            }

            SourceData data = new SourceData(productSpec.width, productSpec.height);
            System.arraycopy(jdBuffer, 0, data.pixels, 0, jdBuffer.length);
            double[] areaBuffer = new double[data.areas.length];
            getAreas(temp.getSceneGeoCoding(), productSpec.width, productSpec.height, areaBuffer);

            Band lcClassification = lcProduct.getBand("lcclass");
            lcClassification.readPixels(x * 90, y * 90, 90, 90, data.lcClasses);

            data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels, productSpec.width, productSpec.height), true);
            data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels, productSpec.width, productSpec.height), false);

            return data;
        }

        private static CrsGeoCoding getSceneGeoCoding(GridFormatUtils.ProductSpec productSpec) {
            try {
                return new CrsGeoCoding(DefaultGeographicCRS.WGS84,
                        productSpec.width, productSpec.height,
                        productSpec.ul.x, productSpec.lr.y,
                        S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE
                );
            } catch (FactoryException | TransformException e) {
                throw new IllegalStateException("Unable to create temporary geo-coding", e);
            }
        }

    }
}