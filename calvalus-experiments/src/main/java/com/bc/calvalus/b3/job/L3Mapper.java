package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.BinningContext;
import com.bc.calvalus.b3.ObservationSlice;
import com.bc.calvalus.b3.SpatialBin;
import com.bc.calvalus.b3.SpatialBinProcessor;
import com.bc.calvalus.b3.SpatialBinner;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import com.bc.calvalus.hadoop.io.FSImageInputStream;
import com.bc.ceres.glevel.MultiLevelImage;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.GeoCoding;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.datamodel.RasterDataNode;
import org.esa.beam.framework.datamodel.VirtualBand;
import org.esa.beam.jai.ImageManager;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import java.awt.Point;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads an N1 product and produces an emits (binIndex, spatialBin) pairs.
 *
 * @author Marco Zuehlke
 * @author Norman Fomferra
 */
public class L3Mapper extends Mapper<NullWritable, NullWritable, IntWritable, SpatialBin> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        final SpatialBinEmitter spatialBinEmitter = new SpatialBinEmitter(context);

        final BinningContext ctx = L3Config.getBinningContext(context.getConfiguration());

        JAI.enableDefaultTileCache();
        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(512 * 1024 * 1024); // 512 MB

        final int numScansPerSlice = context.getConfiguration().getInt(L3Config.CONFNAME_L3_NUM_SCANS_PER_SLICE, L3Config.DEFAULT_L3_NUM_SCANS_PER_SLICE);
        // todo - must tell BEAM to use this tileHeight in all readers, not only Envisat reader
        System.setProperty("beam.envisat.tileHeight", Integer.toString(numScansPerSlice));

        final FileSplit split = (FileSplit) context.getInputSplit();

        // write initial log entry for runtime measurements
        LOG.info(MessageFormat.format("{0} starts processing of split {1}", context.getTaskAttemptID(), split));
        final long startTime = System.nanoTime();

        // open the single split as ImageInputStream and get a product via an Envisat product reader
        final Path path = split.getPath();
        final FileSystem inputFileSystem = path.getFileSystem(context.getConfiguration());
        final FSDataInputStream fsDataInputStream = inputFileSystem.open(path);
        final FileStatus status = inputFileSystem.getFileStatus(path);
        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        final ProductReader productReader = plugIn.createReaderInstance();
        final SpatialBinner spatialBinner = new SpatialBinner(ctx, spatialBinEmitter);

        ImageInputStream imageInputStream = new FSImageInputStream(fsDataInputStream, status.getLen());
        try {
            final Product product = productReader.readProductNodes(imageInputStream, null);
            processProduct(product, numScansPerSlice, ctx, spatialBinner);
            product.dispose();
        } finally {
            imageInputStream.close();
        }

        // write final log entry for runtime measurements
        long stopTime = System.nanoTime();
        LOG.info(MessageFormat.format("{0} stops processing of split {1} after {2} sec ({3} observations seen, {4} bins produced)",
                                      context.getTaskAttemptID(), split, (stopTime - startTime) / 1E9, spatialBinEmitter.numObsTotal, spatialBinEmitter.numBinsTotal));

        final Exception[] exceptions = spatialBinner.getExceptions();
        for (int i = 0; i < exceptions.length; i++) {
            Exception exception = exceptions[i];
            String m = MessageFormat.format("Failed to process input slice of split {0}", split);
            LOG.log(Level.SEVERE, m, exception);
        }
    }

    static void processProduct(Product product, int sliceHeight, BinningContext ctx, SpatialBinner spatialBinner) {
        if (product.getGeoCoding() == null) {
            throw new IllegalArgumentException("product.getGeoCoding() == null");
        }

        final int sliceWidth = product.getSceneRasterWidth();

        for (int i = 0; i < ctx.getVariableContext().getVariableCount(); i++) {
            String variableName = ctx.getVariableContext().getVariableName(i);
            String variableExpr = ctx.getVariableContext().getVariableExpr(i);
            if (variableExpr != null) {
                VirtualBand band = new VirtualBand(variableName,
                                                   ProductData.TYPE_FLOAT32,
                                                   product.getSceneRasterWidth(),
                                                   product.getSceneRasterHeight(),
                                                   variableExpr);
                band.setValidPixelExpression(ctx.getVariableContext().getMaskExpr());
                product.addBand(band);
            }
        }

        final String maskExpr = ctx.getVariableContext().getMaskExpr();
        final MultiLevelImage maskImage = ImageManager.getInstance().getMaskImage(maskExpr, product);
        checkImageTileSize(MessageFormat.format("Mask image for expr ''{0}''", maskExpr),
                           maskImage, sliceWidth, sliceHeight);

        final MultiLevelImage[] varImages = new MultiLevelImage[ctx.getVariableContext().getVariableCount()];
        for (int i = 0; i < ctx.getVariableContext().getVariableCount(); i++) {
            final String nodeName = ctx.getVariableContext().getVariableName(i);
            final RasterDataNode node = getRasterDataNode(product, nodeName);
            final MultiLevelImage varImage = node.getGeophysicalImage();
            checkImageTileSize(MessageFormat.format("Geophysical image for node ''{0}''", node.getName()),
                               maskImage, sliceWidth, sliceHeight);
            varImages[i] = varImage;
        }

        final GeoCoding geoCoding = product.getGeoCoding();
        final Point[] tileIndices = maskImage.getTileIndices(null);
        ObservationSlice observationSlice;
        for (Point tileIndex : tileIndices) {
            observationSlice = createObservationSlice(geoCoding,
                                                      maskImage, varImages,
                                                      tileIndex,
                                                      sliceWidth, sliceHeight);
            spatialBinner.processObservationSlice(observationSlice);
        }

        spatialBinner.complete();
    }

    private static ObservationSlice createObservationSlice(GeoCoding geoCoding,
                                                           RenderedImage maskImage,
                                                           RenderedImage[] varImages,
                                                           Point tileIndex,
                                                           int sliceWidth,
                                                           int sliceHeight) {

        final Raster maskRaster = maskImage.getTile(tileIndex.x, tileIndex.y);
        final Raster[] varRasters = new Raster[varImages.length];
        for (int i = 0; i < varImages.length; i++) {
            varRasters[i] = varImages[i].getTile(tileIndex.x, tileIndex.y);
        }

        final ObservationSlice observationSlice = new ObservationSlice(varRasters, sliceWidth * sliceHeight);
        final int y1 = maskRaster.getMinY();
        final int y2 = y1 + maskRaster.getHeight();
        final int x1 = maskRaster.getMinX();
        final int x2 = x1 + sliceWidth;
        final PixelPos pixelPos = new PixelPos();
        final GeoPos geoPos = new GeoPos();
        for (int y = y1; y < y2; y++) {
            for (int x = x1; x < x2; x++) {
                if (maskRaster.getSample(x, y, 0) != 0) {
                    pixelPos.setLocation(x + 0.5f, y + 0.5f);
                    geoCoding.getGeoPos(pixelPos, geoPos);
                    observationSlice.addObservation(geoPos.lat, geoPos.lon, x, y);
                }
            }
        }

        return observationSlice;
    }

    private static RasterDataNode getRasterDataNode(Product product, String nodeName) {
        final RasterDataNode node = product.getRasterDataNode(nodeName);
        if (node == null) {
            throw new IllegalStateException(String.format("Can't find raster data node '%s' in product '%s'",
                                                          nodeName, product.getName()));
        }
        return node;
    }

    private static void checkImageTileSize(String msg, MultiLevelImage image, int sliceWidth, int sliceHeight) {
        if (image.getTileWidth() != sliceWidth
                || image.getTileHeight() != sliceHeight) {
            throw new IllegalStateException(MessageFormat.format(
                    "{0}: unexpected slice size detected: " +
                            "expected {1} x {2}, " +
                            "but was image tile size was {3} x {4} pixels (BEAM reader problem?)",
                    msg,
                    sliceWidth,
                    sliceHeight,
                    image.getTileWidth(),
                    image.getTileHeight()));
        }
    }

    private static class SpatialBinEmitter implements SpatialBinProcessor {
        private Context context;
        int numObsTotal = 0;
        int numBinsTotal = 0;

        public SpatialBinEmitter(Context context) {
            this.context = context;
        }

        @Override
        public void processSpatialBinSlice(BinningContext ctx, List<SpatialBin> spatialBins) throws Exception {
            for (SpatialBin spatialBin : spatialBins) {
                context.write(new IntWritable(spatialBin.getIndex()), spatialBin);
                numObsTotal += spatialBin.getNumObs();
                numBinsTotal++;
            }
        }
    }

}
