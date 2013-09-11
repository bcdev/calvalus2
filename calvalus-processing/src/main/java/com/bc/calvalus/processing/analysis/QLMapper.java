/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.analysis;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProductSplitProgressMonitor;
import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.bc.ceres.glayer.CollectionLayer;
import com.bc.ceres.glayer.Layer;
import com.bc.ceres.glayer.LayerTypeRegistry;
import com.bc.ceres.glayer.support.ImageLayer;
import com.bc.ceres.grender.Rendering;
import com.bc.ceres.grender.Viewport;
import com.bc.ceres.grender.support.BufferedImageRendering;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.ColorPaletteDef;
import org.esa.beam.framework.datamodel.ImageInfo;
import org.esa.beam.framework.datamodel.ImageLegend;
import org.esa.beam.framework.datamodel.Mask;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductNodeGroup;
import org.esa.beam.framework.datamodel.RGBChannelDef;
import org.esa.beam.framework.datamodel.RGBImageProfile;
import org.esa.beam.framework.datamodel.Stx;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.glayer.MaskLayerType;
import org.esa.beam.glayer.NoDataLayerType;
import org.esa.beam.glevel.BandImageMultiLevelSource;
import org.esa.beam.util.PropertyMap;
import org.esa.beam.util.io.FileUtils;

import javax.imageio.ImageIO;
import javax.media.jai.Interpolation;
import javax.media.jai.operator.ScaleDescriptor;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A mapper for generating quick-looks of products.
 */
public class QLMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    private static final String FILE_SYSTEM_COUNTERS = "FileSystemCounters";
    private static final String FILE_BYTES_WRITTEN = "FILE_BYTES_WRITTEN";

    public static final Logger LOGGER = CalvalusLogger.getLogger();

    static {
        try {
            // Make "hdfs:" a recognised URL protocol
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Throwable e) {
            // ignore as it is most likely already set
            String msg = String.format("Cannot set URLStreamHandlerFactory (message: '%s'). " +
                                       "This may not be a problem because it is most likely already set.",
                                       e.getMessage());
            LOGGER.fine(msg);
        }
    }

    @Override
    public void run(Mapper.Context context) throws IOException, InterruptedException {
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProductSplitProgressMonitor(context);
        pm.beginTask("Image generation", 100);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, 5));
            if (product != null) {
                String productName = FileUtils.getFilenameWithoutExtension(processorAdapter.getInputPath().getName());
                Quicklooks.QLConfig[] configs = Quicklooks.get(context.getConfiguration());
                for (Quicklooks.QLConfig config : configs) {
                    String imageFileName = productName + "_" + config.getBandName();
                    createQuicklook(product, imageFileName, context, config);
                }
            }
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    public static void createQuicklook(Product product, String imageFileName, Mapper.Context context,
                                       Quicklooks.QLConfig config) {
        try {
            RenderedImage quicklookImage = createImage(context, product, config);
            if (quicklookImage != null) {
                OutputStream outputStream = createOutputStream(context, imageFileName + "." + config.getImageType());
                OutputStream pmOutputStream = new BytesCountingOutputStream(outputStream, context);
                try {
                    ImageIO.write(quicklookImage, config.getImageType(), pmOutputStream);
                } finally {
                    outputStream.close();
                }
            }
        } catch (Exception e) {
            String msg = String.format("Could not create quicklook image '%s'.", config.getBandName());
            LOGGER.log(Level.WARNING, msg, e);
        }
    }

    private static OutputStream createOutputStream(Mapper.Context context, String fileName) throws Exception {
        Path path = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);
        final FSDataOutputStream fsDataOutputStream = path.getFileSystem(context.getConfiguration()).create(path);
        return new BufferedOutputStream(fsDataOutputStream);
    }

    static RenderedImage createImage(final Mapper.Context context, Product product, Quicklooks.QLConfig qlConfig) throws IOException {

        if (qlConfig.getSubSamplingX() > 0 || qlConfig.getSubSamplingY() > 0) {
            Map<String, Object> subsetParams = new HashMap<String, Object>();
            subsetParams.put("subSamplingX", qlConfig.getSubSamplingX());
            subsetParams.put("subSamplingY", qlConfig.getSubSamplingY());
            product = GPF.createProduct("Subset", subsetParams, product);
        }
        BandImageMultiLevelSource multiLevelSource;
        Band masterBand;
        if (qlConfig.getRGBAExpressions() != null && qlConfig.getRGBAExpressions().length > 0) {
            String[] rgbaExpressions;
            if (qlConfig.getRGBAExpressions().length == 4) {
                rgbaExpressions = qlConfig.getRGBAExpressions();
            } else if (qlConfig.getRGBAExpressions().length == 3) {
                rgbaExpressions = new String[4];
                System.arraycopy(qlConfig.getRGBAExpressions(), 0, rgbaExpressions, 0,
                                 qlConfig.getRGBAExpressions().length);
                rgbaExpressions[3] = "";
            } else {
                throw new IllegalArgumentException("RGBA expression must contain 3 or 4 band names");
            }
            RGBImageProfile.storeRgbaExpressions(product, rgbaExpressions);
            final Band[] rgbBands = {
                    product.getBand(RGBImageProfile.RED_BAND_NAME),
                    product.getBand(RGBImageProfile.GREEN_BAND_NAME),
                    product.getBand(RGBImageProfile.BLUE_BAND_NAME),
            };
            masterBand = rgbBands[0];
            for (Band band : rgbBands) {
                band.setNoDataValue(Float.NaN);
                band.setNoDataValueUsed(true);
            }
            multiLevelSource = BandImageMultiLevelSource.create(rgbBands, ProgressMonitor.NULL);
            if (qlConfig.getRGBAMinSamples() != null && qlConfig.getRGBAMaxSamples() != null &&
                qlConfig.getRGBAMinSamples().length == qlConfig.getRGBAMaxSamples().length) {
                ImageInfo imageInfo = multiLevelSource.getImageInfo();
                RGBChannelDef rgbChannelDef = imageInfo.getRgbChannelDef();
                for (int i = 0; i < qlConfig.getRGBAMinSamples().length; i++) {
                    rgbChannelDef.setMinDisplaySample(i, qlConfig.getRGBAMinSamples()[i]);
                    rgbChannelDef.setMaxDisplaySample(i, qlConfig.getRGBAMaxSamples()[i]);
                }
            }
        } else if (qlConfig.getBandName() != null) {
            masterBand = product.getBand(qlConfig.getBandName());
            String cpdURL = qlConfig.getCpdURL();
            if (masterBand == null) {
                String msg = String.format("Could not create quicklook. Product does not contain band '%s'",
                                           qlConfig.getBandName());
                LOGGER.warning(msg);
                return null;
            }
            if (cpdURL == null) {
                String msg = String.format("Could not create quicklook. No CPD-URL given for band '%s'",
                                           qlConfig.getBandName());
                LOGGER.warning(msg);
                return null;

            }

            masterBand.getImageInfo(new ProgressableWrappingPM(context));
            multiLevelSource = BandImageMultiLevelSource.create(masterBand, new ProgressableWrappingPM(context));

            InputStream inputStream = new URL(cpdURL).openStream();
            try {
                ColorPaletteDef colorPaletteDef = loadColorPaletteDef(inputStream);
                ImageInfo imageInfo = multiLevelSource.getImageInfo();
                if (masterBand.getIndexCoding() != null) {
                    imageInfo.setColors(colorPaletteDef.getColors());
                } else {
                    Stx stx = masterBand.getStx();
                    imageInfo.setColorPaletteDef(colorPaletteDef,
                                                 stx.getMinimum(),
                                                 stx.getMaximum(), false);
                }
            } finally {
                inputStream.close();
            }
        } else {
            throw new IllegalArgumentException("Neither RGB nor band information given");
        }
        final ImageLayer imageLayer = new ImageLayer(multiLevelSource);
        boolean canUseAlpha = canUseAlpha(qlConfig);
        CollectionLayer collectionLayer = new CollectionLayer();
        List<Layer> layerChildren = collectionLayer.getChildren();

        layerChildren.add(0, imageLayer);

        if (qlConfig.getMaskOverlays() != null) {
            addMaskOverlays(product, qlConfig.getMaskOverlays(), masterBand, layerChildren);
        }

        // TODO generalize
        Configuration configuration = context.getConfiguration();
        if ("FRESHMON".equalsIgnoreCase(configuration.get(JobConfigNames.CALVALUS_PROJECT_NAME))) {
            addFreshmonOverlay(qlConfig, masterBand, imageLayer, canUseAlpha, layerChildren);
        } else {
            if (qlConfig.getOverlayURL() != null) {
                addOverlay(imageLayer, layerChildren, qlConfig.getOverlayURL());
            }
            if (qlConfig.isLegendEnabled()) {
                addLegend(masterBand, imageLayer, canUseAlpha, layerChildren);
            }
        }


        Rectangle2D modelBounds = collectionLayer.getModelBounds();
        Rectangle2D imageBounds = imageLayer.getModelToImageTransform().createTransformedShape(modelBounds).getBounds2D();
        int imageType = canUseAlpha ? BufferedImage.TYPE_4BYTE_ABGR : BufferedImage.TYPE_3BYTE_BGR;
        BufferedImage bufferedImage = new BufferedImage((int) imageBounds.getWidth(),
                                                        (int) imageBounds.getHeight(), imageType);

        final BufferedImageRendering rendering = new BufferedImageRendering(bufferedImage);
        Viewport viewport = rendering.getViewport();
        viewport.setModelYAxisDown(isModelYAxisDown(imageLayer));
        viewport.zoom(modelBounds);

        final Graphics2D graphics = rendering.getGraphics();
        graphics.setColor(qlConfig.getBackgroundColor());
        graphics.fill(imageBounds);

        collectionLayer.render(new Rendering() {
            @Override
            public Graphics2D getGraphics() {
                context.progress();
                return rendering.getGraphics();
            }

            @Override
            public Viewport getViewport() {
                context.progress();
                return rendering.getViewport();
            }
        });
        return rendering.getImage();
    }

    private static void addFreshmonOverlay(Quicklooks.QLConfig qlConfig, Band masterBand, ImageLayer imageLayer,
                                           boolean canUseAlpha, List<Layer> layerChildren) throws IOException {
        BufferedImage legend = createImageLegend(masterBand, canUseAlpha, ImageLegend.VERTICAL);
        RenderedImage logo = ImageIO.read(new URL(qlConfig.getOverlayURL()).openStream());
        float scale = (float) legend.getWidth() / (float) logo.getWidth();
        RenderingHints hints = new RenderingHints(RenderingHints.KEY_ANTIALIASING,
                                                  RenderingHints.VALUE_ANTIALIAS_ON);
        logo = ScaleDescriptor.create(logo, scale, scale, 0.0f, 0.0f,
                                      Interpolation.getInstance(Interpolation.INTERP_NEAREST), hints);

        AffineTransform legendI2M = imageLayer.getImageToModelTransform();
        legendI2M.translate(masterBand.getSceneRasterWidth() + 10, logo.getHeight());
        final ImageLayer legendLayer = new ImageLayer(legend, legendI2M, 1);

        layerChildren.add(0, legendLayer);

        AffineTransform logoI2M = imageLayer.getImageToModelTransform();
        logoI2M.translate(masterBand.getSceneRasterWidth() + 10, 0);
        final ImageLayer logoLayer = new ImageLayer(logo, logoI2M, 1);

        layerChildren.add(0, logoLayer);

        NoDataLayerType layerType = LayerTypeRegistry.getLayerType(NoDataLayerType.class);
        PropertySet container = layerType.createLayerConfig(null);
        container.setValue(NoDataLayerType.PROPERTY_NAME_COLOR, Color.BLACK);
        container.setValue(NoDataLayerType.PROPERTY_NAME_RASTER, masterBand);
        final Layer noDataLayer = layerType.createLayer(null, container);
        noDataLayer.setVisible(true);
        layerChildren.add(noDataLayer); // add layer as background
    }

    private static void addOverlay(ImageLayer imageLayer, List<Layer> layerChildren, String overlayURL) throws
                                                                                                        IOException {
        InputStream inputStream = new URL(overlayURL).openStream();
        BufferedImage bufferedImage = ImageIO.read(inputStream);
        final ImageLayer overlayLayer = new ImageLayer(bufferedImage, imageLayer.getImageToModelTransform(), 1);
        layerChildren.add(0, overlayLayer);
    }

    private static void addLegend(Band masterBand, ImageLayer imageLayer, boolean useAlpha, List<Layer> layerChildren) {
        BufferedImage legend = createImageLegend(masterBand, useAlpha, ImageLegend.VERTICAL);

        AffineTransform imageToModelTransform = imageLayer.getImageToModelTransform();
        imageToModelTransform.translate(masterBand.getSceneRasterWidth() - legend.getWidth(),
                                        masterBand.getSceneRasterHeight() - legend.getHeight());
        final ImageLayer overlayLayer = new ImageLayer(legend, imageToModelTransform, 1);
        layerChildren.add(0, overlayLayer);
    }

    private static BufferedImage createImageLegend(Band masterBand, boolean useAlpha, int orientation) {
        ImageLegend imageLegend = new ImageLegend(masterBand.getImageInfo(), masterBand);
        imageLegend.setHeaderText(masterBand.getName());
        imageLegend.setOrientation(orientation);
        imageLegend.setBackgroundTransparency(0.6f);
        imageLegend.setBackgroundTransparencyEnabled(useAlpha);
        imageLegend.setAntialiasing(true);
        return imageLegend.createImage();
    }

    private static void addMaskOverlays(Product product, String[] maskOverlays, Band masterBand,
                                        List<Layer> layerChildren) {
        ProductNodeGroup<Mask> maskGroup = product.getMaskGroup();
        for (String maskOverlay : maskOverlays) {
            Layer layer = MaskLayerType.createLayer(masterBand, maskGroup.get(maskOverlay));
            layer.setVisible(true);
            layerChildren.add(0, layer);
        }
    }

    private static boolean canUseAlpha(Quicklooks.QLConfig qlConfig) {
        return !"bmp".equalsIgnoreCase(qlConfig.getImageType()) && !"jpeg".equalsIgnoreCase(qlConfig.getImageType());
    }

    private static boolean isModelYAxisDown(ImageLayer imageLayer) {
        return imageLayer.getImageToModelTransform().getDeterminant() > 0.0;
    }

    /**
     * Taken from  ColorPaletteDef. modified to use InputStream
     */
    private static ColorPaletteDef loadColorPaletteDef(InputStream inputStream) throws IOException {
        final PropertyMap propertyMap = new PropertyMap();
        propertyMap.getProperties().load(inputStream);

        final int numPoints = propertyMap.getPropertyInt("numPoints");
        if (numPoints < 2) {
            throw new IOException("The selected file contains less than\n" +
                                  "two colour points.");
        }
        final ColorPaletteDef.Point[] points = new ColorPaletteDef.Point[numPoints];
        double lastSample = 0;
        for (int i = 0; i < points.length; i++) {
            final ColorPaletteDef.Point point = new ColorPaletteDef.Point();
            final Color color = propertyMap.getPropertyColor("color" + i);
            double sample = propertyMap.getPropertyDouble("sample" + i);
            if (i > 0 && sample < lastSample) {
                sample = lastSample + 1.0;
            }
            point.setColor(color);
            point.setSample(sample);
            points[i] = point;
            lastSample = sample;
        }
        ColorPaletteDef paletteDef = new ColorPaletteDef(points, 256);
        paletteDef.setAutoDistribute(propertyMap.getPropertyBool("autoDistribute", false));
        return paletteDef;
    }

    private static class BytesCountingOutputStream extends OutputStream {

        private final OutputStream wrappedStream;
        private final Mapper.Context context;
        private int countedBytes;

        public BytesCountingOutputStream(OutputStream outputStream, Mapper.Context context) {
            wrappedStream = outputStream;
            this.context = context;
        }

        @Override
        public void write(int b) throws IOException {
            wrappedStream.write(b);
            maybeIncrementHadoopCounter(1);
        }

        @Override
        public void write(byte[] b) throws IOException {
            wrappedStream.write(b);
            maybeIncrementHadoopCounter(b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            wrappedStream.write(b, off, len);
            maybeIncrementHadoopCounter(len - off);
        }

        @Override
        public void close() throws IOException {
            wrappedStream.close();
            incrementHadoopCounter();
        }

        @Override
        public void flush() throws IOException {
            wrappedStream.flush();
            incrementHadoopCounter();
        }


        private void maybeIncrementHadoopCounter(int byteCount) {
            countedBytes += byteCount;
            if (countedBytes / (1024 * 10) >= 1) {
                incrementHadoopCounter();
            }
        }

        private void incrementHadoopCounter() {
            context.getCounter(FILE_SYSTEM_COUNTERS, FILE_BYTES_WRITTEN).increment(countedBytes);
            context.progress();
            countedBytes = 0;
        }
    }

    private static class ProgressableWrappingPM implements ProgressMonitor {

        private final Progressable progressable;

        public ProgressableWrappingPM(Progressable progressable) {
            this.progressable = progressable;
        }

        @Override
        public void beginTask(String taskName, int totalWork) {
        }

        @Override
        public void done() {
        }

        @Override
        public void internalWorked(double work) {
            progressable.progress();
        }

        @Override
        public boolean isCanceled() {
            return false;
        }

        @Override
        public void setCanceled(boolean canceled) {
        }

        @Override
        public void setTaskName(String taskName) {
        }

        @Override
        public void setSubTaskName(String subTaskName) {
        }

        @Override
        public void worked(int work) {
            internalWorked(work);
        }
    }
}
