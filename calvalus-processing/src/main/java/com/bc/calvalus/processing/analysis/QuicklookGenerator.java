/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
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
import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.glayer.CollectionLayer;
import com.bc.ceres.glayer.Layer;
import com.bc.ceres.glayer.LayerContext;
import com.bc.ceres.glayer.LayerTypeRegistry;
import com.bc.ceres.glayer.support.ImageLayer;
import com.bc.ceres.grender.Rendering;
import com.bc.ceres.grender.Viewport;
import com.bc.ceres.grender.support.BufferedImageRendering;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.ColorPaletteDef;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.ImageInfo;
import org.esa.snap.core.datamodel.ImageLegend;
import org.esa.snap.core.datamodel.Mask;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.datamodel.RGBChannelDef;
import org.esa.snap.core.datamodel.RGBImageProfile;
import org.esa.snap.core.datamodel.Stx;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.image.ColoredBandImageMultiLevelSource;
import org.esa.snap.core.layer.MaskLayerType;
import org.esa.snap.core.layer.NoDataLayerType;
import org.esa.snap.core.util.DefaultPropertyMap;
import org.esa.snap.core.util.PropertyMap;

import javax.imageio.ImageIO;
import javax.media.jai.Interpolation;
import javax.media.jai.operator.ScaleDescriptor;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Genrates Quicklooks from products
 */
public class QuicklookGenerator {

    public static final Logger LOGGER = CalvalusLogger.getLogger();

    public static RenderedImage createImage(final TaskAttemptContext context, Product product, Quicklooks.QLConfig qlConfig) throws IOException {

        if (qlConfig.getSubSamplingX() > 0 || qlConfig.getSubSamplingY() > 0) {
            Map<String, Object> subsetParams = new HashMap<>();
            subsetParams.put("subSamplingX", qlConfig.getSubSamplingX());
            subsetParams.put("subSamplingY", qlConfig.getSubSamplingY());
            product = GPF.createProduct("Subset", subsetParams, product);
        }
        ColoredBandImageMultiLevelSource multiLevelSource;
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
            multiLevelSource = ColoredBandImageMultiLevelSource.create(rgbBands, ProgressMonitor.NULL);
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

            masterBand.getImageInfo(wrapPM(context));
            multiLevelSource = ColoredBandImageMultiLevelSource.create(masterBand, wrapPM(context));

            try (InputStream inputStream = new URL(cpdURL).openStream()) {
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
        if(qlConfig.getShapefileURL() != null) {
            layerChildren.add(0, createShapefileLayer(product, qlConfig.getShapefileURL()));
        }

//        // TODO generalize
//        Configuration configuration = context.getConfiguration();
//        if ("FRESHMON".equalsIgnoreCase(configuration.get(JobConfigNames.CALVALUS_PROJECT_NAME))) {
//            addFreshmonOverlay(qlConfig, masterBand, imageLayer, canUseAlpha, layerChildren);
//        } else {
//            if (qlConfig.getOverlayURL() != null) {
//                addOverlay(imageLayer, layerChildren, qlConfig.getOverlayURL());
//            }
//            if (qlConfig.isLegendEnabled()) {
//                addLegend(masterBand, imageLayer, canUseAlpha, layerChildren);
//            }
//        }


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
//                context.progress();
                return rendering.getGraphics();
            }

            @Override
            public Viewport getViewport() {
//                context.progress();
                return rendering.getViewport();
            }
        });
        return rendering.getImage();
    }

    private static Layer createShapefileLayer(final Product product, String shapefileUrl) throws IOException {
        final File shapefile = extractShapefile(shapefileUrl);
        final LayerContext layerContext = new LayerContext() {
            @Override
            public Object getCoordinateReferenceSystem() {
                final GeoCoding geoCoding = product.getSceneGeoCoding();
                if (geoCoding != null) {
                    return Product.findModelCRS(geoCoding);
                }
                return null;
            }

            @Override
            public Layer getRootLayer() {
                return null;
            }
        };
        try {
            return new ShapefileLoader().createLayer(product, shapefile, layerContext);
        } catch (Exception e) {
            throw  new IOException("could not create shape layer", e);
        }
//        FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection = getSimpleFeatureTypeSimpleFeatureFeatureCollection(shapefile, product, pm);
//        Style[] styles = SLDUtils.loadSLD(shapefile);
//        if (styles.length > 0) {
//            SimpleFeatureType featureType = SLDUtils.createStyledFeatureType(featureCollection.getSchema());
//            VectorDataNode vectorDataNode = new VectorDataNode(COASTLINE, featureType);
//            FeatureCollection<SimpleFeatureType, SimpleFeature> styledCollection = vectorDataNode.getFeatureCollection();
//            String defaultCSS = vectorDataNode.getDefaultStyleCss();
//            SLDUtils.applyStyle(styles[0], defaultCSS, featureCollection, styledCollection);
//            return vectorDataNode;
//        } else {
//            return new VectorDataNode(COASTLINE, featureCollection);
//        }
    }

//        // inlined here from org.esa.snap.core.util.FeatureUtils to allow for smaller clip rect
//    private static FeatureCollection<SimpleFeatureType, SimpleFeature> getSimpleFeatureTypeSimpleFeatureFeatureCollection(File shapefile, Product product, ProgressMonitor pm) throws IOException {
//        pm.beginTask("Loading Shapefile", 100);
//        try {
//            FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection = FeatureUtils.loadFeatureCollectionFromShapefile(shapefile);
//            pm.worked(10);
//            final CoordinateReferenceSystem targetCrs = ImageManager.getModelCrs(product.getGeoCoding());
//
//            final Rectangle clipRect = new Rectangle(SHAPE_PRODUCT_BORDER,
//                                                     SHAPE_PRODUCT_BORDER,
//                                                     product.getSceneRasterWidth() - 2 * SHAPE_PRODUCT_BORDER,
//                                                     product.getSceneRasterHeight() - 2 * SHAPE_PRODUCT_BORDER);
//            final Geometry clipGeometry = getGeometry(product, clipRect);
//            pm.worked(10);
//            CoordinateReferenceSystem featureCrs = featureCollection.getSchema().getCoordinateReferenceSystem();
//            return FeatureUtils.clipCollection(featureCollection,
//                                               featureCrs,
//                                               clipGeometry,
//                                               DefaultGeographicCRS.WGS84,
//                                               null,
//                                               targetCrs,
//                                               SubProgressMonitor.create(pm, 80));
//        } finally {
//            pm.done();
//        }
//    }
//
//    private static Geometry getGeometry(Product product, Rectangle clipRect) {
//        GeometryFactory gf = new GeometryFactory();
//        GeoPos[] geoPositions = ProductUtils.createGeoBoundary(product, clipRect, 100);
//        Coordinate[] coordinates;
//        if (geoPositions.length >= 0 && geoPositions.length <= 3) {
//            coordinates = new Coordinate[0];
//        } else {
//            coordinates = new Coordinate[geoPositions.length + 1];
//            for (int i = 0; i < geoPositions.length; i++) {
//                GeoPos geoPos = geoPositions[i];
//                coordinates[i] = new Coordinate(geoPos.lon, geoPos.lat);
//            }
//            coordinates[coordinates.length - 1] = coordinates[0];
//        }
//        return gf.createPolygon(gf.createLinearRing(coordinates), null);
//    }

    private static File extractShapefile(String shapefileUrl) throws IOException {
        File shapefile = null;
        final String shapeDir = Files.createTempDirectory("shapefile").toFile().getAbsolutePath();
        final byte[] buffer = new byte[2048];
        try (ZipInputStream zipInputStream = new ZipInputStream(new BufferedInputStream(new URL(shapefileUrl).openStream()))) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                FileOutputStream output = null;
                try {
                    final String outputfileName = shapeDir + "/" + entry.getName();
                    final File outputFile = new File(outputfileName);
                    if (outputfileName.toLowerCase().endsWith(".shp")) {
                        shapefile = outputFile;
                    }
                    output = new FileOutputStream(outputfileName);
                    int len;
                    while ((len = zipInputStream.read(buffer)) > 0) {
                        output.write(buffer, 0, len);
                    }
                } finally {
                    // we must always close the output file
                    if (output != null) output.close();
                }
            }
        }
        return shapefile;
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
        legendI2M.translate(masterBand.getRasterWidth() + 10, logo.getHeight());
        final ImageLayer legendLayer = new ImageLayer(legend, legendI2M, 1);

        layerChildren.add(0, legendLayer);

        AffineTransform logoI2M = imageLayer.getImageToModelTransform();
        logoI2M.translate(masterBand.getRasterWidth() + 10, 0);
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
        imageToModelTransform.translate(masterBand.getRasterWidth() - legend.getWidth(),
                                        masterBand.getRasterHeight() - legend.getHeight());
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
        final PropertyMap propertyMap = new DefaultPropertyMap();
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

    private static ProgressMonitor wrapPM(Progressable progressable) {
        if (progressable != null) {
            return new ProgressableWrappingPM(progressable);
        } else {
            return ProgressMonitor.NULL;
        }
    }

    private static class ProgressableWrappingPM implements ProgressMonitor {

            private final Progressable progressable;

            private ProgressableWrappingPM(Progressable progressable) {
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
