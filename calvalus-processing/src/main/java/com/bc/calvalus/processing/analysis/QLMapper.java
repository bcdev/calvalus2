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
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProductSplitProgressMonitor;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.bc.ceres.glayer.CollectionLayer;
import com.bc.ceres.glayer.support.ImageLayer;
import com.bc.ceres.grender.Viewport;
import com.bc.ceres.grender.support.BufferedImageRendering;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.ColorPaletteDef;
import org.esa.beam.framework.datamodel.ImageInfo;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.RGBChannelDef;
import org.esa.beam.framework.datamodel.RGBImageProfile;
import org.esa.beam.framework.datamodel.Stx;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.glevel.BandImageMultiLevelSource;
import org.esa.beam.util.PropertyMap;
import org.esa.beam.util.io.FileUtils;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * A mapper for generating quick-looks of products.
 */
public class QLMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    static {
        try {
            // Make "hdfs:" a recognised URL protocol
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Throwable e) {
            // ignore as it is most likely already set
            String msg = String.format("Cannot set URLStreamHandlerFactory (message: '%s'). " +
                                               "This may not be a problem because it is most likely already set.",
                                       e.getMessage());
            CalvalusLogger.getLogger().fine(msg);
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException, ProcessorException {
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProductSplitProgressMonitor(context);
        pm.beginTask("Image generation", 100);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, 5));
            if (product != null) {
                Path inputPath = processorAdapter.getInputPath();
                QLConfig qlConfig = QLConfig.get(context.getConfiguration());
                String name = FileUtils.getFilenameWithoutExtension(inputPath.getName());
                String qlName = name + "." + qlConfig.imageType;
                Path path = new Path(FileOutputFormat.getWorkOutputPath(context), qlName);
                OutputStream quickLookOutputStream = path.getFileSystem(context.getConfiguration()).create(path);
                try {
                    createQuicklookImage(product, quickLookOutputStream, qlConfig);
                } finally {
                    quickLookOutputStream.close();
                }
            }
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    private static void createQuicklookImage(Product product, OutputStream outputStream, QLConfig qlConfig) throws IOException {

        if (qlConfig.subSamplingX > 0 || qlConfig.subSamplingY > 0) {
            Map<String, Object> subsetParams = new HashMap<String, Object>();
            subsetParams.put("subSamplingX", qlConfig.subSamplingX);
            subsetParams.put("subSamplingY", qlConfig.subSamplingY);
            product = GPF.createProduct("Subset", subsetParams, product);
        }
        BandImageMultiLevelSource multiLevelSource;
        if (qlConfig.RGBAExpressions != null && qlConfig.RGBAExpressions.length > 0) {
            String[] rgbaExpressions;
            if (qlConfig.RGBAExpressions.length == 4) {
                rgbaExpressions = qlConfig.RGBAExpressions;
            } else if (qlConfig.RGBAExpressions.length == 3) {
                rgbaExpressions = new String[4];
                System.arraycopy(qlConfig.RGBAExpressions, 0, rgbaExpressions, 0, qlConfig.RGBAExpressions.length);
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
            for (Band band : rgbBands) {
                band.setNoDataValue(Float.NaN);
                band.setNoDataValueUsed(true);
            }
            multiLevelSource = BandImageMultiLevelSource.create(rgbBands, ProgressMonitor.NULL);
            if (qlConfig.v1 != null && qlConfig.v2 != null && qlConfig.v1.length == qlConfig.v2.length) {
                ImageInfo imageInfo = multiLevelSource.getImageInfo();
                RGBChannelDef rgbChannelDef = imageInfo.getRgbChannelDef();
                for (int i = 0; i < qlConfig.v1.length; i++) {
                    rgbChannelDef.setMinDisplaySample(i, qlConfig.v1[i]);
                    rgbChannelDef.setMaxDisplaySample(i, qlConfig.v2[i]);
                }
            }
        } else if (qlConfig.bandName != null) {
            Band band = product.getBand(qlConfig.bandName);
            multiLevelSource = BandImageMultiLevelSource.create(band, ProgressMonitor.NULL);
            if (qlConfig.cpdURL != null) {
                InputStream inputStream = new URL(qlConfig.cpdURL).openStream();
                try {
                    ColorPaletteDef colorPaletteDef = loadColorPaletteDef(inputStream);
                    ImageInfo imageInfo = multiLevelSource.getImageInfo();
                    if (band.getIndexCoding() != null) {
                        imageInfo.setColors(colorPaletteDef.getColors());
                    } else {
                        Stx stx = band.getStx();
                        imageInfo.setColorPaletteDef(colorPaletteDef,
                                                     stx.getMinimum(),
                                                     stx.getMaximum(), false);
                    }
                } finally {
                    inputStream.close();
                }
            }
        } else {
            throw new IllegalArgumentException("Neither RGB nor band information given");
        }
        final ImageLayer imageLayer = new ImageLayer(multiLevelSource);

        CollectionLayer collectionLayer = new CollectionLayer();
        collectionLayer.getChildren().add(imageLayer);
//        Layer landMask = MaskLayerType.createLayer(rgbBands[0], product.getMaskGroup().get("l1p_cc_land"));
//        landMask.setVisible(true);
//        Layer coastlineMask = MaskLayerType.createLayer(rgbBands[0], product.getMaskGroup().get("l1p_cc_coastline"));
//        coastlineMask.setVisible(true);
//        collectionLayer.getChildren().add(0, landMask);
//        collectionLayer.getChildren().add(1, coastlineMask);

        if (qlConfig.overlayURL != null) {
            InputStream inputStream = new URL(qlConfig.overlayURL).openStream();
            BufferedImage bufferedImage = ImageIO.read(inputStream);
            final ImageLayer overlayLayer = new ImageLayer(bufferedImage, imageLayer.getImageToModelTransform(), 1);
            collectionLayer.getChildren().add(0, overlayLayer);
        }
        boolean useAlpha = useAlpha(qlConfig);
        int imageType = useAlpha ? BufferedImage.TYPE_4BYTE_ABGR : BufferedImage.TYPE_3BYTE_BGR;
        BufferedImage bufferedImage = new BufferedImage(product.getSceneRasterWidth(), product.getSceneRasterHeight(), imageType);
        BufferedImageRendering rendering = new BufferedImageRendering(bufferedImage);
        Viewport viewport = rendering.getViewport();
        viewport.setModelYAxisDown(isModelYAxisDown(imageLayer));
        viewport.zoom(collectionLayer.getModelBounds());

        if (!useAlpha) {
            final Graphics2D graphics = rendering.getGraphics();
            graphics.setColor(Color.BLACK);
            graphics.fillRect(0, 0, product.getSceneRasterWidth(), product.getSceneRasterHeight());
        }

        collectionLayer.render(rendering);
        BufferedImage image = rendering.getImage();
        ImageIO.write(image, qlConfig.imageType, outputStream);
    }

    private static boolean useAlpha(QLConfig qlConfig) {
        return !"bmp".equals(qlConfig.imageType) && !"jpeg".equals(qlConfig.imageType);
    }

    private static boolean isModelYAxisDown(ImageLayer imageLayer) {
        return imageLayer.getImageToModelTransform().getDeterminant() > 0.0;
    }

    /**
     * Taken from  ColorPaletteDef. modified to use InputStream
     */
    public static ColorPaletteDef loadColorPaletteDef(InputStream inputStream) throws IOException {
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


//    public static void main(String[] args) throws IOException {
//
//        SystemUtils.init3rdPartyLibs(Thread.currentThread().getContextClassLoader());
//        JAI.enableDefaultTileCache();
//        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(512 * 1024 * 1014);
//        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
//
//        File arg = new File(args[0]);
//        Product product = null;
//        try {
//            product = ProductIO.readProduct(arg, "NetCDF4-BEAM");
//            if (product != null) {
//                QLConfig qlConfig = new QLConfig();
//                qlConfig.imageType = "jpeg";
//                qlConfig.RGBAExpressions = new String[]{"radiance_7_mean", "radiance_5_mean", "radiance_3_mean"};
//
//                String qlName = "TEST." + qlConfig.imageType;
//                OutputStream quickLookOutputStream = new FileOutputStream(new File(arg.getParentFile(), qlName));
//
//                try {
//                    createQuicklookImage(product, quickLookOutputStream, qlConfig);
//                } finally {
//                    quickLookOutputStream.close();
//                }
//            }
//        } finally {
//            if (product != null) {
//                product.dispose();
//            }
//        }
//    }

}
