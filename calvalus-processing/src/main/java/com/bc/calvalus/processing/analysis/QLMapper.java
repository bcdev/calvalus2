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
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A mapper for generating quick-looks of products.
 */
public class QLMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {


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
    public void run(Context context) throws IOException, InterruptedException {
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProductSplitProgressMonitor(context);
        pm.beginTask("Image generation", 100);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, 5));
            if (product != null) {
                createQuicklooks(product, processorAdapter.getInputPath(), context);
            }
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    public static void createQuicklooks(Product product, Path inputPath, Mapper.Context context) throws
                                                                                                 IOException,
                                                                                                 InterruptedException {
        Quicklooks.QLConfig[] configs = Quicklooks.get(context.getConfiguration());
        for (Quicklooks.QLConfig config : configs) {
            String bandName = config.getBandName();
            if (bandName == null) {
                bandName = "RGB";
            }
            try {
                RenderedImage quicklookImage = createImage(product, config);
                if (quicklookImage != null) {
                    OutputStream outputStream = createOutputStream(context, inputPath, bandName, config);
                    try {
                        ImageIO.write(quicklookImage, config.getImageType(), outputStream);

                    } finally {
                        outputStream.close();
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Could not create quicklook image '" + bandName + "'.", e);
            }
        }
    }

    private static OutputStream createOutputStream(Mapper.Context context, Path inputPath,
                                                   String bandName, Quicklooks.QLConfig qlConfig)
            throws IOException, InterruptedException {

        String name = FileUtils.getFilenameWithoutExtension(inputPath.getName());
        String qlName = name + "_" + bandName + "." + qlConfig.getImageType();
        Path path = new Path(FileOutputFormat.getWorkOutputPath(context), qlName);
        return path.getFileSystem(context.getConfiguration()).create(path);
    }

    private static RenderedImage createImage(Product product, Quicklooks.QLConfig qlConfig) throws IOException {

        if (qlConfig.getSubSamplingX() > 0 || qlConfig.getSubSamplingY() > 0) {
            Map<String, Object> subsetParams = new HashMap<String, Object>();
            subsetParams.put("subSamplingX", qlConfig.getSubSamplingX());
            subsetParams.put("subSamplingY", qlConfig.getSubSamplingY());
            product = GPF.createProduct("Subset", subsetParams, product);
        }
        BandImageMultiLevelSource multiLevelSource;
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
            for (Band band : rgbBands) {
                band.setNoDataValue(Float.NaN);
                band.setNoDataValueUsed(true);
            }
            multiLevelSource = BandImageMultiLevelSource.create(rgbBands, ProgressMonitor.NULL);
            if (qlConfig.getRGBAMinSamples() != null && qlConfig.getRGBAMaxSamples() != null && qlConfig.getRGBAMinSamples().length == qlConfig.getRGBAMaxSamples().length) {
                ImageInfo imageInfo = multiLevelSource.getImageInfo();
                RGBChannelDef rgbChannelDef = imageInfo.getRgbChannelDef();
                for (int i = 0; i < qlConfig.getRGBAMinSamples().length; i++) {
                    rgbChannelDef.setMinDisplaySample(i, qlConfig.getRGBAMinSamples()[i]);
                    rgbChannelDef.setMaxDisplaySample(i, qlConfig.getRGBAMaxSamples()[i]);
                }
            }
        } else if (qlConfig.getBandName() != null) {
            Band band = product.getBand(qlConfig.getBandName());
            String cpdURL = qlConfig.getCpdURL();
            if (band == null) {
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
            multiLevelSource = BandImageMultiLevelSource.create(band, ProgressMonitor.NULL);
            InputStream inputStream = new URL(cpdURL).openStream();
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
        } else {
            throw new IllegalArgumentException("Neither RGB nor band information given");
        }
        final ImageLayer imageLayer = new ImageLayer(multiLevelSource);

        CollectionLayer collectionLayer = new CollectionLayer();
        collectionLayer.getChildren().add(imageLayer);

        if (qlConfig.getOverlayURL() != null) {
            InputStream inputStream = new URL(qlConfig.getOverlayURL()).openStream();
            BufferedImage bufferedImage = ImageIO.read(inputStream);
            final ImageLayer overlayLayer = new ImageLayer(bufferedImage, imageLayer.getImageToModelTransform(), 1);
            collectionLayer.getChildren().add(0, overlayLayer);
        }
        boolean useAlpha = useAlpha(qlConfig);
        int imageType = useAlpha ? BufferedImage.TYPE_4BYTE_ABGR : BufferedImage.TYPE_3BYTE_BGR;
        BufferedImage bufferedImage = new BufferedImage(product.getSceneRasterWidth(), product.getSceneRasterHeight(),
                                                        imageType);
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
        return rendering.getImage();
    }

    private static boolean useAlpha(Quicklooks.QLConfig qlConfig) {
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
