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

package com.bc.calvalus.processing.ql;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.beam.ProductFactory;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.glayer.CollectionLayer;
import com.bc.ceres.glayer.support.ImageLayer;
import com.bc.ceres.grender.Viewport;
import com.bc.ceres.grender.support.BufferedImageRendering;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.ImageInfo;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.RGBChannelDef;
import org.esa.beam.framework.datamodel.RGBImageProfile;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.glevel.BandImageMultiLevelSource;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A mapper for generating quick-looks of products.
 */
public class QLMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    public void run(Context context) throws IOException, InterruptedException, ProcessorException {
        Configuration jobConfig = context.getConfiguration();
        ProductFactory productFactory = new ProductFactory(jobConfig);

        final FileSplit split = (FileSplit) context.getInputSplit();
        Path inputPath = split.getPath();
        String inputFormat = jobConfig.get(JobConfigNames.CALVALUS_INPUT_FORMAT, null);
        Geometry regionGeometry = JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        Product product = productFactory.getProduct(inputPath,
                                                    inputFormat,
                                                    regionGeometry,
                                                    true,
                                                    null,
                                                    null);
        try {
            if (product != null) {
                QLConfig qlConfig = QLConfig.get(jobConfig);
                String qlName = product.getName() + "." + qlConfig.imageType;
                Path path = new Path(FileOutputFormat.getWorkOutputPath(context), qlName);
                OutputStream quickLookOutputStream = path.getFileSystem(context.getConfiguration()).create(path);
                try {
                    createQuicklookImage(product, quickLookOutputStream, qlConfig);
                } finally {
                    quickLookOutputStream.close();
                }
            }
        } finally {
            if (product != null) {
                product.dispose();
            }
            productFactory.dispose();
        }
    }

    private static void createQuicklookImage(Product product, OutputStream outputStream, QLConfig qlConfig) throws IOException {

        if (qlConfig.subSamplingX > 0 || qlConfig.subSamplingY > 0) {
            Map<String, Object> subsetParams = new HashMap<String, Object>();
            subsetParams.put("subSamplingX", qlConfig.subSamplingX);
            subsetParams.put("subSamplingY", qlConfig.subSamplingY);
            product = GPF.createProduct("Subset", subsetParams, product);
        }
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
        BandImageMultiLevelSource multiLevelSource = BandImageMultiLevelSource.create(rgbBands, ProgressMonitor.NULL);
        if (qlConfig.v1 != null && qlConfig.v2 != null && qlConfig.v1.length == qlConfig.v2.length) {
            ImageInfo imageInfo = multiLevelSource.getImageInfo();
            RGBChannelDef rgbChannelDef = imageInfo.getRgbChannelDef();
            for (int i = 0; i < qlConfig.v1.length; i++) {
                rgbChannelDef.setMinDisplaySample(i, qlConfig.v1[i]);
                rgbChannelDef.setMaxDisplaySample(i, qlConfig.v2[i]);
            }
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
        BufferedImageRendering rendering = new BufferedImageRendering(product.getSceneRasterWidth(),
                                                                      product.getSceneRasterHeight());
        Viewport viewport = rendering.getViewport();
        viewport.setModelYAxisDown(isModelYAxisDown(imageLayer));
        viewport.zoom(collectionLayer.getModelBounds());

        collectionLayer.render(rendering);
        BufferedImage image = rendering.getImage();
        ImageIO.write(image, qlConfig.imageType, outputStream);
    }

    private static boolean isModelYAxisDown(ImageLayer imageLayer) {
        return imageLayer.getImageToModelTransform().getDeterminant() > 0.0;
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
