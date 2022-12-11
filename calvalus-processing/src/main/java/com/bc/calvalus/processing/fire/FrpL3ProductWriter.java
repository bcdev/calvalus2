package com.bc.calvalus.processing.fire;

import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.AbstractProductWriter;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.ProductUtils;

import java.io.File;
import java.io.IOException;

public class FrpL3ProductWriter extends AbstractProductWriter {

    protected ProductWriter s3aNightWriter;
    protected ProductWriter s3bNightWriter;
    protected ProductWriter s3aDayWriter;
    protected ProductWriter s3bDayWriter;

    protected Product s3aNightProduct;
    protected Product s3bNightProduct;
    protected Product s3aDayProduct;
    protected Product s3bDayProduct;

    protected boolean withS3a;
    protected boolean withS3b;
    protected boolean withNighttime;
    protected boolean withDaytime;

    FrpL3ProductWriter(ProductWriterPlugIn writerPlugIn) {
        super(writerPlugIn);
        FrpL3ProductFileWriterPlugIn plugin = new FrpL3ProductFileWriterPlugIn();
        final String platformFilter = System.getProperty("calvalus.filterPlatform", null);
        withS3a = platformFilter == null || "S3A".equalsIgnoreCase(platformFilter);
        withS3b = platformFilter == null || "S3B".equalsIgnoreCase(platformFilter);
        withNighttime = System.getProperty("calvalus.filterNightOrDay", "night").startsWith("night");
        withDaytime = System.getProperty("calvalus.filterNightOrDay", "day").startsWith("day");
        if (withNighttime && withS3a) {
            s3aNightWriter = plugin.createWriterInstance();
        }
        if (withNighttime && withS3b) {
            s3bNightWriter = plugin.createWriterInstance();
        }
        if (withDaytime && withS3a) {
            s3aDayWriter = plugin.createWriterInstance();
        }
        if (withDaytime && withS3b) {
            s3bDayWriter = plugin.createWriterInstance();
        }
    }

    @Override
    protected void writeProductNodesImpl() throws IOException {
        final File parent;
        final String name;
        if (getOutput() instanceof File) {
            parent = ((File) getOutput()).getParentFile();
            name = ((File) getOutput()).getName();
        } else if (getOutput() instanceof String) {
            parent = new File(".");
            name = (String) getOutput();
        } else {
            throw new IllegalArgumentException("invalid input type");
        }
        // $1$2$3-C3S-L3-FRP-SLSTR-P1D-0.1deg-S3A-night-fv1.1
        if (withNighttime && withS3a) {
            s3aNightProduct = createAndWriteProduct(name,
                                                "S3A", "night",
                                                parent, s3aNightWriter);
        }
        if (withNighttime && withS3b) {
            s3bNightProduct = createAndWriteProduct(name.replace("S3A", "S3B"),
                                                "S3B", "night",
                                                parent, s3bNightWriter);
        }
        if (withDaytime && withS3a) {
            s3aDayProduct = createAndWriteProduct(name.replace("night", "day"),
                                              "S3A", "day",
                                              parent, s3aDayWriter);
        }
        if (withDaytime && withS3b) {
            s3bDayProduct = createAndWriteProduct(name.replace("night", "day")
                                                  .replace("S3A", "S3B"),
                                              "S3B", "day",
                                              parent, s3bDayWriter);
        }
    }

    private Product createAndWriteProduct(String name, String platform, String nightOrDay, File parent, ProductWriter writer) throws IOException {
        Product product = new Product(name, platform + "-" + nightOrDay, getSourceProduct().getSceneRasterWidth(), getSourceProduct().getSceneRasterHeight());
        ProductUtils.copyGeoCoding(getSourceProduct(), product);
        ProductUtils.copyTimeInformation(getSourceProduct(), product);
        final String prefix = platform.toLowerCase() + "_" + nightOrDay + "_";
        for (Band band: getSourceProduct().getBands()) {
            if (band.getName().startsWith(prefix)) {
                ProductUtils.copyBand(band.getName(), getSourceProduct(), band.getName().substring(prefix.length()), product, true);
            }
        }
        product.setStartTime(getSourceProduct().getStartTime());
        product.setEndTime(getSourceProduct().getEndTime());
        product.setPreferredTileSize(product.getSceneRasterWidth() / 12, product.getSceneRasterHeight() / 16);
        File file = new File(parent, name + ".nc");
        writer.writeProductNodes(product, file);
        return product;
    }

    @Override
    public void writeBandRasterData(Band sourceBand, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData sourceBuffer, ProgressMonitor pm) throws IOException {
        final String name = sourceBand.getName();
        final String targetName = name.substring(name.indexOf('_', 4) + 1);  // TODO oversimplification
        // s3a_night_frp
        if (name.startsWith("s3a_") && withS3a) {
            if (name.contains("_night_") && withNighttime) {
                s3aNightWriter.writeBandRasterData(s3aNightProduct.getBand(targetName),
                                                   sourceOffsetX, sourceOffsetY, sourceWidth, sourceHeight,
                                                   sourceBuffer, pm);
            } else if (name.contains("_day_") && withDaytime) {
                s3aDayWriter.writeBandRasterData(s3aDayProduct.getBand(targetName),
                                                   sourceOffsetX, sourceOffsetY, sourceWidth, sourceHeight,
                                                   sourceBuffer, pm);
            }
        } else if (name.startsWith("s3b_") && withS3b) {
            if (name.contains("_night_") && withNighttime) {
                s3bNightWriter.writeBandRasterData(s3bNightProduct.getBand(targetName),
                                                   sourceOffsetX, sourceOffsetY, sourceWidth, sourceHeight,
                                                   sourceBuffer, pm);
            } else if (name.contains("_day_") && withDaytime) {
                s3bDayWriter.writeBandRasterData(s3bDayProduct.getBand(targetName),
                                                   sourceOffsetX, sourceOffsetY, sourceWidth, sourceHeight,
                                                   sourceBuffer, pm);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (s3aNightWriter != null) { s3aNightWriter.flush(); }
        if (s3bNightWriter != null) { s3bNightWriter.flush(); }
        if (s3aDayWriter != null) { s3aDayWriter.flush(); }
        if (s3bDayWriter != null) { s3bDayWriter.flush(); }
    }

    @Override
    public void close() throws IOException {
        if (s3aNightWriter != null) { s3aNightWriter.close(); }
        if (s3bNightWriter != null) { s3bNightWriter.close(); }
        if (s3aDayWriter != null) { s3aDayWriter.close(); }
        if (s3bDayWriter != null) { s3bDayWriter.close(); }
    }

    @Override
    public void deleteOutput() throws IOException {
        if (s3aNightWriter != null) { s3aNightWriter.deleteOutput(); }
        if (s3bNightWriter != null) { s3bNightWriter.deleteOutput(); }
        if (s3aDayWriter != null) { s3aDayWriter.deleteOutput(); }
        if (s3bDayWriter != null) { s3bDayWriter.deleteOutput(); }
    }
}
