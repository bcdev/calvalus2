package com.bc.calvalus.processing.fire.format.pixel.meris;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.MerisStrategy;
import com.bc.calvalus.processing.fire.format.PixelProductArea;
import com.bc.calvalus.processing.fire.format.SensorStrategy;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.dataio.dimap.DimapProductWriterPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.Rectangle;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * @author thomas
 */
public class MerisPixelReducer extends Reducer<Text, MerisPixelCell, NullWritable, NullWritable> {

    private Product product;
    private PixelProductArea area;
    private PixelVariableType variableType;
    private String year;
    private String month;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        SensorStrategy strategy = new MerisStrategy();
        area = strategy.getArea(context.getConfiguration().get("area"));
        variableType = PixelVariableType.valueOf(context.getConfiguration().get("variableType"));
        year = context.getConfiguration().get("calvalus.year");
        month = context.getConfiguration().get("calvalus.month");
        prepareTargetProduct();
    }

    @Override
    protected void reduce(Text key, Iterable<MerisPixelCell> values, Context context) throws IOException, InterruptedException {
        Iterator<MerisPixelCell> iterator = values.iterator();
        MerisPixelCell pixelCell = iterator.next();

        int leftTargetXForTile = getLeftTargetXForTile(area, key.toString());
        int topTargetYForTile = getTopTargetYForTile(area, key.toString());

        int leftXSrc = getLeftSourceXForTile(area, key.toString());
        int topYSrc = getTopSourceYForTile(area, key.toString());

        int maxXSrc = getMaxSourceXForTile(area, key.toString());
        int maxYSrc = getMaxSourceYForTile(area, key.toString());

        if (maxXSrc == -1 || maxYSrc == -1) {
            return;
        }

        int width = maxXSrc - leftXSrc + 1;
        int height = maxYSrc - topYSrc + 1;

        Band band = product.getBand(variableType.bandName);

        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, %d*%d (tile %s) into variable '%s'", leftTargetXForTile, topTargetYForTile, width, height, key, variableType.bandName));

        short[] data = getTargetValues(leftXSrc, maxXSrc, topYSrc, maxYSrc, pixelCell.values);
        product.getProductWriter().writeBandRasterData(band, leftTargetXForTile, topTargetYForTile, width, height, new ProductData.Short(data), ProgressMonitor.NULL);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        product.closeIO();

        String filename = product.getFileLocation().getName();
        CalvalusLogger.getLogger().info("Zipping product...");
        String zipFilename = variableType.bandName + ".tar.gz";
        createTarGZ(filename, filename.substring(0, filename.indexOf(".")) + ".data", zipFilename);

        product.dispose();
        CalvalusLogger.getLogger().info(String.format("...copying product to %s...", zipFilename));
        String outputDir = context.getConfiguration().get("calvalus.output.dir");
        Path path = new Path(outputDir + "/" + year + "/" + month + "/" + "/" + area.nicename + "-to-merge/" + zipFilename);
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FileUtil.copy(new File(zipFilename), fs, path, false, context.getConfiguration());
        CalvalusLogger.getLogger().info("...done.");
    }

    private void prepareTargetProduct() throws IOException {
        int sceneRasterWidth = computeTargetWidth(area);
        int sceneRasterHeight = computeTargetHeight(area);

        String baseFilename = variableType.bandName;

        product = new Product(baseFilename, "Fire_CCI-Pixel Product", sceneRasterWidth, sceneRasterHeight);
        product.addBand(variableType.bandName, ProductData.TYPE_INT16);
        product.setSceneGeoCoding(createGeoCoding(area, sceneRasterWidth, sceneRasterHeight));
        File fileLocation = new File("./" + baseFilename + ".dim");
        product.setFileLocation(fileLocation);
        product.setProductWriter(new DimapProductWriterPlugIn().createWriterInstance());
        product.getProductWriter().writeProductNodes(product, product.getFileLocation());
    }

    private static CrsGeoCoding createGeoCoding(PixelProductArea area, int sceneRasterWidth, int sceneRasterHeight) throws IOException {
        double easting = area.left - 180;
        double northing = 90 - area.top;
        try {
            return new CrsGeoCoding(DefaultGeographicCRS.WGS84, sceneRasterWidth, sceneRasterHeight, easting, northing, 1 / 360.0, 1 / 360.0, 0.0, 0.0);
        } catch (FactoryException | TransformException e) {
            throw new IOException("Unable to create geo-coding", e);
        }
    }

    static int computeTargetWidth(PixelProductArea area) {
        return (area.right - area.left) * 360;
    }

    static int computeTargetHeight(PixelProductArea area) {
        return (area.bottom - area.top) * 360;
    }

    static int computeFullTargetWidth(PixelProductArea area) {
        boolean exactlyMatchesBorder = area.right % 10.0 == 0.0 || area.left % 10.0 == 0.0;
        return (area.right / 10 - area.left / 10 + (exactlyMatchesBorder ? 0 : 1)) * 3600;
    }

    static int computeFullTargetHeight(PixelProductArea area) {
        boolean exactlyMatchesBorder = area.top % 10.0 == 0.0 || area.bottom % 10.0 == 0.0;
        return (area.bottom / 10 - area.top / 10 + (exactlyMatchesBorder ? 0 : 1)) * 3600;
    }

    static Rectangle computeTargetRect(PixelProductArea area) {
        int x = (area.left - area.left / 10 * 10) * 360;
        int y = (area.top - area.top / 10 * 10) * 360;
        int width = (area.right - area.left) * 360;
        int height = (area.bottom - area.top) * 360;
        return new Rectangle(x, y, width, height);
    }

    static short[] getTargetValues(int leftXSrc, int maxXSrc, int topYSrc, int maxYSrc, short[] sourceValues) {
        int fullWidthSrc = (int) Math.sqrt(sourceValues.length);

        int width = maxXSrc - leftXSrc + 1;
        int height = maxYSrc - topYSrc + 1;

        short[] data = new short[(width) * (height)];
        int targetLine = 0;
        for (int y = topYSrc; y <= maxYSrc; y++) {
            int srcPos = y * fullWidthSrc + leftXSrc;
            int targetPos = targetLine * width;
            System.arraycopy(sourceValues, srcPos, data, targetPos, width);
            targetLine++;
        }
        return data;
    }

    static int getLeftTargetXForTile(PixelProductArea area, String key) {
        int tileX = Integer.parseInt(key.substring(12));
        if (tileX * 10 > area.left) {
            return (tileX * 10 - area.left) * 360;
        }
        return 0;
    }

    static int getTopTargetYForTile(PixelProductArea area, String key) {
        int tileY = Integer.parseInt(key.substring(9, 11));
        if (tileY * 10 > area.top) {
            return (tileY * 10 - area.top) * 360;
        }
        return 0;
    }

    static int getLeftSourceXForTile(PixelProductArea area, String key) {
        int tileX = Integer.parseInt(key.substring(12));
        if (tileX * 10 < area.left) {
            return (area.left - tileX * 10) * 360;
        }
        return 0;
    }

    private static int getMaxSourceXForTile(PixelProductArea area, String key) {
        int tileX = Integer.parseInt(key.substring(12));
        if ((tileX + 1) * 10 > area.right) {
            return (area.right - tileX * 10) * 360 - 1;
        }
        return 3599;
    }

    private static int getTopSourceYForTile(PixelProductArea area, String key) {
        int tileY = Integer.parseInt(key.substring(9, 11));
        if (tileY * 10 < area.top) {
            return (area.top - tileY * 10) * 360;
        }
        return 0;
    }

    private static int getMaxSourceYForTile(PixelProductArea area, String key) {
        int tileY = Integer.parseInt(key.substring(9, 11));
        if ((tileY + 1) * 10 > area.bottom) {
            return (area.bottom - tileY * 10) * 360 - 1;
        }
        return 3599;
    }

    private static void createTarGZ(String filePath, String folderPath, String outputPath) throws IOException {
        try (OutputStream fOut = new FileOutputStream(new File(outputPath));
             OutputStream bOut = new BufferedOutputStream(fOut);
             OutputStream gzOut = new GzipCompressorOutputStream(bOut);
             TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {
            addFileToTarGz(tOut, filePath, "");
            addFileToTarGz(tOut, folderPath, "");
        }
    }

    private static void addFileToTarGz(TarArchiveOutputStream tOut, String path, String base)
            throws IOException {
        File f = new File(path);
        String entryName = base + f.getName();
        TarArchiveEntry tarEntry = new TarArchiveEntry(f, entryName);
        tOut.putArchiveEntry(tarEntry);

        if (f.isFile()) {
            IOUtils.copy(new FileInputStream(f), tOut);
            tOut.closeArchiveEntry();
        } else {
            tOut.closeArchiveEntry();
            File[] children = f.listFiles();
            if (children != null) {
                for (File child : children) {
                    addFileToTarGz(tOut, child.getAbsolutePath(), entryName + "/");
                }
            }
        }
    }
}
