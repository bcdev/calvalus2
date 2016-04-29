package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.dataio.netcdf.metadata.profiles.cf.CfNetCdf4WriterPlugIn;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author thomas
 */
public class FireGridReducer extends Reducer<Text, GridCell, NullWritable, NullWritable> {

    private static final int SCENE_RASTER_WIDTH = 1440;
    private static final int SCENE_RASTER_HEIGHT = 720;
    private Product resultFirstHalf;
    private Product resultSecondHalf;
    private final List<int[]> writtenChunksFirstHalf = new ArrayList<>();
    private final List<int[]> writtenChunksSecondHalf = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        prepareTargetProducts();
    }

    private void prepareTargetProducts() throws IOException {
        resultFirstHalf = new Product("target", "type", SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT);
        resultSecondHalf = new Product("target", "type", SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT);
        resultFirstHalf.setProductWriter(new CfNetCdf4WriterPlugIn().createWriterInstance());
        resultSecondHalf.setProductWriter(new CfNetCdf4WriterPlugIn().createWriterInstance());
        resultFirstHalf.setFileLocation(new File("./thomas-ba-07.nc"));
        resultSecondHalf.setFileLocation(new File("./thomas-ba-22.nc"));
        try {
            resultFirstHalf.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT, -180, 90, 0.25, 0.25, 0.0, 0.0));
            resultSecondHalf.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT, -180, 90, 0.25, 0.25, 0.0, 0.0));
        } catch (FactoryException | TransformException e) {
            throw new IOException("Unable to create geo-coding", e);
        }

        Band burnedAreaFirstHalf = resultFirstHalf.addBand("burned_area", ProductData.TYPE_FLOAT32);
        burnedAreaFirstHalf.setUnit("m2");

        Band burnedAreaSecondHalf = resultSecondHalf.addBand("burned_area", ProductData.TYPE_FLOAT32);
        burnedAreaSecondHalf.setUnit("m2");

        Band standardErrorFirstHalf = resultFirstHalf.addBand("standard_error", ProductData.TYPE_FLOAT32);
        standardErrorFirstHalf.setUnit("m2");

        Band standardErrorSecondHalf = resultSecondHalf.addBand("standard_error", ProductData.TYPE_FLOAT32);
        standardErrorSecondHalf.setUnit("m2");

        resultFirstHalf.addBand("patch_number", ProductData.TYPE_INT32);
        resultSecondHalf.addBand("patch_number", ProductData.TYPE_INT32);

        for (int lcClass = 1; lcClass <= FireGridMapper.LC_CLASSES_COUNT; lcClass++) {
            Band band1 = resultFirstHalf.addBand(getBandNameFor(lcClass), ProductData.TYPE_FLOAT32);
            Band band2 = resultSecondHalf.addBand(getBandNameFor(lcClass), ProductData.TYPE_FLOAT32);
            band1.setUnit("m2");
            band2.setUnit("m2");
        }

        ProductWriter productWriterFH = resultFirstHalf.getProductWriter();
        ProductWriter productWriterSH = resultSecondHalf.getProductWriter();
        productWriterFH.writeProductNodes(resultFirstHalf, resultFirstHalf.getFileLocation());
        productWriterSH.writeProductNodes(resultSecondHalf, resultSecondHalf.getFileLocation());
    }

    @Override
    protected void reduce(Text key, Iterable<GridCell> values, Context context) throws IOException, InterruptedException {
        Iterator<GridCell> iterator = values.iterator();
        GridCell gridCell = iterator.next();

        float[] burnedAreaFirstHalf = gridCell.baFirstHalf;
        float[] burnedAreaSecondHalf = gridCell.baSecondHalf;

        int[] patchNumbersFirstHalf = gridCell.patchNumberFirstHalf;
        int[] patchNumbersSecondHalf = gridCell.patchNumberSecondHalf;

        float[] errorsFirstHalf = gridCell.errorsFirstHalf;
        float[] errorsSecondHalf = gridCell.errorsSecondHalf;

        List<float[]> baInLcFirstHalf = gridCell.baInLcFirstHalf;
        List<float[]> baInLcSecondHalf = gridCell.baInLcSecondHalf;

        writeData(key, burnedAreaFirstHalf, patchNumbersFirstHalf, errorsFirstHalf, baInLcFirstHalf, resultFirstHalf, writtenChunksFirstHalf);
        writeData(key, burnedAreaSecondHalf, patchNumbersSecondHalf, errorsSecondHalf, baInLcSecondHalf, resultSecondHalf, writtenChunksSecondHalf);
    }

    private void writeData(Text key, float[] burnedArea, int[] patchNumbers, float[] errors, List<float[]> baInLc, Product product, List<int[]> writtenChunks) throws IOException {
        int x = getX(key);
        int y = getY(key);
        CalvalusLogger.getLogger().info(String.format("Writing raster data: x=%d, y=%d, 40*40", x, y));
        ProductWriter productWriter = product.getProductWriter();
        productWriter.writeBandRasterData(product.getBand("burned_area"), x, y, 40, 40, new ProductData.Float(burnedArea), ProgressMonitor.NULL);
        productWriter.writeBandRasterData(product.getBand("patch_number"), x, y, 40, 40, new ProductData.Int(patchNumbers), ProgressMonitor.NULL);
        productWriter.writeBandRasterData(product.getBand("standard_error"), x, y, 40, 40, new ProductData.Float(errors), ProgressMonitor.NULL);
        for (int lcClass = 1; lcClass <= FireGridMapper.LC_CLASSES_COUNT; lcClass++) {
            productWriter.writeBandRasterData(product.getBand(getBandNameFor(lcClass)), x, y, 40, 40, new ProductData.Float(baInLc.get(lcClass - 1)), ProgressMonitor.NULL);
        }
        if (!alreadyWritten(x, y, writtenChunks)) {
            writtenChunks.add(new int[]{x, y});
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        fillBand(resultFirstHalf.getBand("burned_area"), resultFirstHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
        fillBand(resultSecondHalf.getBand("burned_area"), resultSecondHalf.getProductWriter(), writtenChunksSecondHalf, createFloatFillData());
        fillBand(resultFirstHalf.getBand("patch_number"), resultFirstHalf.getProductWriter(), writtenChunksFirstHalf, createIntFillData());
        fillBand(resultSecondHalf.getBand("patch_number"), resultSecondHalf.getProductWriter(), writtenChunksSecondHalf, createIntFillData());
        fillBand(resultFirstHalf.getBand("standard_error"), resultFirstHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
        fillBand(resultSecondHalf.getBand("standard_error"), resultSecondHalf.getProductWriter(), writtenChunksSecondHalf, createFloatFillData());
        for (int lcClass = 1; lcClass <= FireGridMapper.LC_CLASSES_COUNT; lcClass++) {
            fillBand(resultFirstHalf.getBand(getBandNameFor(lcClass)), resultFirstHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
            fillBand(resultSecondHalf.getBand(getBandNameFor(lcClass)), resultSecondHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
        }
        write(context.getConfiguration(), resultFirstHalf, "hdfs://calvalus/calvalus/home/thomas/thomas-ba-2008-06-07.nc");
        write(context.getConfiguration(), resultSecondHalf, "hdfs://calvalus/calvalus/home/thomas/thomas-ba-2008-06-22.nc");
    }

    private static String getBandNameFor(int lcClass) {
        return String.format("burned_area_in_vegetation_class%d", lcClass);
    }

    private static ProductData.Int createIntFillData() {
        int[] array = new int[40 * 40];
        Arrays.fill(array, 0);
        return new ProductData.Int(array);
    }

    private static ProductData.Float createFloatFillData() {
        float[] array = new float[40 * 40];
        Arrays.fill(array, 0.0F);
        return new ProductData.Float(array);
    }

    private void write(Configuration configuration, Product product, String pathString) throws IOException {
        product.closeIO();
        File fileLocation = product.getFileLocation();
        Path path = new Path(pathString);
        FileSystem fs = path.getFileSystem(configuration);
        fs.delete(path, true);
        FileUtil.copy(fileLocation, fs, path, false, configuration);
    }

    private static void fillBand(Band band, ProductWriter productWriter, List<int[]> writtenChunks, ProductData fillData) throws IOException {
        // the netcdf4-writer expects the band to be fully populated with data
        // also, it can write data only once
        // --> we have to fill remaining pixels here
        for (int y = 0; y < SCENE_RASTER_HEIGHT; y += 40) {
            for (int x = 0; x < SCENE_RASTER_WIDTH; x += 40) {
                if (!alreadyWritten(x, y, writtenChunks)) {
                    CalvalusLogger.getLogger().info("Filling chunk at: x=" + x + "; y=" + y);
                    productWriter.writeBandRasterData(band, x, y, 40, 40, fillData, ProgressMonitor.NULL);
                }
            }
        }
    }

    private static boolean alreadyWritten(int x, int y, List<int[]> writtenChunks) {
        for (int[] item : writtenChunks) {
            if (Arrays.equals(item, new int[]{x, y})) {
                return true;
            }
        }
        return false;
    }

    private int getX(Text key) {
        int x = Integer.parseInt(key.toString().substring(12));
        return x * 40;
    }

    private int getY(Text key) {
        int y = Integer.parseInt(key.toString().substring(9, 11));
        return y * 40;
    }
}
