package com.bc.calvalus.processing.fire;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.image.ResolutionLevel;
import org.esa.snap.core.image.SingleBandedOpImage;
import org.esa.snap.dataio.netcdf.metadata.profiles.cf.CfNetCdf4WriterPlugIn;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.Dimension;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author thomas
 */
public class FireGridReducer extends Reducer<Text, GridCell, NullWritable, NullWritable> {

    public static final int SCENE_RASTER_WIDTH = 1440;
    public static final int SCENE_RASTER_HEIGHT = 720;
    Product result;

    @Override
    protected void reduce(Text key, Iterable<GridCell> values, Context context) throws IOException, InterruptedException {
        System.out.println("key = " + key);
        Iterator<GridCell> iterator = values.iterator();
        GridCell gridCell = iterator.next();
        float[] burnedAreaFirstHalfData = gridCell.data[0];

        Product resultFirstHalf = new Product("target", "type", SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT);
        resultFirstHalf.setProductWriter(new CfNetCdf4WriterPlugIn().createWriterInstance());
        File localFile = new File("./thomas-ba.nc");
        resultFirstHalf.setFileLocation(localFile);
        try {
            resultFirstHalf.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT, 0, 0, 0.25, 0.25, 0.0, 0.0));
        } catch (FactoryException | TransformException e) {
            throw new IOException("Unable to create geo-coding", e);
        }

        Band burnedAreaFirstHalf = resultFirstHalf.addBand("burned_area", ProductData.TYPE_FLOAT32);
        burnedAreaFirstHalf.setUnit("m^2");
        burnedAreaFirstHalf.setSourceImage(new SingleBandedOpImage(ProductData.TYPE_FLOAT32, SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT, new Dimension(40, 40), null, ResolutionLevel.MAXRES) {
            @Override
            public Raster computeTile(int tileX, int tileY) {
                WritableRaster raster = (WritableRaster) super.computeTile(tileX, tileY);
                raster.setPixels(0, 0, 40, 40, burnedAreaFirstHalfData);
                return raster;
            }
        });

        ProductIO.writeProduct(resultFirstHalf, localFile, "NetCDF4-BEAM", false);
        Path path = new Path("hdfs://calvalus/calvalus/home/thomas/thomas-ba.nc");
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        fs.delete(path, true);
        FileUtil.copy(localFile, fs, path, false, context.getConfiguration());
    }

    private int findX(Text key) {
        int x = Integer.parseInt(key.toString().substring(12));
        System.out.println("x = " + x);
        return x * 40;
    }

    private int findY(Text key) {
        int y = Integer.parseInt(key.toString().substring(8, 10));
        System.out.println("y = " + y);
        return y * 40;
    }
}
