package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.collocation.CollocateOp;
import org.esa.snap.collocation.ResamplingType;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;

import java.io.File;
import java.io.IOException;

public class LcSubsetterMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.setProperty("snap.dataio.netcdf.metadataElementLimit", "1");
        File localLcFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/lc4s2/ESACCI-LC-L4-LC10-Map-20m-P1Y-2016-v1.0.tif"), context.getConfiguration());
        Product lcProduct = ProductIO.readProduct(localLcFile);

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        File localFile = CalvalusProductIO.copyFileToLocal(inputSplit.getPath(0), context.getConfiguration());
        String fileId = localFile.getName();
        String tile = fileId.split("-")[1].substring(1);

        String targetFilename = subset(lcProduct, localFile, tile);
        FileSystem fileSystem = inputSplit.getPath(0).getFileSystem(context.getConfiguration());
        FileUtil.copy(new File(targetFilename), fileSystem, new Path("hdfs://calvalus/calvalus/projects/fire/aux/lc4s2-from-s2/", targetFilename), false, context.getConfiguration());
    }

    static String subset(Product lcProduct, File localFile, String tile) throws IOException {
        Product s2Product = ProductIO.readProduct(localFile);

        CollocateOp collocateOp = new CollocateOp();
        collocateOp.setMasterProduct(s2Product);
        collocateOp.setSlaveProduct(lcProduct);
        collocateOp.setResamplingType(ResamplingType.NEAREST_NEIGHBOUR);

        Product targetProduct = collocateOp.getTargetProduct();
        targetProduct.getBand("band_1").setName("lccs_class");
        removeBandsSafe(targetProduct);
        if (targetProduct.getSceneRasterWidth() != 5490) {
            throw new IllegalStateException("targetProduct.getSceneRasterWidth() (" + targetProduct.getSceneRasterWidth() + ") != 5400");
        }
        if (targetProduct.getSceneRasterHeight() != 5490) {
            throw new IllegalStateException("targetProduct.getSceneRasterHeight() (" + targetProduct.getSceneRasterHeight() + ") != 5400");
        }

        targetProduct.setSceneGeoCoding(null);

        String targetFilename = "lc-2010-T" + tile + ".nc";
        ProductIO.writeProduct(targetProduct, targetFilename, "NetCDF4-CF");
        s2Product.dispose();
        targetProduct.dispose();
        return targetFilename;
    }

    private static void removeBandsSafe(Product p) {
        for (String bandName : p.getBandNames()) {
            if (!bandName.equals("lccs_class")) {
                p.removeBand(p.getBand(bandName));
            }
        }
    }


}
