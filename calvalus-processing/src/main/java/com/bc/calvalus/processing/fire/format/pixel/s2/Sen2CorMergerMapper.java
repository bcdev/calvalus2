package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.BandMathsOp;
import org.esa.snap.core.util.ProductUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Sen2CorMergerMapper extends Mapper {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String tile = ((CombineFileSplit) context.getInputSplit()).getPath(0).getName();

        CalvalusLogger.getLogger().info("Searching for files containing tile " + tile + "...");
        for (String month : new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",}) {
            String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/s2-ba/T" + tile + "/BA-T" + tile + "-2016" + month + "01T000000.nc";
            FileStatus[] fileStatuses = getFileStatuses(inputPathPattern, conf);
            if (fileStatuses.length > 0) {
                continue;
            }
            String inputPathPattern2 = "hdfs://calvalus/calvalus/projects/fire/s2-ba/T" + tile + "/BA-T" + tile + "-2016" + month + "01T000001.nc";
            FileStatus[] fileStatuses2 = getFileStatuses(inputPathPattern2, conf);
            if (fileStatuses2.length > 0) {
                continue;
            }
            CalvalusLogger.getLogger().info("Filling data for tile " + tile + " in month " + month);

            String sen2corInputPathPattern = "hdfs://calvalus/calvalus/projects/fire/s2-pre/2016/" + month + "/.*/.*" + tile + ".*tif";
            FileStatus[] sen2CorFileStatuses = getFileStatuses(sen2corInputPathPattern, conf);

            List<Product> products = new ArrayList<>();

            Arrays.stream(sen2CorFileStatuses).forEach(
                    fileStatus -> {
                        try {
                            File file = CalvalusProductIO.copyFileToLocal(fileStatus.getPath(), conf);
                            products.add(ProductIO.readProduct(file));
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
            );

            if (products.isEmpty()) {
                writeNotObservedProduct(conf, tile, month);
                continue;
            }

            BandMathsOp bandMathsOp = new BandMathsOp();
            bandMathsOp.setSourceProducts(products.toArray(new Product[0]));
            BandMathsOp.BandDescriptor jdBandDescriptor = new BandMathsOp.BandDescriptor();
            jdBandDescriptor.name = "JD";
            jdBandDescriptor.type = "float32";
            String clause =
                    "not( v == 0" +
                            " or v == 8" +
                            " or v == 9" +
                            " or v == 10 ) ? 0 : 998";

            for (int i = 1; i < products.size(); i++) {
                clause = "not ($sourceProduct.{i}.v == 0" +
                        " or $sourceProduct.{i}.v == 8" +
                        " or $sourceProduct.{i}.v == 9" +
                        " or $sourceProduct.{i}.v == 10) ? 0 : (" + clause + ")";
                clause = clause.replace("{i}", i + "");
            }

            jdBandDescriptor.expression = clause.replace("v", "quality_scene_classification");

            BandMathsOp.BandDescriptor clBandDescriptor = new BandMathsOp.BandDescriptor();
            clBandDescriptor.name = "CL";
            clBandDescriptor.type = "float32";
            clBandDescriptor.expression = "0";

            bandMathsOp.setTargetBandDescriptors(jdBandDescriptor, clBandDescriptor);

            export(conf, tile, month, bandMathsOp.getTargetProduct());
        }
    }

    private static void export(Configuration conf, String tile, String month, Product targetProduct) throws IOException {
        String targetFile = String.format("BA-T%s-2016%s01T000000.nc", tile, month);
        ProductIO.writeProduct(targetProduct, targetFile, "NetCDF4-CF");
        Path targetPath = new Path("hdfs://calvalus/calvalus/projects/fire/s2-ba/T" + tile + "/" + targetFile);
        FileUtil.copy(new File(targetFile), FileSystem.get(conf), targetPath, false, conf);
    }

    private void writeNotObservedProduct(Configuration conf, String tile, String month) throws IOException {
        CalvalusLogger.getLogger().info("No Sen2Cor data for tile " + tile + " in month " + month + " found, writing non-observed BA.");

        Product notObservedProduct = new Product("BA-T" + tile + "-2016" + month + "01T000001", "Non-observed-BA", 5490, 5490);
        notObservedProduct.addBand("JD", "998");
        notObservedProduct.addBand("CL", "0");

        FileStatus[] fileStatuses = new FileStatus[0];
        for (String searchMonth : new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",}) {
            String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/s2-pre/2016/" + searchMonth + "/.*/.*T" + tile + ".*tif";
            fileStatuses = getFileStatuses(inputPathPattern, conf);
            if (fileStatuses.length > 0) {
                break;
            }
        }
        if (fileStatuses.length == 0) {
            CalvalusLogger.getLogger().warning("No sen2cor image for tile '" + tile + "' found.");
            return;
        }

        File file = CalvalusProductIO.copyFileToLocal(fileStatuses[0].getPath(), conf);
        Product sampleProduct = ProductIO.readProduct(file);

        ProductUtils.copyGeoCoding(sampleProduct, notObservedProduct);

        String targetFile = String.format("BA-T%s-2016%s01T000001.nc", tile, month);
        ProductIO.writeProduct(notObservedProduct, targetFile, "NetCDF4-CF");
        Path targetPath = new Path("hdfs://calvalus/calvalus/projects/fire/s2-ba/T" + tile + "/" + targetFile);
        FileUtil.copy(new File(targetFile), FileSystem.get(conf), targetPath, false, conf);
    }

    public static void main(String[] args) throws IOException {
        List<Product> products = new ArrayList<>();

        Files.list(Paths.get("D:\\workspace\\fire-cci\\temp\\pixel-product"))
                .filter(p -> p.getFileName().toString().endsWith("tif"))
                .forEach(p -> {
                            try {
                                products.add(ProductIO.readProduct(p.toFile()));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                );
        BandMathsOp bandMathsOp = new BandMathsOp();
        bandMathsOp.setSourceProducts(products.toArray(new Product[0]));
        BandMathsOp.BandDescriptor bandDescriptor = new BandMathsOp.BandDescriptor();
        bandDescriptor.name = "JD";
        bandDescriptor.type = "int8";
        String clause =
                "not( v == 0" +
                        " or v == 8" +
                        " or v == 9" +
                        " or v == 10 ) ? 0 : 997";

        for (int i = 1; i < products.size(); i++) {
            clause = "not ($sourceProduct.{i}.v == 0" +
                    " or $sourceProduct.{i}.v == 8" +
                    " or $sourceProduct.{i}.v == 9" +
                    " or $sourceProduct.{i}.v == 10) ? 0 : (" + clause + ")";
            clause = clause.replace("{i}", i + "");
        }

        bandDescriptor.expression = clause.replace("v", "quality_scene_classification");
        System.out.println(bandDescriptor.expression);
        System.out.println(clause);

        bandMathsOp.setTargetBandDescriptors(bandDescriptor);
        ProductIO.writeProduct(bandMathsOp.getTargetProduct(), "D:\\workspace\\fire-cci\\temp\\pixel-product\\merged.nc", "NetCDF4-CF");

    }

    private FileStatus[] getFileStatuses(String inputPathPatterns,
                                         Configuration conf) throws IOException {

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return hdfsInventoryService.globFileStatuses(inputPatterns, conf);
    }

}
