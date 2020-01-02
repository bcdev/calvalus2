package com.bc.calvalus.processing.fire.format.grid.olci;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author thomas
 * @author marcop
 */
public class OlciGridInputFormat extends InputFormat {

    private int year;
    private int month;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getOutputFileStatuses(hdfsInventoryService, conf);
        createSplits(fileStatuses, splits, conf);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void createSplits(FileStatus[] outputFileStatuses, List<InputSplit> splits, Configuration conf) throws IOException {
        // creating splits of classification, fraction of observed area, LandCover
        for (FileStatus fileStatus : outputFileStatuses) {
            List<Path> filePaths = new ArrayList<>();
            List<Long> fileLengths = new ArrayList<>();
            Path path = fileStatus.getPath();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());

            FileStatus foaPath = getFoaFileStatus(path, path.getFileSystem(conf));
            filePaths.add(foaPath.getPath());
            fileLengths.add(foaPath.getLen());

            String lcMapPath = conf.get("calvalus.aux.lcMapPath");
            Path lcPath0 = new Path(lcMapPath);
            FileStatus lcPath = getLcFileStatus(lcPath0, lcPath0.getFileSystem(conf));
            filePaths.add(lcPath.getPath());
            fileLengths.add(lcPath.getLen());

            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
        }
    }

    private FileStatus getFoaFileStatus(Path path, FileSystem fileSystem) throws IOException {
        String outputPath = path.toString(); // hdfs://calvalus/calvalus/projects/c3s/olci-ba-v1.7/h00v00/2000-01/ba-outputs-h00v00-2000-01.tar.gz
        String foaFilePath = outputPath.replace("outputs", "composites");
        return fileSystem.getFileStatus(new Path(foaFilePath));
    }

    private static FileStatus getLcFileStatus(Path path, FileSystem fileSystem) throws IOException {
//        String outputPath = path.toString(); // hdfs://calvalus/calvalus/projects/c3s/olci-ba-v1.7/h00v00/2000-01/ba-outputs-h00v00-2000-01.tar.gz
//        int tileIndex = outputPath.indexOf("ba-outputs-") + "ba-outputs-".length();
//        String tile = outputPath.substring(tileIndex, tileIndex + 6);
//        String yearBefore = String.valueOf(Integer.parseInt(outputPath.substring(tileIndex+7, tileIndex+7+4))-1);
//        String lcFilePath = String.format("hdfs://calvalus/calvalus/projects/c3s/aux/splitted-lc-data/%s/lc-%s-%s.nc", yearBefore, yearBefore, tile);
//        String lcFilePath = String.format("/mnt/auxiliary/auxiliary/c3s/lc/...", yearBefore);
//        return fileSystem.getFileStatus(new Path(lcFilePath));
        return fileSystem.getFileStatus(path);
    }

    private FileStatus[] getOutputFileStatuses(HdfsFileSystemService hdfsInventoryService,
                                               Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        String inputRootDir = conf.get("calvalus.input.root", "hdfs://calvalus/calvalus/projects/c3s/olci-ba-v1.7.5");
        String inputPathPattern = String.format(inputRootDir + "/.*/%s-%02d/ba-outputs-.*-%s-%02d.tar.gz", year, month, year, month);
        System.out.println(inputPathPattern);
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPattern);
        return hdfsInventoryService.globFileStatuses(inputPatterns, conf);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NoRecordReader();
    }

    static String[] OLCI_TILES = {
            "h02v07", "h07v07", "h08v07", "h09v07", "h02v06", "h06v06", "h07v06", "h08v06", "h09v06", "h10v06", "h05v05",
            "h06v05", "h07v05", "h08v05", "h09v05", "h10v05", "h05v04", "h06v04", "h07v04", "h08v04", "h09v04", "h10v04",
            "h11v04", "h12v04", "h00v03", "h01v03", "h02v03", "h04v03", "h05v03", "h06v03", "h07v03", "h08v03", "h09v03",
            "h10v03", "h11v03", "h12v03", "h01v02", "h02v02", "h03v02", "h04v02", "h05v02", "h06v02", "h07v02", "h08v02",
            "h09v02", "h10v02", "h11v02", "h01v01", "h02v01", "h03v01", "h05v01", "h06v01", "h07v01", "h08v01", "h09v01",
            "h10v01", "h10v14", "h11v14", "h12v14", "h10v13", "h11v13", "h10v12", "h11v12", "h12v12", "h10v11", "h11v11",
            "h12v11", "h13v11", "h10v10", "h11v10", "h12v10", "h13v10", "h14v10", "h08v09", "h09v09", "h10v09", "h11v09",
            "h12v09", "h13v09", "h14v09", "h08v08", "h09v08", "h10v08", "h11v08", "h12v08", "h10v07", "h11v07", "h17v05",
            "h18v05", "h19v05", "h20v05", "h21v05", "h17v04", "h18v04", "h19v04", "h20v04", "h21v04", "h17v03", "h18v03",
            "h19v03", "h20v03", "h21v03", "h12v02", "h13v02", "h14v02", "h15v02", "h16v02", "h18v02", "h19v02", "h20v02",
            "h21v02", "h11v01", "h12v01", "h13v01", "h14v01", "h15v01", "h16v01", "h19v01", "h20v01", "h25v08", "h26v08",
            "h25v07", "h26v07", "h27v07", "h28v07", "h29v07", "h22v06", "h23v06", "h24v06", "h25v06", "h26v06", "h27v06",
            "h28v06", "h29v06", "h30v06", "h22v05", "h23v05", "h24v05", "h25v05", "h26v05", "h27v05", "h28v05", "h29v05",
            "h30v05", "h31v05", "h32v05", "h22v04", "h23v04", "h24v04", "h25v04", "h26v04", "h27v04", "h28v04", "h29v04",
            "h30v04", "h31v04", "h32v04", "h22v03", "h23v03", "h24v03", "h25v03", "h26v03", "h27v03", "h28v03", "h29v03",
            "h30v03", "h31v03", "h32v03", "h33v03", "h34v03", "h00v02", "h22v02", "h23v02", "h24v02", "h25v02", "h26v02",
            "h27v02", "h28v02", "h29v02", "h30v02", "h31v02", "h32v02", "h33v02", "h34v02", "h35v02", "h00v01", "h23v01",
            "h24v01", "h25v01", "h26v01", "h27v01", "h28v01", "h29v01", "h30v01", "h31v01", "h32v01", "h33v01", "h35v01",
            "h19v12", "h20v12", "h19v11", "h20v11", "h21v11", "h22v11", "h23v11", "h19v10", "h20v10", "h21v10", "h22v10",
            "h23v10", "h18v09", "h19v09", "h20v09", "h21v09", "h22v09", "h16v08", "h17v08", "h18v08", "h19v08", "h20v08",
            "h21v08", "h22v08", "h23v08", "h15v07", "h16v07", "h17v07", "h18v07", "h19v07", "h20v07", "h21v07", "h22v07",
            "h23v07", "h16v06", "h17v06", "h18v06", "h19v06", "h20v06", "h21v06", "h32v13", "h34v13", "h35v13", "h29v12",
            "h30v12", "h31v12", "h32v12", "h33v12", "h35v12", "h29v11", "h30v11", "h31v11", "h32v11", "h33v11", "h34v11",
            "h30v10", "h31v10", "h32v10", "h33v10", "h34v10", "h35v10", "h27v09", "h28v09", "h29v09", "h30v09", "h31v09",
            "h32v09", "h33v09", "h34v09", "h27v08", "h28v08", "h29v08", "h30v08", "h30v07"
    };

    static String[] NORTHERN_TILES = new String[]{
            "h00v00",
            "h01v00",
            "h02v00",
            "h03v00",
            "h04v00",
            "h05v00",
            "h06v00",
            "h07v00",
            "h08v00",
            "h09v00",
            "h10v00",
            "h11v00",
            "h12v00",
            "h13v00",
            "h14v00",
            "h15v00",
            "h16v00",
            "h17v00",
            "h18v00",
            "h19v00",
            "h20v00",
            "h21v00",
            "h22v00",
            "h23v00",
            "h24v00",
            "h25v00",
            "h26v00",
            "h27v00",
            "h28v00",
            "h29v00",
            "h30v00",
            "h31v00",
            "h32v00",
            "h33v00",
            "h34v00",
            "h35v00"
    };

    public static void main(String[] args) throws FactoryException, TransformException, IOException {
        createDummyTiles(args[0]);
    }

    public static void createDummyTiles(String targetDir) throws IOException, FactoryException, TransformException {

        for (int year = 2017; year <= 2018; year++) {
            for (int month = 1; month <= 12; month++) {
                for (String tile : NORTHERN_TILES) {
                    double easting = Integer.parseInt(tile.substring(1, 3)) * 10 - 180;

                    String filenameC = String.format("OLCIMODIS%s_%s_%s_Classification.tif", year, month, tile);
                    String filenameU = String.format("OLCIMODIS%s_%s_%s_Uncertainty.tif", year, month, tile);
                    String filenameF = String.format("OLCIMODIS%s_%s_%s_FractionOfObservedArea.tif", year, month, tile);

                    Product productC = new Product("dummy", "dummy", 3600, 3600);
                    Band cBand = productC.addBand("band_1", ProductData.TYPE_INT16);
                    cBand.setRasterData(new ProductData.Short(new short[3600 * 3600]));
                    productC.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 3600, 3600, easting, 90, 1.0 / 360.0, 1.0 / 360.0, 0.0, 0.0));

                    Product productU = new Product("dummy", "dummy", 3600, 3600);
                    productU.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 3600, 3600, easting, 90, 1.0 / 360.0, 1.0 / 360.0, 0.0, 0.0));
                    Band uBand = productU.addBand("band_1", ProductData.TYPE_UINT8);
                    uBand.setRasterData(new ProductData.UByte(new byte[3600 * 3600]));

                    Product productF = new Product("dummy", "dummy", 3600, 3600);
                    productF.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 3600, 3600, easting, 90, 1.0 / 360.0, 1.0 / 360.0, 0.0, 0.0));
                    Band fBand = productF.addBand("band_1", ProductData.TYPE_UINT8);
                    byte[] foaValues = new byte[3600 * 3600];
                    Arrays.fill(foaValues, (byte) 3);
                    fBand.setRasterData(new ProductData.UByte(foaValues));

                    ProductIO.writeProduct(productC, targetDir + File.separator + filenameC, "GeoTIFF");
                    ProductIO.writeProduct(productU, targetDir + File.separator + filenameU, "GeoTIFF");
                    ProductIO.writeProduct(productF, targetDir + File.separator + filenameF, "GeoTIFF");
                }
            }
        }
    }

}
