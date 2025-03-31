package com.bc.calvalus.processing.fire.format.grid.syn;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author thomas
 * @author marcop
 */
public class SynGridInputFormat extends InputFormat {

    private int year;
    private int month;
    private int nextYear;
    private int nextMonth;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        YearMonth nextYearMonth = YearMonth.of(year, month).plusMonths(1);
        nextMonth = nextYearMonth.getMonthValue();
        nextYear = nextYearMonth.getYear();

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getOutputFileStatuses(hdfsInventoryService, conf);
        createSplits(fileStatuses, splits);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private FileStatus[] getOutputFileStatuses(HdfsFileSystemService hdfsInventoryService,
                                               Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        String inputRootDir = conf.get("calvalus.input.root", "hdfs://calvalus/calvalus/projects/fire/syn-ba/main-output-test-v1.3");
        String dummyRootDir = conf.get("calvalus.dummy.root", "hdfs://calvalus/calvalus/projects/fire/syn-ba/dummy-tiles");
        String inputPathPattern = String.format(inputRootDir + "/%s-%02d/.*/ba-outputs-.*-%s-%02d.tar.gz", nextYear, nextMonth, year, month);
        inputPathPattern += String.format(",%s/ba-outputs-.*-%s-%02d.tar.gz", dummyRootDir, year, month);
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPattern);
        return hdfsInventoryService.globFileStatuses(inputPatterns, conf);
    }

    private void createSplits(FileStatus[] outputFileStatuses, List<InputSplit> splits) {
        // creating splits of classification and LandCover
        for (FileStatus fileStatus : outputFileStatuses) {
            Path path = fileStatus.getPath();
            splits.add(new FileSplit(path, 0, fileStatus.getLen(), null));
        }
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NoRecordReader();
    }

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

        for (int year = 2021; year <= 2021; year++) {
            for (int month = 1; month <= 2; month++) {
                for (String tile : NORTHERN_TILES) {
                    double easting = Integer.parseInt(tile.substring(1, 3)) * 10 - 180;

                    String filenameC = String.format("SYNVIIRS%s_%s_%s_Classification.tif", year, month, tile);
                    String filenameU = String.format("SYNVIIRS%s_%s_%s_BurnProbabilityError.tif", year, month, tile);
                    String filenameF = String.format("SYNVIIRS%s_%s_%s_FractionOfObservedArea.tif", year, month, tile);

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

                    final String filePathC = targetDir + File.separator + filenameC;
                    final String filePathU = targetDir + File.separator + filenameU;
                    final String filePathF = targetDir + File.separator + filenameF;

                    ProductIO.writeProduct(productC, filePathC, "GeoTIFF");
                    ProductIO.writeProduct(productU, filePathU, "GeoTIFF");
                    ProductIO.writeProduct(productF, filePathF, "GeoTIFF");
                }
            }
        }
    }

}
