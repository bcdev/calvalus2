package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.fire.format.pixel.GlobalPixelProductAreaProvider.GlobalPixelProductArea;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.image.ColoredBandImageMultiLevelSource;
import org.junit.Ignore;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper.TILE_SIZE;
import static org.junit.Assert.assertEquals;

public class PixelFinaliseMapperTest {

    @Test
    public void makeTimeSeries() throws Exception {
        String basePath = "D:\\workspace\\fire-cci\\timeseries-4-emilio";
        Files.list(Paths.get(basePath))
                .filter(path -> path.toString().contains(".tif"))
                .forEach(path -> {
                    try {
                        Product product = ProductIO.readProduct(path.toFile());
                        File targetFile = new File(basePath + "\\" + product.getName() + ".png");
                        if (targetFile.exists()) {
                            return;
                        }
                        System.out.println("Creating RGB at " + targetFile.getName());
//                        SubsetOp subsetOp = new SubsetOp();
//                        subsetOp.setRegion(new Rectangle(3800, 4269, 1000, 1000));
//                        subsetOp.setParameterDefaultValues();
//                        subsetOp.setSourceProduct(product);
//                        subsetOp.setCopyMetadata(true);
//                        Product subset = subsetOp.getTargetProduct();

//                        ProductIO.writeProduct(subset, "c:\\ssd\\miau.nc", "NetCDF4-CF");
//                        System.exit(0);

//                        BandMathsOp bandMathsOp = new BandMathsOp();
//                        bandMathsOp.setSourceProduct(product);
//                        BandMathsOp.BandDescriptor bandDescriptor = new BandMathsOp.BandDescriptor();
//                        bandDescriptor.expression = "JD == 999 ? -1 : JD == 998 ? -1 : JD == 997 ? -2 : JD";
//                        bandDescriptor.name = "JD";
//                        bandDescriptor.type = ProductData.getTypeString(product.getBand("JD").getDataType());
//                        bandMathsOp.setTargetBandDescriptors(bandDescriptor);
//                        bandMathsOp.setParameterDefaultValues();

                        Band b12 = product.getBand("B12");
                        Band b11 = product.getBand("B11");
                        Band b4 = product.getBand("B4");
                        Band[] rasters = new Band[]{
                                b12, b11, b4
                        };
                        ColoredBandImageMultiLevelSource source = ColoredBandImageMultiLevelSource.create(rasters, ProgressMonitor.NULL);
                        RenderedImage image1 = source.createImage(3);

//                        Quicklooks.QLConfig qlConfig = new Quicklooks.QLConfig();
//                        qlConfig.setLegendEnabled(true);
//                        qlConfig.setRGBAExpressions(new String[]{
//                                "B12", "B11", "B4", ""
//                        });
//                        RenderedImage image = QuicklookGenerator.createImage(new MyTaskAttemptContext(), product, qlConfig);
                        ImageIO.write(image1, "PNG", targetFile);
//                        ImageIO.write(image, "PNG", targetFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    @Test
    public void name() throws Exception {
        Files.list(Paths.get("c:\\ssd\\modis-analysis")).filter(p -> p.getFileName().toString().contains("CCI_LC")).forEach(
                (Path p) -> {
                    Product inputProduct;
                    try {
                        inputProduct = ProductIO.readProduct(p.toFile());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println("Handling product " + inputProduct.getName());
                    inputProduct.getBand("band_1").setName("lccs_class");
                    String name = inputProduct.getName();
                    String newName = null;
                    String year = null;
                    if (name.contains("2000")) {
                        year = "2000";
                    } else if (name.contains("2005")) {
                        year = "2005";
                    } else if (name.contains("2010")) {
                        year = "2010";
                    }
                    if (name.contains("SouthAm")) {
                        newName = "south_america-" + year;
                    } else if (name.contains("NorthAm")) {
                        newName = "north_america-" + year;
                    } else if (name.contains("Europe")) {
                        newName = "europe-" + year;
                    } else if (name.contains("Australia")) {
                        newName = "australia-" + year;
                    } else if (name.contains("Asia")) {
                        newName = "asia-" + year;
                    }
                    inputProduct.setName(newName);
                    try {
                        ProductIO.writeProduct(inputProduct, "c:\\ssd\\" + newName + ".nc", "NetCDF4-CF");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Ignore
    @Test
    public void testRemap() throws Exception {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        Product lcProduct = ProductIO.readProduct("c:\\ssd\\modis-analysis\\africa-2000.nc");
        lcProduct.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        final File localL3 = new File("C:\\ssd\\modis-analysis\\subset_0_of_L3_2006-03-01_2006-03-31.nc");
        Product product = getPixelFinaliseMapper().remap(ProductIO.readProduct(localL3), "c:\\ssd\\test.nc", "4", lcProduct);

        ProductIO.writeProduct(product, "C:\\ssd\\test4.nc", "NetCDF4-CF");
    }

    private static PixelFinaliseMapper getPixelFinaliseMapper() {
        return new PixelFinaliseMapper() {
            @Override
            protected ClScaler getClScaler() {
                return cl -> cl;
            }

            @Override
            protected Product collocateWithSource(Product lcProduct, Product source) {
                return lcProduct;
            }
        };
    }

    @Test
    public void testFindNeighbourValue_1() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[56] = 20; // right next to the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        PixelFinaliseMapper.NeighbourResult neighbourResult = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true);

        int neighbourValue = (int) neighbourResult.neighbourValue;
        int newPixelIndex = neighbourResult.newPixelIndex;
        assertEquals(20, neighbourValue);
        assertEquals(56, newPixelIndex);
    }

    @Test
    public void testFindNeighbourValue_with_precedence_1() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[44] = 10; // upper left of the source value
        sourceJdArray[56] = 20; // right next to the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_with_precedence_2() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[45] = 10; // upper center of the source value
        sourceJdArray[46] = 20; // upper right of the source value
        sourceJdArray[54] = 30; // center left of the source value
        sourceJdArray[56] = 40; // center right of the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge() throws Exception {
        int pixelIndex = 0;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[1] = 10; // center right of the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge_2() throws Exception {
        int pixelIndex = 53248;
        int wrongNeighbourPixelIndex = 52991;

        float[] sourceJdArray = new float[256 * 256];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[wrongNeighbourPixelIndex] = 10;

        int[] lcArray = new int[256 * 256];
        Arrays.fill(lcArray, 210); // all non-burnable
        lcArray[wrongNeighbourPixelIndex] = 180; // burnable


        final Rectangle destRect = new Rectangle(19712, 25344, 256, 256);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(-2, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge2() throws Exception {
        int pixelIndex = 99;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[88] = 10; // upper left of the source value
        sourceJdArray[89] = 20; // upper center of the source value
        sourceJdArray[98] = 30; // center left of the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_exc() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[pixelIndex] = 23;

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(23, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_non_burnable() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[pixelIndex + 1] = 23;
        sourceJdArray[pixelIndex + 10] = 24; // below start pixel

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        lcArray[pixelIndex + 1] = 25; // not burnable
        lcArray[pixelIndex + 10] = 180; // burnable


        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(24, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_all_non_burnable() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[pixelIndex + 1] = 23;
        sourceJdArray[pixelIndex + 10] = 24; // below start pixel

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        lcArray[pixelIndex + 1] = 25; // not burnable
        lcArray[pixelIndex + 10] = 25; // not burnable


        final Rectangle destRect = new Rectangle(10, 10);
        PixelFinaliseMapper.NeighbourResult neighbourResult = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true);

        int neighbourValue = (int) neighbourResult.neighbourValue;
        int newPixelIndex = neighbourResult.newPixelIndex;
        assertEquals(-2, neighbourValue);
        assertEquals(pixelIndex, newPixelIndex);
    }

    @Test
    public void testFindNeighbourValue_all_non_burnable_cl() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[pixelIndex + 1] = 23;
        sourceJdArray[pixelIndex + 10] = 24; // below start pixel

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        lcArray[pixelIndex + 1] = 25; // not burnable
        lcArray[pixelIndex + 10] = 25; // not burnable


        final Rectangle destRect = new Rectangle(10, 10);
        PixelFinaliseMapper.NeighbourResult neighbourResult = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, false);

        int neighbourValue = (int) neighbourResult.neighbourValue;
        int newPixelIndex = neighbourResult.newPixelIndex;
        assertEquals(0, neighbourValue);
        assertEquals(pixelIndex, newPixelIndex);
    }


    @Ignore
    @Test
    public void testCreateMetadata() throws Exception {
//        for (PixelProductArea area : new S2Strategy().getAllAreas()) {
        for (GlobalPixelProductArea area : GlobalPixelProductArea.values()) {
//            for (MonthYear monthYear : s2MonthYears()) {
            // "h37v17;185;90;190;95"
            for (int year = 2000; year <= 2000; year++) {
                for (int month = 12; month <= 12; month++) {
                    String monthPad = month < 10 ? "0" + month : "" + month;
                    String areaString = area.index + ";" + area.nicename + ";" + area.left + ";" + area.top + ";" + area.right + ";" + area.bottom;
                    String baseFilename = PixelFinaliseMapper.createBaseFilename(year + "", monthPad, "v5.0", areaString);
                    String metadata = PixelFinaliseMapper.createMetadata(year + "", monthPad, "v5.0", areaString);
                    String targetDir = "c:\\ssd\\" + area.name();
                    if (Files.notExists(Paths.get(targetDir))) {
                        Files.createDirectory(Paths.get(targetDir));
                    }
                    try (FileWriter fw = new FileWriter(targetDir + "\\" + baseFilename + ".xml")) {
                        fw.write(metadata);
                    }
                }
            }
        }
    }


    private static MonthYear[] s2MonthYears() {
        ArrayList<MonthYear> monthYears = new ArrayList<>();
        for (String m : new String[]{"06", "07", "08", "09", "10", "11", "12"}) {
            monthYears.add(new MonthYear(m, "2015"));
        }
        for (String m : new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09"}) {
            monthYears.add(new MonthYear(m, "2016"));
        }
        return monthYears.toArray(new MonthYear[0]);
    }

    private static class MonthYear {
        String month;
        String year;

        public MonthYear(String month, String year) {
            this.month = month;
            this.year = year;
        }
    }


    private static class MyTaskAttemptContext implements TaskAttemptContext {
        @Override
        public TaskAttemptID getTaskAttemptID() {
            return null;
        }

        @Override
        public void setStatus(String msg) {

        }

        @Override
        public String getStatus() {
            return null;
        }

        @Override
        public float getProgress() {
            return 0;
        }

        @Override
        public Counter getCounter(Enum<?> counterName) {
            return null;
        }

        @Override
        public Counter getCounter(String groupName, String counterName) {
            return null;
        }

        @Override
        public Configuration getConfiguration() {
            Configuration entries = new Configuration();
            entries.set(JobConfigNames.CALVALUS_PROJECT_NAME, "purzel");
            return entries;
        }

        @Override
        public Credentials getCredentials() {
            return null;
        }

        @Override
        public JobID getJobID() {
            return null;
        }

        @Override
        public int getNumReduceTasks() {
            return 0;
        }

        @Override
        public org.apache.hadoop.fs.Path getWorkingDirectory() throws IOException {
            return null;
        }

        @Override
        public Class<?> getOutputKeyClass() {
            return null;
        }

        @Override
        public Class<?> getOutputValueClass() {
            return null;
        }

        @Override
        public Class<?> getMapOutputKeyClass() {
            return null;
        }

        @Override
        public Class<?> getMapOutputValueClass() {
            return null;
        }

        @Override
        public String getJobName() {
            return null;
        }

        @Override
        public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
            return null;
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
            return null;
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
            return null;
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
            return null;
        }

        @Override
        public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
            return null;
        }

        @Override
        public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
            return null;
        }

        @Override
        public RawComparator<?> getSortComparator() {
            return null;
        }

        @Override
        public String getJar() {
            return null;
        }

        @Override
        public RawComparator<?> getCombinerKeyGroupingComparator() {
            return null;
        }

        @Override
        public RawComparator<?> getGroupingComparator() {
            return null;
        }

        @Override
        public boolean getJobSetupCleanupNeeded() {
            return false;
        }

        @Override
        public boolean getTaskCleanupNeeded() {
            return false;
        }

        @Override
        public boolean getProfileEnabled() {
            return false;
        }

        @Override
        public String getProfileParams() {
            return null;
        }

        @Override
        public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
            return null;
        }

        @Override
        public String getUser() {
            return null;
        }

        @Override
        public boolean getSymlink() {
            return false;
        }

        @Override
        public org.apache.hadoop.fs.Path[] getArchiveClassPaths() {
            return new org.apache.hadoop.fs.Path[0];
        }

        @Override
        public URI[] getCacheArchives() throws IOException {
            return new URI[0];
        }

        @Override
        public URI[] getCacheFiles() throws IOException {
            return new URI[0];
        }

        @Override
        public org.apache.hadoop.fs.Path[] getLocalCacheArchives() throws IOException {
            return new org.apache.hadoop.fs.Path[0];
        }

        @Override
        public org.apache.hadoop.fs.Path[] getLocalCacheFiles() throws IOException {
            return new org.apache.hadoop.fs.Path[0];
        }

        @Override
        public org.apache.hadoop.fs.Path[] getFileClassPaths() {
            return new org.apache.hadoop.fs.Path[0];
        }

        @Override
        public String[] getArchiveTimestamps() {
            return new String[0];
        }

        @Override
        public String[] getFileTimestamps() {
            return new String[0];
        }

        @Override
        public int getMaxMapAttempts() {
            return 0;
        }

        @Override
        public int getMaxReduceAttempts() {
            return 0;
        }

        @Override
        public void progress() {

        }
    }
}