package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemappingS2;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductNode;

import java.io.File;
import java.io.IOException;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Runs the fire S2 formatting grid mapper.
 *
 * @author thomas
 */
public class S2GridMapper extends AbstractGridMapper {

    private static final int GRID_CELLS_PER_DEGREE = 4;
    private static final int NUM_GRID_CELLS = 1;
    private String oneDegTile;

    protected S2GridMapper() {
        super(NUM_GRID_CELLS * GRID_CELLS_PER_DEGREE, NUM_GRID_CELLS * GRID_CELLS_PER_DEGREE);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        String m;
        if (month < 10) {
            m = "0" + month;
        } else {
            m = "" + month;
        }

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] fakePaths = inputSplit.getPaths();
        if (fakePaths.length == 1) {
            LOG.info("No input product for tile " + fakePaths[0].getName() + ", terminate map.");
            return;
        }
        LOG.info("paths=" + Arrays.toString(fakePaths));

        List<File> paths = new ArrayList<>();
        for (int i = 0; i < fakePaths.length - 1; i++) {
            Path path = fakePaths[i];
            String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/s2-pixel/" + path.getName() + "-2016-" + m + "-Fire-Pixel-Formatting.*/.*tif";
            FileStatus[] fileStatuses = getFileStatuses(inputPathPattern, context.getConfiguration());
            for (FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.getPath().getName().contains("LC")) {
                    File file = CalvalusProductIO.copyFileToLocal(fileStatus.getPath(), context.getConfiguration());
                    paths.add(file);
                }
            }
        }

        File lcFile;
        File tmpLc = new File("/tmp/ESACCI-LC-L4-LC10-Map-20m-P1Y-2016-v1.0.tif");
        if (tmpLc.exists() && tmpLc.length() > 0) {
            LOG.info("Re-using prestored LC file.");
            lcFile = tmpLc;
        } else {
            lcFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/lc4s2/ESACCI-LC-L4-LC10-Map-20m-P1Y-2016-v1.0.tif"), context.getConfiguration());
            try {
                FileUtils.copyFile(lcFile, tmpLc);
            } catch (IOException e) {
                LOG.info("Unable to copy file: " + e.getMessage());
                if (tmpLc.exists()) {
                    FileUtils.forceDelete(tmpLc);
                }
            }
        }
        Product lcProduct = ProductIO.readProduct(lcFile);
        LOG.info(tmpLc.length() + "");

        oneDegTile = fakePaths[fakePaths.length - 1].getName();

        List<Product> sourceProducts = new ArrayList<>();
        List<Product> lcProducts = new ArrayList<>();
        List<Product> clProducts = new ArrayList<>();
        ProgressSplitProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        pm.beginTask("Copying data and computing grid cells...", targetRasterWidth * targetRasterHeight);
        for (File productFile : paths) {
            Product product = ProductIO.readProduct(productFile);
            if (product == null) {
                throw new IllegalStateException("Product " + productFile + " is broken.");
            }
            if (productFile.getName().contains("JD")) {
                sourceProducts.add(product);
            } else if (productFile.getName().contains("CL")) {
                clProducts.add(product);
            } else {
                throw new IllegalStateException("Unknown product: " + productFile.getName());
            }

            sourceProducts.sort(Comparator.comparing(ProductNode::getName));
            clProducts.sort(Comparator.comparing(ProductNode::getName));
        }

        lcProducts.add(lcProduct);

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        S2FireGridDataSource dataSource = new S2FireGridDataSource(oneDegTile, sourceProducts.toArray(new Product[0]), clProducts.toArray(new Product[0]), lcProducts.toArray(new Product[0]));
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        setDataSource(dataSource);
        GridCells gridCells = computeGridCells(year, month, context);

        context.write(new Text(String.format("%d-%02d-%s", year, month, oneDegTile)), gridCells);
    }

    @Override
    protected void validate(float burnableFraction, List<double[]> baInLc, int targetGridCellIndex, double area) {
        double lcAreaSum = 0.0F;
        for (double[] baValues : baInLc) {
            lcAreaSum += baValues[targetGridCellIndex];
        }
        float lcAreaSumFraction = getFraction(lcAreaSum, area);
        if (lcAreaSumFraction > burnableFraction * 1.05) {
            throw new IllegalStateException("fraction of burned pixels in LC classes (" + lcAreaSumFraction + ") > burnable fraction (" + burnableFraction + ") at target pixel " + targetGridCellIndex + "!");
        }
    }

    @Override
    protected float getErrorPerPixel(double[] probabilityOfBurn, double gridCellArea, double burnedPercentage) {
        // Mask all pixels with value of 0 in the confidence level layer (they should not be included in the analysis)
        double[] probabilityOfBurnMasked = Arrays.stream(probabilityOfBurn).filter(d -> d > 0).toArray();
        int n = probabilityOfBurnMasked.length;

        // pixel area is the area of the pixels. In the case of S2 it is the area of one S2 pixel, you can calculate it as the area of the 0.25º grid cell divided the TOTAL number of S2 pixels (both masked and unmasked)
        double pixelArea = gridCellArea / (double) probabilityOfBurn.length;
        if (n == 1) {
            return (float) pixelArea;
        }

        // pb_i = value of confidence level of pixel /100
        double[] pb = Arrays.stream(probabilityOfBurnMasked).map(d -> d / 100.0).toArray();

        // Var_c = sum (pb_i*(1-pb_i)
        double var_c = Arrays.stream(pb).map(pb_i -> (pb_i * (1.0 - pb_i))).sum();

        // SE = sqr(var_c*(n/(n-1))) * pixel area
        return (float) (Math.sqrt(var_c * (n / (n - 1.0))) * pixelArea);

    }

    @Override
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
    }

    @Override
    protected int getLcClassesCount() {
        return 6;
    }

    @Override
    protected void addBaInLandCover(List<double[]> baInLc, int targetGridCellIndex, double burnedArea, int sourceLc) {
        boolean inBurnableClass = LcRemappingS2.isInBurnableLcClass(sourceLc);
        if (sourceLc <= baInLc.size()) {
            baInLc.get(sourceLc - 1)[targetGridCellIndex] += inBurnableClass ? burnedArea : 0.0F;
        }
    }

    @Override
    protected boolean isInBrokenLCZone(int x, int y) {
        // oneDegTile = x191y98
        int targetGridCellX = 4 * Integer.parseInt(oneDegTile.split("y")[0].replace("x", "")) + x;
        int targetGridCellY = 720 - 4 * (Integer.parseInt(oneDegTile.split("y")[1]) + 1) + y;

        // targetGridCellX = 4*191 + x = 764 + x
        // targetGridCellY = 720 - 4 * 98 + y = 328 + y

        return
                (targetGridCellX == 767 && targetGridCellY == 414)
                        || (targetGridCellX == 767 && targetGridCellY == 415)
                        || (targetGridCellX == 767 && targetGridCellY == 416)
                        || (targetGridCellX == 767 && targetGridCellY == 417)
                        || (targetGridCellX == 767 && targetGridCellY == 418)
                        || (targetGridCellX == 767 && targetGridCellY == 419)
                        || (targetGridCellX == 766 && targetGridCellY == 418)
                        || (targetGridCellX == 766 && targetGridCellY == 419)
                        || (targetGridCellX == 768 && targetGridCellY == 414)
                        || (targetGridCellX == 923 && targetGridCellY == 323)
                        || (targetGridCellX == 924 && targetGridCellY == 323)
                        || (targetGridCellX == 925 && targetGridCellY == 323)
                        || (targetGridCellX == 916 && targetGridCellY == 440)
                        || (targetGridCellX == 819 && targetGridCellY == 497)
                        || (targetGridCellX == 819 && targetGridCellY == 498)
                        || (targetGridCellX == 770 && targetGridCellY == 440)
                        || (targetGridCellX == 766 && targetGridCellY == 382)
                        || (targetGridCellX == 767 && targetGridCellY == 382)
                        || (targetGridCellX == 663 && targetGridCellY == 323)
                        || (targetGridCellX == 664 && targetGridCellY == 323)
                        || (targetGridCellX == 665 && targetGridCellY == 323)
                        || (targetGridCellX == 648 && targetGridCellY == 300)
                        || (targetGridCellX == 647 && targetGridCellY == 300)
                        || (targetGridCellX == 650 && targetGridCellY == 299)
                        || (targetGridCellX == 864 && targetGridCellY == 265)
                        || (targetGridCellX == 912 && targetGridCellY == 313)
                        || (targetGridCellX == 912 && targetGridCellY == 314)
                        || (targetGridCellX == 878 && targetGridCellY == 427)
                        || (targetGridCellX == 860 && targetGridCellY == 440)
                        || (targetGridCellX == 842 && targetGridCellY == 484)
                        || (targetGridCellX == 839 && targetGridCellY == 488)
                        || (targetGridCellX == 771 && targetGridCellY == 409)
                        || (targetGridCellX == 912 && targetGridCellY == 410)
                        || (targetGridCellX == 912 && targetGridCellY == 411)
                        || (targetGridCellX == 894 && targetGridCellY == 440)
                        || (targetGridCellX == 895 && targetGridCellY == 440)
                        || (targetGridCellX == 896 && targetGridCellY == 440)
                        || (targetGridCellX == 776 && targetGridCellY == 455)
                        || (targetGridCellX == 776 && targetGridCellY == 456)
                        || (targetGridCellX == 778 && targetGridCellY == 459)
                        || (targetGridCellX == 884 && targetGridCellY == 300)
                        || (targetGridCellX == 885 && targetGridCellY == 300)
                        || (targetGridCellX == 886 && targetGridCellY == 300)
                        || (targetGridCellX == 784 && targetGridCellY == 287)
                        || (targetGridCellX == 892 && targetGridCellY == 306)
                        || (targetGridCellX == 893 && targetGridCellY == 306)
                        || (targetGridCellX == 892 && targetGridCellY == 307)
                        || (targetGridCellX == 893 && targetGridCellY == 307)
                        || (targetGridCellX == 894 && targetGridCellY == 307)
                        || (targetGridCellX == 893 && targetGridCellY == 308)
                        || (targetGridCellX == 894 && targetGridCellY == 308)
                        || (targetGridCellX == 894 && targetGridCellY == 309)
                        || (targetGridCellX == 895 && targetGridCellY == 309);
    }

    private FileStatus[] getFileStatuses(String inputPathPatterns, Configuration conf) throws IOException {
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return hdfsInventoryService.globFileStatuses(inputPatterns, conf);
    }

}