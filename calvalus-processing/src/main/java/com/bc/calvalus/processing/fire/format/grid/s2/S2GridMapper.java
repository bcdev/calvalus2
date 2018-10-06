package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemappingS2;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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

    protected S2GridMapper() {
        super(NUM_GRID_CELLS * GRID_CELLS_PER_DEGREE, NUM_GRID_CELLS * GRID_CELLS_PER_DEGREE);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] fakePaths = inputSplit.getPaths();
        if (fakePaths.length == 1) {
            return;
        }
        LOG.info("paths=" + Arrays.toString(fakePaths));

        List<File> paths = new ArrayList<>();
        for (int i = 0; i < fakePaths.length - 1; i++) {
            Path path = fakePaths[i];
            String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/s2-pixel/" + path.getName() + "-2016-" + month + "-Fire-Pixel-Formatting.*/.*tif";
            FileStatus[] fileStatuses = getFileStatuses(inputPathPattern, context.getConfiguration());
            for (FileStatus fileStatus : fileStatuses) {
                File file = CalvalusProductIO.copyFileToLocal(fileStatus.getPath(), context.getConfiguration());
                paths.add(file);
            }
        }

        String oneDegTile = fakePaths[fakePaths.length - 1].getName();

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
            } else if (productFile.getName().contains("LC")) {
                lcProducts.add(product);
            } else {
                throw new IllegalStateException("Unknown product: " + productFile.getName());
            }

            sourceProducts.sort(Comparator.comparing(ProductNode::getName));
            clProducts.sort(Comparator.comparing(ProductNode::getName));
            lcProducts.sort(Comparator.comparing(ProductNode::getName));
        }

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        S2FireGridDataSource dataSource = new S2FireGridDataSource(oneDegTile, sourceProducts.toArray(new Product[0]), clProducts.toArray(new Product[0]), lcProducts.toArray(new Product[0]));
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        setDataSource(dataSource);
        GridCells gridCells = computeGridCells(year, month, pm);

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
    protected float getErrorPerPixel(double[] probabilityOfBurn, double area, int numberOfBurnedPixels, double burnedArea) {
        double[] values = Arrays.stream(probabilityOfBurn)
                .map(d -> d == 0 ? 0.01 : d)
                .filter(d -> d >= 0.01 && d <= 1.0)
                .toArray();

        double sum_pb = Arrays.stream(values).sum();
        double S = numberOfBurnedPixels / sum_pb;
        double[] pb_i_star = Arrays.stream(values).map(d -> d * S).toArray();
        double checksum = Arrays.stream(pb_i_star).sum();

        if (Math.abs(checksum - numberOfBurnedPixels) > 0.0001) {
            throw new IllegalStateException(String.format("Math.abs(checksum (%s) - numberOfBurnedPixels (%s)) > 0.0001", checksum, numberOfBurnedPixels));
        }

        double var_c = 0.0;
        int count = 0;
        for (double p : pb_i_star) {
            var_c += p * (1 - p);
            count++;
        }

        if (count == 0) {
            return 0;
        }
        if (count == 1) {
            return 1;
        }

        return (float) Math.sqrt(var_c * (count / (count - 1.0))) * (float) (area / probabilityOfBurn.length);
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

    private FileStatus[] getFileStatuses(String inputPathPatterns, Configuration conf) throws IOException {
        HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(conf, "eodata");
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return hdfsInventoryService.globFileStatuses(inputPatterns, conf);
    }

}